package retriever

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/filecoin-project/go-clock"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-trustless-utils/traversal"
	"github.com/parkan/sheltie/pkg/blockbroker"
	"github.com/parkan/sheltie/pkg/types"
)

// HybridRetriever wraps an existing retriever and adds fallback to per-block
// retrieval when the primary retriever fails with a missing block error.
type HybridRetriever struct {
	inner           types.Retriever
	candidateSource types.CandidateSource
	httpClient      *http.Client
	clock           clock.Clock
}

// NewHybridRetriever creates a new hybrid retriever that wraps an existing
// retriever and falls back to per-block fetching on partial responses.
func NewHybridRetriever(
	inner types.Retriever,
	candidateSource types.CandidateSource,
	httpClient *http.Client,
) *HybridRetriever {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &HybridRetriever{
		inner:           inner,
		candidateSource: candidateSource,
		httpClient:      httpClient,
		clock:           clock.New(),
	}
}

type phaseOneStats struct {
	bytesReceived  uint64
	blocksReceived uint64
}

func (hr *HybridRetriever) Retrieve(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {
	startTime := hr.clock.Now()

	if eventsCallback == nil {
		eventsCallback = func(types.RetrievalEvent) {}
	}

	var p1Stats phaseOneStats
	wrappedCallback := func(event types.RetrievalEvent) {
		if br, ok := event.(interface{ ByteCount() uint64 }); ok {
			p1Stats.bytesReceived += br.ByteCount()
			p1Stats.blocksReceived++
		}
		eventsCallback(event)
	}

	stats, err := hr.inner.Retrieve(ctx, request, wrappedCallback)
	if err == nil {
		return stats, nil
	}

	missingCid, ok := extractMissingCid(err)
	if !ok {
		return nil, err
	}

	logger.Infow("whole-DAG incomplete, falling back to per-block",
		"root", request.Root,
		"missingCid", missingCid,
		"phase1Blocks", p1Stats.blocksReceived,
		"phase1Bytes", p1Stats.bytesReceived,
		"err", err)

	stats, err = hr.continuePerBlock(ctx, request, eventsCallback, startTime, p1Stats)
	if err != nil {
		return nil, err
	}
	return stats, nil
}

func (hr *HybridRetriever) continuePerBlock(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
	startTime time.Time,
	p1Stats phaseOneStats,
) (*types.RetrievalStats, error) {
	session := blockbroker.NewSession(hr.candidateSource, hr.httpClient)
	defer session.Close()

	// Seed with providers from root CID to reuse partial-content providers
	session.SeedProviders(ctx, request.Root)

	var p2BlocksOut uint64
	var p2BytesOut uint64

	lsys := hr.makeSessionBackedLinkSystem(request.LinkSystem, session, &p2BytesOut, &p2BlocksOut)

	originalWriteOpener := lsys.StorageWriteOpener
	if originalWriteOpener != nil {
		lsys.StorageWriteOpener = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
			w, wc, err := originalWriteOpener(lc)
			if err != nil {
				return nil, nil, err
			}
			return w, wc, nil
		}
	}

	var maxBlocks uint64
	if request.MaxBlocks > 0 {
		if p1Stats.blocksReceived >= request.MaxBlocks {
			maxBlocks = 1 // Need at least 1 to attempt traversal
		} else {
			maxBlocks = request.MaxBlocks - p1Stats.blocksReceived
		}
	}

	cfg := traversal.Config{
		Root:      request.Root,
		Selector:  request.GetSelector(),
		MaxBlocks: maxBlocks,
	}

	_, err := cfg.Traverse(ctx, lsys, nil)
	if err != nil {
		return nil, fmt.Errorf("per-block traversal failed: %w", err)
	}

	totalBytes := p1Stats.bytesReceived + atomic.LoadUint64(&p2BytesOut)
	totalBlocks := p1Stats.blocksReceived + atomic.LoadUint64(&p2BlocksOut)
	duration := hr.clock.Since(startTime)

	var speed uint64
	if duration.Seconds() > 0 {
		speed = uint64(float64(totalBytes) / duration.Seconds())
	}

	return &types.RetrievalStats{
		RootCid:      request.Root,
		Size:         totalBytes,
		Blocks:       totalBlocks,
		Duration:     duration,
		AverageSpeed: speed,
		TotalPayment: big.Zero(),
	}, nil
}

// makeSessionBackedLinkSystem wraps baseLsys to fetch missing blocks via session.
// Tries CAR subgraph fetch first (efficient), falls back to per-block.
func (hr *HybridRetriever) makeSessionBackedLinkSystem(
	baseLsys linking.LinkSystem,
	session blockbroker.BlockSession,
	bytesOut *uint64,
	blocksOut *uint64,
) linking.LinkSystem {
	lsys := baseLsys
	originalReader := lsys.StorageReadOpener

	lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		c := l.(cidlink.Link).Cid

		if originalReader != nil {
			rdr, err := originalReader(lc, l)
			if err == nil {
				return rdr, nil
			}
			// Only network-fetch on not-found; propagate other errors
			if !isNotFoundError(err) {
				return nil, err
			}
		}

		// Try CAR subgraph first (use baseLsys to avoid recursion)
		blocksFromCAR, carErr := session.GetSubgraph(lc.Ctx, c, baseLsys)
		if carErr == nil && blocksFromCAR > 0 {
			logger.Debugw("subgraph CAR fetch succeeded", "cid", c, "blocks", blocksFromCAR)
			if originalReader != nil {
				rdr, err := originalReader(lc, l)
				if err == nil {
					return rdr, nil
				}
				logger.Warnw("CAR fetch succeeded but block not in storage", "cid", c, "err", err)
			}
		}

		if carErr != nil {
			logger.Debugw("CAR fetch failed, falling back to per-block", "cid", c, "carErr", carErr)
		}

		block, err := session.Get(lc.Ctx, c)
		if err != nil {
			return nil, err
		}

		blockData := block.RawData()
		atomic.AddUint64(bytesOut, uint64(len(blockData)))
		atomic.AddUint64(blocksOut, 1)

		if baseLsys.StorageWriteOpener != nil {
			w, wc, werr := baseLsys.StorageWriteOpener(lc)
			if werr == nil {
				_, _ = io.Copy(w, bytes.NewReader(blockData))
				_ = wc(l)
			}
		}

		return bytes.NewReader(blockData), nil
	}

	return lsys
}

func isNotFoundError(err error) bool {
	var notFound format.ErrNotFound
	if errors.As(err, &notFound) {
		return true
	}

	var nf interface{ NotFound() bool }
	if errors.As(err, &nf) && nf.NotFound() {
		return true
	}

	// memstore returns literal "404" string
	if err != nil && err.Error() == "404" {
		return true
	}

	return false
}

func extractMissingCid(err error) (cid.Cid, bool) {
	var notFound format.ErrNotFound
	if errors.As(err, &notFound) {
		return notFound.Cid, true
	}

	if errors.Is(err, traversal.ErrMissingBlock) {
		var nf format.ErrNotFound
		if errors.As(err, &nf) {
			return nf.Cid, true
		}
		return cid.Undef, true
	}

	return cid.Undef, false
}

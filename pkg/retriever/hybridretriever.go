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
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
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

	logger.Infow("switching to per-block retrieval (missing blocks in initial response)",
		"root", request.Root,
		"missingCid", missingCid,
		"phase1Blocks", p1Stats.blocksReceived,
		"phase1Bytes", p1Stats.bytesReceived)

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

	// Calculate max blocks for this phase
	var maxBlocks uint64
	if request.MaxBlocks > 0 {
		if p1Stats.blocksReceived >= request.MaxBlocks {
			maxBlocks = 1 // Need at least 1 to attempt traversal
		} else {
			maxBlocks = request.MaxBlocks - p1Stats.blocksReceived
		}
	}

	// Use frontier-based streaming traversal
	err := hr.streamingTraverse(ctx, request, session, &p2BytesOut, &p2BlocksOut, maxBlocks)
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
	}, nil
}

// streamingTraverse uses frontier-based traversal to fetch blocks without
// requiring read-back from storage. Blocks are fetched from network, links
// are parsed immediately, and data is written to output.
func (hr *HybridRetriever) streamingTraverse(
	ctx context.Context,
	request types.RetrievalRequest,
	session blockbroker.BlockSession,
	bytesOut *uint64,
	blocksOut *uint64,
	maxBlocks uint64,
) error {
	frontier := NewFrontier(request.Root)
	baseLsys := request.LinkSystem

	var blockCount uint64

	for !frontier.Empty() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		c := frontier.Pop()

		// Skip if already seen
		if frontier.Seen(c) {
			continue
		}

		// Check if already in storage (from phase 1)
		if baseLsys.StorageReadOpener != nil {
			rdr, err := baseLsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
			if err == nil {
				// Block exists in storage - read it to parse links
				data, readErr := io.ReadAll(rdr)
				if readErr == nil {
					block, blockErr := blocks.NewBlockWithCid(data, c)
					if blockErr == nil {
						logger.Debugw("using cached block from phase 1", "cid", c)
						// Parse links and add to frontier
						links, _ := ExtractLinks(block)
						frontier.PushAll(links)
					}
				}
				frontier.MarkSeen(c)
				continue
			}
		}

		// Fetch block from network
		block, err := hr.fetchBlock(ctx, c, session, baseLsys)
		if err != nil {
			return fmt.Errorf("failed to fetch block %s: %w", c, err)
		}

		// Parse links and add to frontier
		links, err := ExtractLinks(block)
		if err != nil {
			logger.Warnw("failed to extract links", "cid", c, "err", err)
		} else {
			frontier.PushAll(links)
		}

		// Write to output
		if err := hr.writeBlock(ctx, block, baseLsys); err != nil {
			return fmt.Errorf("failed to write block %s: %w", c, err)
		}

		// Update stats
		blockData := block.RawData()
		atomic.AddUint64(bytesOut, uint64(len(blockData)))
		atomic.AddUint64(blocksOut, 1)

		frontier.MarkSeen(c)
		blockCount++

		// Check max blocks limit
		if maxBlocks > 0 && blockCount >= maxBlocks {
			logger.Infow("reached max blocks limit", "limit", maxBlocks)
			break
		}
	}

	return nil
}

// fetchBlock tries to get a block from the network, preferring CAR subgraph fetch
// for blocks that may have children (dag-pb, dag-cbor), but using direct raw fetch
// for leaf blocks (raw codec) which have no children.
func (hr *HybridRetriever) fetchBlock(
	ctx context.Context,
	c cid.Cid,
	session blockbroker.BlockSession,
	baseLsys linking.LinkSystem,
) (blocks.Block, error) {
	// Raw codec (0x55) blocks are leaves with no children - skip CAR subgraph
	// and fetch directly as raw block (more efficient, less overhead)
	if c.Prefix().Codec == cid.Raw {
		block, err := session.Get(ctx, c)
		if err != nil {
			return nil, err
		}
		logger.Debugw("fetched raw leaf block", "cid", c, "bytes", len(block.RawData()))
		return block, nil
	}

	// For dag-pb/dag-cbor, try CAR subgraph first (efficient for subtrees)
	blocksFromCAR, carErr := session.GetSubgraph(ctx, c, baseLsys)
	if carErr == nil && blocksFromCAR > 0 {
		logger.Debugw("fetched subgraph via CAR", "cid", c, "blocks", blocksFromCAR)
		// The CAR fetch wrote to baseLsys, now read back the block we need
		if baseLsys.StorageReadOpener != nil {
			rdr, err := baseLsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, cidlink.Link{Cid: c})
			if err == nil {
				data, err := io.ReadAll(rdr)
				if err == nil {
					return blocks.NewBlockWithCid(data, c)
				}
			}
		}
	}

	// CAR subgraph failed, fall back to single raw block fetch
	if carErr != nil {
		logger.Debugw("CAR subgraph unavailable, trying single block", "cid", c, "reason", carErr)
	}

	block, err := session.Get(ctx, c)
	if err != nil {
		return nil, err
	}
	logger.Debugw("fetched single block", "cid", c, "bytes", len(block.RawData()))
	return block, nil
}

// writeBlock writes a block to the output via the LinkSystem's write opener.
func (hr *HybridRetriever) writeBlock(
	ctx context.Context,
	block blocks.Block,
	baseLsys linking.LinkSystem,
) error {
	if baseLsys.StorageWriteOpener == nil {
		return nil
	}

	w, wc, err := baseLsys.StorageWriteOpener(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}

	if _, err := io.Copy(w, bytes.NewReader(block.RawData())); err != nil {
		return err
	}

	return wc(cidlink.Link{Cid: block.Cid()})
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

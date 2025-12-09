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

// phaseOneStats holds stats from the first phase of retrieval
type phaseOneStats struct {
	bytesReceived  uint64
	blocksReceived uint64
}

// Retrieve attempts to fetch the requested DAG. It first tries the inner
// retriever (whole-DAG), then falls back to per-block retrieval if the
// inner retriever returns a missing block error.
func (hr *HybridRetriever) Retrieve(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {
	startTime := hr.clock.Now()

	if eventsCallback == nil {
		eventsCallback = func(types.RetrievalEvent) {}
	}

	// Track phase 1 stats via events
	var p1Stats phaseOneStats
	wrappedCallback := func(event types.RetrievalEvent) {
		// Track blocks received during phase 1
		if br, ok := event.(interface{ ByteCount() uint64 }); ok {
			p1Stats.bytesReceived += br.ByteCount()
			p1Stats.blocksReceived++
		}
		eventsCallback(event)
	}

	// Phase 1: Try whole-DAG from inner retriever
	stats, err := hr.inner.Retrieve(ctx, request, wrappedCallback)
	if err == nil {
		return stats, nil
	}

	// Check if it's a "missing block" error we can recover from
	missingCid, ok := extractMissingCid(err)
	if !ok {
		return nil, err // Not recoverable
	}

	logger.Infow("whole-DAG fetch incomplete, falling back to per-block",
		"root", request.Root,
		"missingCid", missingCid,
		"phase1Blocks", p1Stats.blocksReceived,
		"phase1Bytes", p1Stats.bytesReceived,
		"err", err)

	// Phase 2: Fall back to per-block fetching
	// Note: The LinkSystem already has blocks from partial fetch
	stats, err = hr.continuePerBlock(ctx, request, eventsCallback, startTime, p1Stats)
	if err != nil {
		return nil, err
	}

	return stats, nil
}

// continuePerBlock continues the retrieval using per-block fetching for missing blocks.
func (hr *HybridRetriever) continuePerBlock(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
	startTime time.Time,
	p1Stats phaseOneStats,
) (*types.RetrievalStats, error) {

	// Create a block session for per-block fetching
	// Seed it with candidates from the root CID to potentially reuse providers
	session := blockbroker.NewSession(hr.candidateSource, hr.httpClient)
	defer session.Close()

	// Pre-seed session with providers for the root CID
	// This helps reuse providers that may have had partial content
	session.SeedProviders(ctx, request.Root)

	// Track blocks/bytes for phase 2 stats using atomics (safe for concurrent access)
	var p2BlocksOut uint64
	var p2BytesOut uint64

	// Create a LinkSystem that:
	// 1. Checks existing storage first (blocks from partial whole-DAG fetch)
	// 2. Falls back to session.Get() for missing blocks
	// 3. Tracks stats for blocks fetched via session
	lsys := hr.makeSessionBackedLinkSystem(request.LinkSystem, session, &p2BytesOut, &p2BlocksOut)

	// Wrap the write opener to track stats if present
	originalWriteOpener := lsys.StorageWriteOpener
	if originalWriteOpener != nil {
		lsys.StorageWriteOpener = func(lc linking.LinkContext) (io.Writer, linking.BlockWriteCommitter, error) {
			w, wc, err := originalWriteOpener(lc)
			if err != nil {
				return nil, nil, err
			}
			// Just pass through - stats are tracked in makeSessionBackedLinkSystem
			return w, wc, nil
		}
	}

	// Calculate remaining block budget for phase 2
	// Phase 1 already consumed p1Stats.blocksReceived blocks
	var maxBlocks uint64
	if request.MaxBlocks > 0 {
		if p1Stats.blocksReceived >= request.MaxBlocks {
			// Already at or over limit - this shouldn't happen normally
			// but handle gracefully by allowing no more blocks
			maxBlocks = 1 // Need at least 1 to attempt traversal
		} else {
			maxBlocks = request.MaxBlocks - p1Stats.blocksReceived
		}
	}

	// Resume traversal from root
	// Already-fetched blocks will be found in storage
	cfg := traversal.Config{
		Root:      request.Root,
		Selector:  request.GetSelector(),
		MaxBlocks: maxBlocks,
	}

	_, err := cfg.Traverse(ctx, lsys, nil)
	if err != nil {
		return nil, fmt.Errorf("per-block traversal failed: %w", err)
	}

	// Combine stats from both phases
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

// makeSessionBackedLinkSystem creates a LinkSystem that first checks the
// existing storage (for blocks from the partial whole-DAG fetch), then
// falls back to fetching via the block session for not-found errors only.
// It tracks bytes and blocks fetched via the session for accurate stats.
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

		// Try existing storage first (blocks from partial whole-DAG fetch)
		if originalReader != nil {
			rdr, err := originalReader(lc, l)
			if err == nil {
				return rdr, nil
			}
			// Only fall back to network fetch for not-found errors
			// Propagate other errors (codec, timeout, permission, etc.)
			if !isNotFoundError(err) {
				return nil, err
			}
		}

		// Fetch via session (with per-block provider discovery)
		block, err := session.Get(lc.Ctx, c)
		if err != nil {
			return nil, err
		}

		// Track stats for blocks fetched via session (regardless of write opener)
		blockData := block.RawData()
		atomic.AddUint64(bytesOut, uint64(len(blockData)))
		atomic.AddUint64(blocksOut, 1)

		// Write to storage for future use (and output) if writer available
		if lsys.StorageWriteOpener != nil {
			w, wc, werr := lsys.StorageWriteOpener(lc)
			if werr == nil {
				_, _ = io.Copy(w, bytes.NewReader(blockData))
				_ = wc(l)
			}
		}

		return bytes.NewReader(blockData), nil
	}

	return lsys
}

// isNotFoundError checks if an error indicates a block was not found,
// as opposed to other errors like codec/timeout/permission issues.
func isNotFoundError(err error) bool {
	// Check for format.ErrNotFound (go-ipld-format)
	var notFound format.ErrNotFound
	if errors.As(err, &notFound) {
		return true
	}

	// Check for interface-based not-found (go-ipld-prime style)
	var nf interface{ NotFound() bool }
	if errors.As(err, &nf) && nf.NotFound() {
		return true
	}

	return false
}

// extractMissingCid extracts the missing CID from an error, if it's a
// recoverable "missing block" error.
func extractMissingCid(err error) (cid.Cid, bool) {
	// Check for format.ErrNotFound
	var notFound format.ErrNotFound
	if errors.As(err, &notFound) {
		return notFound.Cid, true
	}

	// Check for traversal.ErrMissingBlock wrapper
	if errors.Is(err, traversal.ErrMissingBlock) {
		// Try to extract the CID from the wrapped error
		var nf format.ErrNotFound
		if errors.As(err, &nf) {
			return nf.Cid, true
		}
		// Even without the CID, we know it's a missing block error
		return cid.Undef, true
	}

	return cid.Undef, false
}

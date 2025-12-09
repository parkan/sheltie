package blockbroker

import (
	"context"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
)

// BlockSession manages per-block fetching with provider caching.
// A session maintains a set of providers discovered during block fetching
// and reuses them for subsequent blocks until they fail.
type BlockSession interface {
	// Get fetches a block, trying cached providers first, then discovering new ones
	Get(ctx context.Context, c cid.Cid) (blocks.Block, error)
	// SeedProviders pre-populates the session with providers for a CID (best-effort)
	SeedProviders(ctx context.Context, c cid.Cid)
	// Close releases session resources
	Close() error
}

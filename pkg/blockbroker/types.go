package blockbroker

import (
	"context"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/linking"
)

// BlockSession manages per-block fetching with provider caching.
type BlockSession interface {
	Get(ctx context.Context, c cid.Cid) (blocks.Block, error)
	GetSubgraph(ctx context.Context, c cid.Cid, lsys linking.LinkSystem) (int, error)
	// GetSubgraphStream returns a raw CAR stream for the given CID.
	// Caller must close the returned ReadCloser.
	GetSubgraphStream(ctx context.Context, c cid.Cid) (io.ReadCloser, string, error)
	SeedProviders(ctx context.Context, c cid.Cid)
	// UsedProviders returns the list of provider endpoints that served blocks
	UsedProviders() []string
	Close() error
}

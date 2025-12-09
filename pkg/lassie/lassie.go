// MODIFIED: 2025-10-30
// - Renamed package from lassie to sheltie
// - Removed bitswap concurrency configuration and constants
// - Removed bitswap protocol initialization
// MODIFIED: 2025-12-09
// - Removed graphsync support, HTTP-only
// - Removed libp2p host, datastore dependencies

package lassie

import (
	"context"
	"net/http"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/session"
	"github.com/filecoin-project/lassie/pkg/types"
)

var _ types.Fetcher = &Lassie{}

const DefaultProviderTimeout = 20 * time.Second

// Lassie represents a reusable retrieval client.
type Lassie struct {
	cfg       *LassieConfig
	retriever *retriever.Retriever
}

// LassieConfig customizes the behavior of a Lassie instance.
type LassieConfig struct {
	Source            types.CandidateSource
	ProviderTimeout   time.Duration
	GlobalTimeout     time.Duration
	ProviderBlockList map[peer.ID]bool
	ProviderAllowList map[peer.ID]bool
}

type LassieOption func(cfg *LassieConfig)

// NewLassie creates a new Lassie instance.
func NewLassie(ctx context.Context, opts ...LassieOption) (*Lassie, error) {
	cfg := NewLassieConfig(opts...)
	return NewLassieWithConfig(ctx, cfg)
}

// NewLassieConfig creates a new LassieConfig instance with the given LassieOptions.
func NewLassieConfig(opts ...LassieOption) *LassieConfig {
	cfg := &LassieConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// NewLassieWithConfig creates a new Lassie instance with a custom
// configuration.
func NewLassieWithConfig(ctx context.Context, cfg *LassieConfig) (*Lassie, error) {
	// HTTP-only protocol
	protocols := []multicodec.Code{multicodec.TransportIpfsGatewayHttp}

	if cfg.Source == nil {
		var err error
		cfg.Source, err = indexerlookup.NewCandidateSource(
			indexerlookup.WithHttpClient(&http.Client{}),
			indexerlookup.WithProtocols(protocols),
		)
		if err != nil {
			return nil, err
		}
	}

	if cfg.ProviderTimeout == 0 {
		cfg.ProviderTimeout = DefaultProviderTimeout
	}

	sessionConfig := session.DefaultConfig().
		WithProviderBlockList(cfg.ProviderBlockList).
		WithProviderAllowList(cfg.ProviderAllowList).
		WithDefaultProviderConfig(session.ProviderConfig{
			RetrievalTimeout: cfg.ProviderTimeout,
		})
	sess := session.NewSession(sessionConfig, true)

	protocolRetrievers := map[multicodec.Code]types.CandidateRetriever{
		multicodec.TransportIpfsGatewayHttp: retriever.NewHttpRetriever(sess, http.DefaultClient),
	}

	ret, err := retriever.NewRetriever(ctx, sess, cfg.Source, protocolRetrievers)
	if err != nil {
		return nil, err
	}

	// Wrap the retriever with HybridRetriever for per-block fallback
	ret.WrapWithHybrid(cfg.Source, http.DefaultClient)

	ret.Start()

	l := &Lassie{
		cfg:       cfg,
		retriever: ret,
	}

	return l, nil
}

// WithCandidateSource allows you to specify a custom candidate finder.
func WithCandidateSource(finder types.CandidateSource) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Source = finder
	}
}

// WithProviderTimeout allows you to specify a custom timeout for retrieving
// data from a provider. Beyond this limit, when no data has been received,
// the retrieval will fail.
func WithProviderTimeout(timeout time.Duration) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ProviderTimeout = timeout
	}
}

// WithGlobalTimeout allows you to specify a custom timeout for the entire
// retrieval process.
func WithGlobalTimeout(timeout time.Duration) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.GlobalTimeout = timeout
	}
}

// WithProviderBlockList allows you to specify a custom provider block list.
func WithProviderBlockList(providerBlockList map[peer.ID]bool) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ProviderBlockList = providerBlockList
	}
}

// WithProviderAllowList allows you to specify a custom set of providers to
// allow fetching from. If this is not set, all providers will be allowed unless
// they are in the block list.
func WithProviderAllowList(providerAllowList map[peer.ID]bool) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ProviderAllowList = providerAllowList
	}
}

// Fetch initiates a retrieval request and returns either some details about
// the retrieval or an error. The request should contain all of the parameters
// of the requested retrieval, including the LinkSystem where the blocks are
// intended to be stored.
func (l *Lassie) Fetch(ctx context.Context, request types.RetrievalRequest, opts ...types.FetchOption) (*types.RetrievalStats, error) {
	var cancel context.CancelFunc
	if l.cfg.GlobalTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, l.cfg.GlobalTimeout)
		defer cancel()
	}
	return l.retriever.Retrieve(ctx, request, types.NewFetchConfig(opts...).EventsCallback)
}

// RegisterSubscriber registers a subscriber to receive retrieval events.
// The returned function can be called to unregister the subscriber.
func (l *Lassie) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	return l.retriever.RegisterSubscriber(subscriber)
}

package sheltie

import (
	"context"
	"net/http"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/parkan/sheltie/pkg/indexerlookup"
	"github.com/parkan/sheltie/pkg/retriever"
	"github.com/parkan/sheltie/pkg/session"
	"github.com/parkan/sheltie/pkg/types"
)

var _ types.Fetcher = &Sheltie{}

const DefaultProviderTimeout = 20 * time.Second

// Sheltie represents a reusable retrieval client.
type Sheltie struct {
	cfg       *SheltieConfig
	retriever *retriever.Retriever
}

// SheltieConfig customizes the behavior of a Sheltie instance.
type SheltieConfig struct {
	Source            types.CandidateSource
	ProviderTimeout   time.Duration
	GlobalTimeout     time.Duration
	ProviderBlockList map[peer.ID]bool
	ProviderAllowList map[peer.ID]bool
}

type SheltieOption func(cfg *SheltieConfig)

// NewSheltie creates a new Sheltie instance.
func NewSheltie(ctx context.Context, opts ...SheltieOption) (*Sheltie, error) {
	cfg := NewSheltieConfig(opts...)
	return NewSheltieWithConfig(ctx, cfg)
}

// NewSheltieConfig creates a new SheltieConfig instance with the given SheltieOptions.
func NewSheltieConfig(opts ...SheltieOption) *SheltieConfig {
	cfg := &SheltieConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

// NewSheltieWithConfig creates a new Sheltie instance with a custom
// configuration.
func NewSheltieWithConfig(ctx context.Context, cfg *SheltieConfig) (*Sheltie, error) {
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

	httpRetriever := retriever.NewHttpRetriever(sess, http.DefaultClient)
	ret, err := retriever.NewRetriever(ctx, sess, cfg.Source, httpRetriever, multicodec.TransportIpfsGatewayHttp)
	if err != nil {
		return nil, err
	}

	// Wrap the retriever with HybridRetriever for per-block fallback
	ret.WrapWithHybrid(cfg.Source, http.DefaultClient)

	ret.Start()

	sheltie := &Sheltie{
		cfg:       cfg,
		retriever: ret,
	}

	return sheltie, nil
}

// WithCandidateSource allows you to specify a custom candidate finder.
func WithCandidateSource(finder types.CandidateSource) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.Source = finder
	}
}

// WithProviderTimeout allows you to specify a custom timeout for retrieving
// data from a provider. Beyond this limit, when no data has been received,
// the retrieval will fail.
func WithProviderTimeout(timeout time.Duration) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.ProviderTimeout = timeout
	}
}

// WithGlobalTimeout allows you to specify a custom timeout for the entire
// retrieval process.
func WithGlobalTimeout(timeout time.Duration) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.GlobalTimeout = timeout
	}
}

// WithProviderBlockList allows you to specify a custom provider block list.
func WithProviderBlockList(providerBlockList map[peer.ID]bool) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.ProviderBlockList = providerBlockList
	}
}

// WithProviderAllowList allows you to specify a custom set of providers to
// allow fetching from. If this is not set, all providers will be allowed unless
// they are in the block list.
func WithProviderAllowList(providerAllowList map[peer.ID]bool) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.ProviderAllowList = providerAllowList
	}
}

// Fetch initiates a retrieval request and returns either some details about
// the retrieval or an error. The request should contain all of the parameters
// of the requested retrieval, including the LinkSystem where the blocks are
// intended to be stored.
func (l *Sheltie) Fetch(ctx context.Context, request types.RetrievalRequest, opts ...types.FetchOption) (*types.RetrievalStats, error) {
	var cancel context.CancelFunc
	if l.cfg.GlobalTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, l.cfg.GlobalTimeout)
		defer cancel()
	}
	return l.retriever.Retrieve(ctx, request, types.NewFetchConfig(opts...).EventsCallback)
}

// RegisterSubscriber registers a subscriber to receive retrieval events.
// The returned function can be called to unregister the subscriber.
func (l *Sheltie) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	return l.retriever.RegisterSubscriber(subscriber)
}

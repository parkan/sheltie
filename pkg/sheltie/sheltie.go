package sheltie

import (
	"context"
	"net/http"
	"time"

	"github.com/parkan/sheltie/pkg/indexerlookup"
	"github.com/parkan/sheltie/pkg/net/client"
	"github.com/parkan/sheltie/pkg/net/host"
	"github.com/parkan/sheltie/pkg/retriever"
	"github.com/parkan/sheltie/pkg/session"
	"github.com/parkan/sheltie/pkg/types"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
)

var _ types.Fetcher = &Sheltie{}

const DefaultProviderTimeout = 20 * time.Second
const DefaultBitswapConcurrency = 32
const DefaultBitswapConcurrencyPerRetrieval = 12

// Sheltie represents a reusable retrieval client.
type Sheltie struct {
	cfg       *SheltieConfig
	retriever *retriever.Retriever
}

// SheltieConfig customizes the behavior of a Sheltie instance.
type SheltieConfig struct {
	Source                         types.CandidateSource
	Host                           host.Host
	ProviderTimeout                time.Duration
	ConcurrentSPRetrievals         uint
	GlobalTimeout                  time.Duration
	Libp2pOptions                  []libp2p.Option
	Protocols                      []multicodec.Code
	ProviderBlockList              map[peer.ID]bool
	ProviderAllowList              map[peer.ID]bool
	BitswapConcurrency             int
	BitswapConcurrencyPerRetrieval int
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
	if cfg.Source == nil {
		var err error
		cfg.Source, err = indexerlookup.NewCandidateSource(indexerlookup.WithHttpClient(&http.Client{}))
		if err != nil {
			return nil, err
		}
	}

	if cfg.ProviderTimeout == 0 {
		cfg.ProviderTimeout = DefaultProviderTimeout
	}
	if cfg.BitswapConcurrency == 0 {
		cfg.BitswapConcurrency = DefaultBitswapConcurrency
	}
	if cfg.BitswapConcurrencyPerRetrieval == 0 {
		cfg.BitswapConcurrencyPerRetrieval = DefaultBitswapConcurrencyPerRetrieval
	}

	datastore := sync.MutexWrap(datastore.NewMapDatastore())

	if cfg.Host == nil {
		var err error
		cfg.Host, err = host.InitHost(ctx, cfg.Libp2pOptions)
		if err != nil {
			return nil, err
		}
	}

	sessionConfig := session.DefaultConfig().
		WithProviderBlockList(cfg.ProviderBlockList).
		WithProviderAllowList(cfg.ProviderAllowList).
		WithDefaultProviderConfig(session.ProviderConfig{
			RetrievalTimeout:        cfg.ProviderTimeout,
			MaxConcurrentRetrievals: cfg.ConcurrentSPRetrievals,
		})
	session := session.NewSession(sessionConfig, true)

	if len(cfg.Protocols) == 0 {
		cfg.Protocols = []multicodec.Code{multicodec.TransportBitswap, multicodec.TransportGraphsyncFilecoinv1, multicodec.TransportIpfsGatewayHttp}
	}

	protocolRetrievers := make(map[multicodec.Code]types.CandidateRetriever)
	for _, protocol := range cfg.Protocols {
		switch protocol {
		case multicodec.TransportGraphsyncFilecoinv1:
			retrievalClient, err := client.NewClient(ctx, datastore, cfg.Host)
			if err != nil {
				return nil, err
			}

			if err := retrievalClient.AwaitReady(); err != nil { // wait for dt setup
				return nil, err
			}
			protocolRetrievers[protocol] = retriever.NewGraphsyncRetriever(session, retrievalClient)
		// DISABLED: bitswap support removed for boxo v0.35.0 compatibility
		// case multicodec.TransportBitswap:
		// 	protocolRetrievers[protocol] = retriever.NewBitswapRetrieverFromHost(ctx, cfg.Host, retriever.BitswapConfig{
		// 		BlockTimeout:            cfg.ProviderTimeout,
		// 		Concurrency:             cfg.BitswapConcurrency,
		// 		ConcurrencyPerRetrieval: cfg.BitswapConcurrencyPerRetrieval,
		// 	})
		case multicodec.TransportIpfsGatewayHttp:
			protocolRetrievers[protocol] = retriever.NewHttpRetriever(session, http.DefaultClient)
		}
	}

	retriever, err := retriever.NewRetriever(ctx, session, cfg.Source, protocolRetrievers)
	if err != nil {
		return nil, err
	}
	retriever.Start()

	lassie := &Sheltie{
		cfg:       cfg,
		retriever: retriever,
	}

	return lassie, nil
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

// WithHost allows you to specify a custom libp2p host.
func WithHost(host host.Host) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.Host = host
	}
}

// WithLibp2pOpts allows you to specify custom libp2p options.
func WithLibp2pOpts(libp2pOptions ...libp2p.Option) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.Libp2pOptions = libp2pOptions
	}
}

// WithConcurrentSPRetrievals allows you to specify a custom number of
// concurrent retrievals from a single storage provider.
func WithConcurrentSPRetrievals(maxConcurrentSPRtreievals uint) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.ConcurrentSPRetrievals = maxConcurrentSPRtreievals
	}
}

// WithProtocols allows you to specify a custom set of protocols to use for
// retrieval.
func WithProtocols(protocols []multicodec.Code) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.Protocols = protocols
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

// WithBitswapConcurrency allows you to specify a custom concurrency for bitswap
// retrievals across all parallel retrievals in the same Sheltie instance. This
// is applied using a preloader during traversals. The default is 32.
func WithBitswapConcurrency(concurrency int) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.BitswapConcurrency = concurrency
	}
}

// WithBitswapConcurrencyPerRetrieval allows you to specify a custom concurrency
// for bitswap retrievals for each individual parallel retrieval. This is
// applied using a preloader during traversals. The default is 8.
func WithBitswapConcurrencyPerRetrieval(concurrency int) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.BitswapConcurrencyPerRetrieval = concurrency
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

// MODIFIED: 2025-10-30
// - Renamed package from lassie to sheltie
// - Removed bitswap concurrency configuration and constants
// - Removed bitswap protocol initialization
// - Updated default protocols to [graphsync, http] (removed bitswap)
// - Added protocol filtering to IndexerCandidateSource initialization
// - Moved protocol defaults before candidate source creation

package lassie

import (
	"context"
	"net/http"
	"time"

	"github.com/filecoin-project/lassie/pkg/indexerlookup"
	"github.com/filecoin-project/lassie/pkg/net/client"
	"github.com/filecoin-project/lassie/pkg/net/host"
	"github.com/filecoin-project/lassie/pkg/retriever"
	"github.com/filecoin-project/lassie/pkg/session"
	"github.com/filecoin-project/lassie/pkg/types"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
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
	Source                 types.CandidateSource
	Host                   host.Host
	ProviderTimeout        time.Duration
	ConcurrentSPRetrievals uint
	GlobalTimeout          time.Duration
	Libp2pOptions          []libp2p.Option
	Protocols              []multicodec.Code
	ProviderBlockList      map[peer.ID]bool
	ProviderAllowList      map[peer.ID]bool
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
	// Set default protocols first, before creating candidate source
	if len(cfg.Protocols) == 0 {
		cfg.Protocols = []multicodec.Code{multicodec.TransportGraphsyncFilecoinv1, multicodec.TransportIpfsGatewayHttp}
	}

	if cfg.Source == nil {
		var err error
		cfg.Source, err = indexerlookup.NewCandidateSource(
			indexerlookup.WithHttpClient(&http.Client{}),
			indexerlookup.WithProtocols(cfg.Protocols),
		)
		if err != nil {
			return nil, err
		}
	}

	if cfg.ProviderTimeout == 0 {
		cfg.ProviderTimeout = DefaultProviderTimeout
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
		case multicodec.TransportIpfsGatewayHttp:
			protocolRetrievers[protocol] = retriever.NewHttpRetriever(session, http.DefaultClient)
		}
	}

	retriever, err := retriever.NewRetriever(ctx, session, cfg.Source, protocolRetrievers)
	if err != nil {
		return nil, err
	}
	retriever.Start()

	lassie := &Lassie{
		cfg:       cfg,
		retriever: retriever,
	}

	return lassie, nil
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

// WithHost allows you to specify a custom libp2p host.
func WithHost(host host.Host) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Host = host
	}
}

// WithLibp2pOpts allows you to specify custom libp2p options.
func WithLibp2pOpts(libp2pOptions ...libp2p.Option) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Libp2pOptions = libp2pOptions
	}
}

// WithConcurrentSPRetrievals allows you to specify a custom number of
// concurrent retrievals from a single storage provider.
func WithConcurrentSPRetrievals(maxConcurrentSPRtreievals uint) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.ConcurrentSPRetrievals = maxConcurrentSPRtreievals
	}
}

// WithProtocols allows you to specify a custom set of protocols to use for
// retrieval.
func WithProtocols(protocols []multicodec.Code) LassieOption {
	return func(cfg *LassieConfig) {
		cfg.Protocols = protocols
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

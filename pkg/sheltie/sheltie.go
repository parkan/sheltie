package sheltie

import (
	"context"
	"net/http"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/parkan/sheltie/pkg/extractor"
	"github.com/parkan/sheltie/pkg/indexerlookup"
	"github.com/parkan/sheltie/pkg/retriever"
	"github.com/parkan/sheltie/pkg/session"
	"github.com/parkan/sheltie/pkg/types"
)

var _ types.Fetcher = &Sheltie{}

type Sheltie struct {
	cfg       *SheltieConfig
	retriever *retriever.Retriever
}

type SheltieConfig struct {
	Source                types.CandidateSource
	GlobalTimeout         time.Duration
	ProviderBlockList     map[peer.ID]bool
	ProviderAllowList     map[peer.ID]bool
	SkipBlockVerification bool
}

type SheltieOption func(cfg *SheltieConfig)

func NewSheltie(ctx context.Context, opts ...SheltieOption) (*Sheltie, error) {
	cfg := NewSheltieConfig(opts...)
	return NewSheltieWithConfig(ctx, cfg)
}

func NewSheltieConfig(opts ...SheltieOption) *SheltieConfig {
	cfg := &SheltieConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return cfg
}

func NewSheltieWithConfig(ctx context.Context, cfg *SheltieConfig) (*Sheltie, error) {
	// http-only protocol
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

	sessionConfig := session.DefaultConfig().
		WithProviderBlockList(cfg.ProviderBlockList).
		WithProviderAllowList(cfg.ProviderAllowList)
	sess := session.NewSession(sessionConfig, true)

	httpRetriever := retriever.NewHttpRetriever(sess, http.DefaultClient)
	ret, err := retriever.NewRetriever(ctx, sess, cfg.Source, httpRetriever, multicodec.TransportIpfsGatewayHttp)
	if err != nil {
		return nil, err
	}

	ret.WrapWithHybrid(cfg.Source, http.DefaultClient, cfg.SkipBlockVerification)

	ret.Start()

	s := &Sheltie{
		cfg:       cfg,
		retriever: ret,
	}

	return s, nil
}

func WithCandidateSource(finder types.CandidateSource) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.Source = finder
	}
}

func WithGlobalTimeout(timeout time.Duration) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.GlobalTimeout = timeout
	}
}

func WithProviderBlockList(providerBlockList map[peer.ID]bool) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.ProviderBlockList = providerBlockList
	}
}

func WithProviderAllowList(providerAllowList map[peer.ID]bool) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.ProviderAllowList = providerAllowList
	}
}

// WARNING: malicious gateways can serve arbitrary data with verification disabled
func WithSkipBlockVerification(skip bool) SheltieOption {
	return func(cfg *SheltieConfig) {
		cfg.SkipBlockVerification = skip
	}
}

func (s *Sheltie) Fetch(ctx context.Context, request types.RetrievalRequest, opts ...types.FetchOption) (*types.RetrievalStats, error) {
	var cancel context.CancelFunc
	if s.cfg.GlobalTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, s.cfg.GlobalTimeout)
		defer cancel()
	}
	return s.retriever.Retrieve(ctx, request, types.NewFetchConfig(opts...).EventsCallback)
}

func (s *Sheltie) RegisterSubscriber(subscriber types.RetrievalEventSubscriber) func() {
	return s.retriever.RegisterSubscriber(subscriber)
}

// Extract retrieves content and extracts it directly to disk.
// blocks are processed once and discarded.
func (s *Sheltie) Extract(
	ctx context.Context,
	rootCid cid.Cid,
	ext *extractor.Extractor,
	eventsCallback func(types.RetrievalEvent),
	onBlock func(int),
) (*types.RetrievalStats, error) {
	var cancel context.CancelFunc
	if s.cfg.GlobalTimeout != time.Duration(0) {
		ctx, cancel = context.WithTimeout(ctx, s.cfg.GlobalTimeout)
		defer cancel()
	}
	return s.retriever.RetrieveAndExtract(ctx, rootCid, ext, eventsCallback, onBlock)
}

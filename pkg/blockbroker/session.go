package blockbroker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/parkan/sheltie/pkg/types"
)

var logger = log.Logger("sheltie/blockbroker")

var _ BlockSession = (*TrustlessGatewaySession)(nil)

// TrustlessGatewaySession implements BlockSession using HTTP trustless gateways.
// It maintains a cache of providers discovered during block fetching and
// performs per-block provider discovery when needed.
type TrustlessGatewaySession struct {
	routing    types.CandidateSource
	httpClient *http.Client

	providers     []types.RetrievalCandidate
	providersLock sync.RWMutex

	// Track failed providers with backoff
	evictedPeers     map[peer.ID]time.Time
	evictedPeersLock sync.RWMutex
	evictBackoff     time.Duration
}

// NewSession creates a new TrustlessGatewaySession for per-block HTTP fetching.
func NewSession(
	routing types.CandidateSource,
	httpClient *http.Client,
) *TrustlessGatewaySession {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	return &TrustlessGatewaySession{
		routing:      routing,
		httpClient:   httpClient,
		providers:    make([]types.RetrievalCandidate, 0),
		evictedPeers: make(map[peer.ID]time.Time),
		evictBackoff: 30 * time.Second,
	}
}

// Get fetches a single block by CID.
// It first tries cached providers, then discovers new ones if needed.
func (s *TrustlessGatewaySession) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	// 1. Try cached providers (parallel, first success wins)
	providers := s.getProviders()
	if len(providers) > 0 {
		block, err := s.tryProviders(ctx, c, providers)
		if err == nil {
			return block, nil
		}
		logger.Debugw("all cached providers failed", "cid", c, "err", err)
		// All cached providers failed, continue to discovery
	}

	// 2. Discover new providers for this CID
	if err := s.findNewProviders(ctx, c); err != nil {
		return nil, fmt.Errorf("no providers found for %s: %w", c, err)
	}

	// 3. Retry with new providers
	providers = s.getProviders()
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers available for %s", c)
	}
	return s.tryProviders(ctx, c, providers)
}

func (s *TrustlessGatewaySession) getProviders() []types.RetrievalCandidate {
	s.providersLock.RLock()
	defer s.providersLock.RUnlock()
	result := make([]types.RetrievalCandidate, len(s.providers))
	copy(result, s.providers)
	return result
}

// tryProviders attempts to fetch a block from the given providers in parallel.
// Returns the first successful result.
func (s *TrustlessGatewaySession) tryProviders(ctx context.Context, c cid.Cid, providers []types.RetrievalCandidate) (blocks.Block, error) {
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers available")
	}

	// Parallel fetch with first-success-wins
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type fetchResult struct {
		block    blocks.Block
		err      error
		provider types.RetrievalCandidate
	}

	results := make(chan fetchResult, len(providers))
	for _, p := range providers {
		go func(provider types.RetrievalCandidate) {
			block, err := s.fetchBlock(ctx, c, provider)
			select {
			case results <- fetchResult{block: block, err: err, provider: provider}:
			case <-ctx.Done():
			}
		}(p)
	}

	var lastErr error
	for range providers {
		select {
		case result := <-results:
			if result.err != nil {
				logger.Debugw("provider failed", "cid", c, "provider", result.provider.MinerPeer.ID, "err", result.err)
				s.evictProvider(result.provider.MinerPeer.ID)
				lastErr = result.err
				continue
			}
			cancel() // Got success, cancel other requests
			return result.block, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("all providers failed: %w", lastErr)
}

// fetchBlock fetches a single block from a provider using the trustless gateway protocol.
// Per the spec: GET /ipfs/{cid}?format=raw with Accept: application/vnd.ipld.raw
func (s *TrustlessGatewaySession) fetchBlock(
	ctx context.Context,
	c cid.Cid,
	provider types.RetrievalCandidate,
) (blocks.Block, error) {
	url, err := provider.ToURL()
	if err != nil {
		return nil, fmt.Errorf("failed to get URL for provider: %w", err)
	}

	// Per Trustless Gateway spec:
	// - Raw block: GET /ipfs/{cid}?format=raw (no dag-scope parameter)
	// - dag-scope is only valid for CAR responses
	reqURL := fmt.Sprintf("%s/ipfs/%s?format=raw", url, c)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "application/vnd.ipld.raw")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
	}

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Verify CID matches the data
	block, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return nil, fmt.Errorf("block verification failed: %w", err)
	}

	return block, nil
}

// findNewProviders queries the routing system for providers of the given CID.
func (s *TrustlessGatewaySession) findNewProviders(ctx context.Context, c cid.Cid) error {
	var found int
	var mu sync.Mutex

	err := s.routing.FindCandidates(ctx, c, func(candidate types.RetrievalCandidate) {
		// Filter for HTTP protocol
		if !s.hasHTTPProtocol(candidate) {
			return
		}
		// Skip recently evicted providers
		if s.isEvicted(candidate.MinerPeer.ID) {
			return
		}

		mu.Lock()
		s.addProvider(candidate)
		found++
		mu.Unlock()
	})

	if err != nil {
		return err
	}
	if found == 0 {
		return fmt.Errorf("no HTTP providers found for %s", c)
	}
	logger.Debugw("found new providers", "cid", c, "count", found)
	return nil
}

// SeedProviders pre-populates the session with providers for a given CID.
// This is useful when falling back from whole-DAG retrieval to reuse
// providers that may have had partial content.
func (s *TrustlessGatewaySession) SeedProviders(ctx context.Context, c cid.Cid) {
	_ = s.findNewProviders(ctx, c) // Ignore error - seeding is best-effort
}

func (s *TrustlessGatewaySession) addProvider(candidate types.RetrievalCandidate) {
	s.providersLock.Lock()
	defer s.providersLock.Unlock()

	// Check for duplicate
	for _, p := range s.providers {
		if p.MinerPeer.ID == candidate.MinerPeer.ID {
			return
		}
	}

	s.providers = append(s.providers, candidate)
}

func (s *TrustlessGatewaySession) evictProvider(peerID peer.ID) {
	s.evictedPeersLock.Lock()
	s.evictedPeers[peerID] = time.Now()
	s.evictedPeersLock.Unlock()

	s.providersLock.Lock()
	defer s.providersLock.Unlock()

	for i, p := range s.providers {
		if p.MinerPeer.ID == peerID {
			s.providers = append(s.providers[:i], s.providers[i+1:]...)
			return
		}
	}
}

func (s *TrustlessGatewaySession) isEvicted(peerID peer.ID) bool {
	s.evictedPeersLock.RLock()
	defer s.evictedPeersLock.RUnlock()

	evictTime, ok := s.evictedPeers[peerID]
	if !ok {
		return false
	}
	// Allow retry after backoff period
	return time.Since(evictTime) < s.evictBackoff
}

func (s *TrustlessGatewaySession) hasHTTPProtocol(candidate types.RetrievalCandidate) bool {
	// Check if any multiaddr is HTTP/HTTPS
	for _, addr := range candidate.MinerPeer.Addrs {
		for _, proto := range addr.Protocols() {
			if proto.Name == "http" || proto.Name == "https" {
				return true
			}
		}
	}
	return false
}

// Close releases session resources.
func (s *TrustlessGatewaySession) Close() error {
	// Clean up resources if needed
	s.providersLock.Lock()
	s.providers = nil
	s.providersLock.Unlock()

	s.evictedPeersLock.Lock()
	s.evictedPeers = nil
	s.evictedPeersLock.Unlock()

	return nil
}

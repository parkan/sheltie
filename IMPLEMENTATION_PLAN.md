# Sheltie: HTTP-Only Per-Block Retrieval Implementation Plan

## Overview

This document outlines the plan to transform Sheltie from a whole-DAG retrieval client into an HTTP-only client that can stitch DAGs across multiple providers using per-block fetching with fallback.

### Problem Statement

Currently, Sheltie fetches entire DAGs from a single provider. If that provider only has partial content (e.g., directory descriptors without leaf blocks), the retrieval fails.

**Goal**: Fetch as much as possible from the first provider, then seamlessly fall back to per-block fetching from other providers discovered via delegated routing.

### Architecture

```
Request(rootCid, selector)
    │
    ▼
Phase 1: Optimistic Whole-DAG Fetch
    │
    ├─► Success: return stats
    │
    └─► ErrMissingBlock(cid):
            │
            ▼
Phase 2: Per-Block Fallback
    - Blocks fetched so far are already in LinkSystem storage
    - Create BlockSession for remaining traversal
    - FindCandidates(missingCid) to discover new providers
    - Resume traversal with session-backed LinkSystem
    - Session does per-block provider discovery as needed
```

---

## Phase 1: Remove Graphsync Support

Simplify the codebase by removing Graphsync, keeping only HTTP trustless gateway support.

### Files to Delete

| File | Reason |
|------|--------|
| `pkg/retriever/graphsyncretriever.go` | Graphsync retriever implementation |
| `pkg/retriever/graphsyncretriever_test.go` | Tests for above |
| `pkg/events/graphsyncaccepted.go` | Graphsync-specific event |
| `pkg/events/graphsyncproposed.go` | Graphsync-specific event |
| `pkg/net/client/` | Entire directory - graphsync/data-transfer client |
| `pkg/net/host/` | Entire directory - libp2p host management |
| `pkg/internal/lp2ptransports/` | Entire directory - libp2p transport parsing |

### Files to Modify

#### `pkg/sheltie/sheltie.go`
- Remove `multicodec.TransportGraphsyncFilecoinv1` case from protocol switch
- Remove libp2p host initialization (`cfg.Host`, `host.InitHost`)
- Remove datastore initialization (only needed for graphsync)
- Remove `client.NewClient` import and usage
- Simplify `SheltieConfig` - remove `Host`, `Libp2pOptions` fields
- Change default protocols to just `[TransportIpfsGatewayHttp]`

#### `pkg/types/request.go`
- Remove "graphsync" case from `ParseProtocolsString()`
- Remove graphsync from `ParseProviderStrings()` protocol parsing
- Remove graphsync from `ToProviderString()`

#### `cmd/sheltie/flags.go`
- Change `FlagProtocols` default text from "graphsync,http" to "http"
- Update usage text to remove graphsync references

#### `pkg/retriever/protocolsplitter.go`
- Remove graphsync handling if present

#### `pkg/session/state.go`
- Remove any graphsync-specific session state

#### Test files
- `pkg/retriever/retriever_test.go` - remove graphsync test cases
- `pkg/session/state_test.go` - remove graphsync references
- `cmd/sheltie/fetch_test.go` - update protocol expectations
- Various integration tests in `pkg/internal/itest/`

### Dependencies to Remove from `go.mod`

After removal, run `go mod tidy` to remove:
- `github.com/filecoin-project/go-data-transfer/v2`
- `github.com/ipfs/go-graphsync`
- `github.com/filecoin-project/go-retrieval-types`
- Many transitive libp2p dependencies (some may remain for peer ID handling)

---

## Phase 2: Implement BlockSession

New package `pkg/blockbroker/` for per-block HTTP fetching with provider caching.

### New Files

#### `pkg/blockbroker/types.go`

```go
package blockbroker

import (
    "context"
    blocks "github.com/ipfs/go-block-format"
    "github.com/ipfs/go-cid"
)

// BlockSession manages per-block fetching with provider caching
type BlockSession interface {
    // Get fetches a block, trying cached providers first, then discovering new ones
    Get(ctx context.Context, c cid.Cid) (blocks.Block, error)
    // Close releases session resources
    Close() error
}

// BlockBroker creates sessions for block fetching
type BlockBroker interface {
    CreateSession(ctx context.Context, root cid.Cid, opts ...SessionOption) (BlockSession, error)
}

type SessionOption func(*sessionConfig)

type sessionConfig struct {
    minProviders int
    maxProviders int
}

func WithMinProviders(n int) SessionOption {
    return func(c *sessionConfig) { c.minProviders = n }
}

func WithMaxProviders(n int) SessionOption {
    return func(c *sessionConfig) { c.maxProviders = n }
}
```

#### `pkg/blockbroker/session.go`

```go
package blockbroker

import (
    "bytes"
    "context"
    "fmt"
    "io"
    "net/http"
    "sync"
    "time"

    blocks "github.com/ipfs/go-block-format"
    "github.com/ipfs/go-cid"
    "github.com/libp2p/go-libp2p/core/peer"
    "github.com/parkan/sheltie/pkg/types"
)

type TrustlessGatewaySession struct {
    routing       types.CandidateSource
    httpClient    *http.Client

    providers     []types.RetrievalCandidate
    providersLock sync.RWMutex

    minProviders  int
    maxProviders  int

    // Track failed providers with backoff
    evictedPeers     map[peer.ID]time.Time
    evictedPeersLock sync.RWMutex
    evictBackoff     time.Duration
}

func NewSession(
    routing types.CandidateSource,
    httpClient *http.Client,
    opts ...SessionOption,
) *TrustlessGatewaySession {
    cfg := &sessionConfig{
        minProviders: 1,
        maxProviders: 10,
    }
    for _, opt := range opts {
        opt(cfg)
    }

    return &TrustlessGatewaySession{
        routing:      routing,
        httpClient:   httpClient,
        providers:    make([]types.RetrievalCandidate, 0),
        evictedPeers: make(map[peer.ID]time.Time),
        evictBackoff: 30 * time.Second,
        minProviders: cfg.minProviders,
        maxProviders: cfg.maxProviders,
    }
}

func (s *TrustlessGatewaySession) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
    // 1. Try cached providers (parallel, first success wins)
    if len(s.getProviders()) > 0 {
        block, err := s.tryProviders(ctx, c)
        if err == nil {
            return block, nil
        }
        // All cached providers failed, continue to discovery
    }

    // 2. Discover new providers for this CID
    if err := s.findNewProviders(ctx, c); err != nil {
        return nil, fmt.Errorf("no providers found for %s: %w", c, err)
    }

    // 3. Retry with new providers
    return s.tryProviders(ctx, c)
}

func (s *TrustlessGatewaySession) getProviders() []types.RetrievalCandidate {
    s.providersLock.RLock()
    defer s.providersLock.RUnlock()
    result := make([]types.RetrievalCandidate, len(s.providers))
    copy(result, s.providers)
    return result
}

func (s *TrustlessGatewaySession) tryProviders(ctx context.Context, c cid.Cid) (blocks.Block, error) {
    providers := s.getProviders()
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

func (s *TrustlessGatewaySession) fetchBlock(
    ctx context.Context,
    c cid.Cid,
    provider types.RetrievalCandidate,
) (blocks.Block, error) {
    url, err := provider.ToURL()
    if err != nil {
        return nil, err
    }

    // Per Trustless Gateway spec:
    // - Raw block: GET /ipfs/{cid}?format=raw (no dag-scope parameter)
    // - dag-scope is only valid for CAR responses
    // Using format=raw for single block fetches
    reqURL := fmt.Sprintf("%s/ipfs/%s?format=raw", url, c)
    req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
    if err != nil {
        return nil, err
    }
    req.Header.Set("Accept", "application/vnd.ipld.raw")

    resp, err := s.httpClient.Do(req)
    if err != nil {
        return nil, err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        return nil, fmt.Errorf("HTTP %d from %s", resp.StatusCode, url)
    }

    data, err := io.ReadAll(resp.Body)
    if err != nil {
        return nil, err
    }

    // Verify CID matches the data
    block, err := blocks.NewBlockWithCid(data, c)
    if err != nil {
        return nil, fmt.Errorf("block verification failed: %w", err)
    }

    return block, nil
}

func (s *TrustlessGatewaySession) findNewProviders(ctx context.Context, c cid.Cid) error {
    var found int
    var mu sync.Mutex

    err := s.routing.FindCandidates(ctx, c, func(candidate types.RetrievalCandidate) {
        // Filter for HTTP protocol
        if !hasHTTPProtocol(candidate) {
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
    return nil
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

    // Enforce max providers
    if len(s.providers) >= s.maxProviders {
        return
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

func (s *TrustlessGatewaySession) Close() error {
    // Clean up resources if needed
    return nil
}

func hasHTTPProtocol(candidate types.RetrievalCandidate) bool {
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
```

#### `pkg/blockbroker/session_test.go`

Unit tests for the session implementation.

---

## Phase 3: Implement HybridRetriever

New retriever that tries whole-DAG first, then falls back to per-block.

### New Files

#### `pkg/retriever/hybridretriever.go`

```go
package retriever

import (
    "bytes"
    "context"
    "errors"
    "io"
    "net/http"

    "github.com/filecoin-project/go-clock"
    format "github.com/ipfs/go-ipld-format"
    "github.com/ipfs/go-cid"
    "github.com/ipld/go-ipld-prime/datamodel"
    "github.com/ipld/go-ipld-prime/linking"
    cidlink "github.com/ipld/go-ipld-prime/linking/cid"
    "github.com/ipld/go-trustless-utils/traversal"
    "github.com/parkan/sheltie/pkg/blockbroker"
    "github.com/parkan/sheltie/pkg/types"
)

type HybridRetriever struct {
    candidateSource types.CandidateSource
    httpClient      *http.Client
    clock           clock.Clock
    session         Session
}

func NewHybridRetriever(
    candidateSource types.CandidateSource,
    httpClient *http.Client,
    session Session,
) *HybridRetriever {
    return &HybridRetriever{
        candidateSource: candidateSource,
        httpClient:      httpClient,
        clock:           clock.New(),
        session:         session,
    }
}

func (hr *HybridRetriever) Retrieve(
    ctx context.Context,
    request types.RetrievalRequest,
    events func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {

    // Phase 1: Try whole-DAG from discovered providers
    stats, err := hr.tryWholeDag(ctx, request, events)
    if err == nil {
        return stats, nil
    }

    // Check if it's a "missing block" error we can recover from
    missingCid, ok := extractMissingCid(err)
    if !ok {
        return nil, err // Not recoverable
    }

    // Phase 2: Fall back to per-block fetching
    // Note: The LinkSystem already has blocks from partial fetch
    return hr.continuePerBlock(ctx, request, missingCid, events)
}

func (hr *HybridRetriever) tryWholeDag(
    ctx context.Context,
    request types.RetrievalRequest,
    events func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {
    // Use existing HTTP retriever logic
    httpRetriever := NewHttpRetriever(hr.session, hr.httpClient)
    retrieval := httpRetriever.Retrieve(ctx, request, events)

    // Find candidates and try them
    inbound, outbound := types.MakeAsyncCandidates(8)

    go func() {
        defer close(outbound)
        hr.candidateSource.FindCandidates(ctx, request.Root, func(candidate types.RetrievalCandidate) {
            outbound.SendNext(ctx, []types.RetrievalCandidate{candidate})
        })
    }()

    return retrieval.RetrieveFromAsyncCandidates(inbound)
}

func (hr *HybridRetriever) continuePerBlock(
    ctx context.Context,
    request types.RetrievalRequest,
    startingCid cid.Cid,
    events func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {

    // Create a block session for per-block fetching
    session := blockbroker.NewSession(hr.candidateSource, hr.httpClient)
    defer session.Close()

    // Bootstrap session with providers for the missing CID
    // (The session will discover providers on first Get call)

    // Create a LinkSystem that:
    // 1. Checks existing storage first (blocks from partial fetch)
    // 2. Falls back to session.Get() for missing blocks
    lsys := hr.makeSessionBackedLinkSystem(request.LinkSystem, session)

    // Resume traversal from root
    // Already-fetched blocks will be found in storage
    cfg := traversal.Config{
        Root:     request.Root,
        Selector: request.GetSelector(),
    }

    result, err := cfg.Traverse(ctx, lsys, nil)
    if err != nil {
        return nil, err
    }

    // TODO: Aggregate stats from both phases
    return &types.RetrievalStats{
        RootCid:  request.Root,
        Blocks:   result.BlocksOut,
        Size:     result.BytesOut,
        Duration: hr.clock.Since(hr.clock.Now()), // TODO: track properly
    }, nil
}

func (hr *HybridRetriever) makeSessionBackedLinkSystem(
    baseLsys linking.LinkSystem,
    session blockbroker.BlockSession,
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
            // Not found in storage, continue to session
        }

        // Fetch via session (with per-block provider discovery)
        block, err := session.Get(lc.Ctx, c)
        if err != nil {
            return nil, err
        }

        // Optionally write to storage for future traversals
        if lsys.StorageWriteOpener != nil {
            w, wc, err := lsys.StorageWriteOpener(lc)
            if err == nil {
                io.Copy(w, bytes.NewReader(block.RawData()))
                wc(l)
            }
        }

        return bytes.NewReader(block.RawData()), nil
    }

    return lsys
}

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
    }

    return cid.Undef, false
}
```

---

## Phase 4: Integration

### Modify `pkg/sheltie/sheltie.go`

Replace the protocol-based retriever map with the hybrid retriever:

```go
func NewSheltieWithConfig(ctx context.Context, cfg *SheltieConfig) (*Sheltie, error) {
    if cfg.Source == nil {
        var err error
        cfg.Source, err = indexerlookup.NewCandidateSource(
            indexerlookup.WithHttpClient(&http.Client{}),
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

    // Use hybrid retriever instead of protocol-specific retrievers
    hybridRetriever := retriever.NewHybridRetriever(
        cfg.Source,
        http.DefaultClient,
        sess,
    )

    ret, err := retriever.NewRetrieverWithHybrid(ctx, sess, hybridRetriever)
    if err != nil {
        return nil, err
    }
    ret.Start()

    return &Sheltie{
        cfg:       cfg,
        retriever: ret,
    }, nil
}
```

---

## Phase 5: Testing

### Unit Tests
- `pkg/blockbroker/session_test.go` - Test provider discovery, caching, eviction
- `pkg/retriever/hybridretriever_test.go` - Test fallback behavior

### Integration Tests
- Create test fixtures with partial DAGs
- Test Set A (shallow) + Set B (deep) provider scenario
- Test various failure modes (all providers fail, timeout, etc.)

### End-to-End Tests
- Update `pkg/internal/itest/` tests for HTTP-only behavior
- Remove graphsync-specific integration tests

---

## Migration Checklist

- [ ] Delete graphsync files
- [ ] Modify sheltie.go to remove graphsync
- [ ] Update flags.go default protocols
- [ ] Update types/request.go protocol parsing
- [ ] Run `go mod tidy`
- [ ] Implement `pkg/blockbroker/`
- [ ] Implement `pkg/retriever/hybridretriever.go`
- [ ] Wire up hybrid retriever in sheltie.go
- [ ] Update/add tests
- [ ] Update README.md

---

## Future Optimizations

1. **Provider Affinity**: Remember which provider had which blocks for locality
2. **Parallel Prefetching**: Request likely-needed blocks ahead of traversal
3. **Smarter Backoff**: Exponential backoff for failed providers
4. **Metrics**: Track per-provider success rates, latencies
5. **CAR Streaming**: For large files from known-complete providers, stream CAR instead of per-block

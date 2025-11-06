package types

import (
	"fmt"
	"io"

	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// TraversalMode defines how the DAG should be traversed
type TraversalMode int

const (
	// TraversalDFS is the default depth-first traversal
	TraversalDFS TraversalMode = iota
	// TraversalBFS uses breadth-first traversal
	TraversalBFS
	// TraversalBFSAdaptive uses BFS with automatic HAMT detection
	TraversalBFSAdaptive
)

// TraversalConfig configures DAG traversal behavior
type TraversalConfig struct {
	// Mode specifies the traversal algorithm
	Mode TraversalMode
	// MaxDepth limits BFS levels before switching to DFS (0 = unlimited)
	MaxDepth int
	// HAMTAware enables automatic depth adjustment for HAMT structures
	HAMTAware bool
	// StreamBlocks enables immediate block streaming without buffering
	StreamBlocks bool
	// AllowFallback enables querying other providers on missing blocks
	AllowFallback bool
}

// DefaultTraversalConfig returns the default traversal configuration
func DefaultTraversalConfig() TraversalConfig {
	return TraversalConfig{
		Mode:          TraversalDFS,
		MaxDepth:      0,
		HAMTAware:     false,
		StreamBlocks:  false,
		AllowFallback: false,
	}
}

// BFSTraversalConfig returns a configuration for BFS traversal
func BFSTraversalConfig(maxDepth int) TraversalConfig {
	return TraversalConfig{
		Mode:          TraversalBFS,
		MaxDepth:      maxDepth,
		HAMTAware:     true,
		StreamBlocks:  true,
		AllowFallback: true,
	}
}

// BuildBFSSelector creates an IPLD selector for BFS traversal
func BuildBFSSelector(maxDepth int) (ipld.Node, error) {
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)

	if maxDepth <= 0 {
		// unlimited depth - use explore-all recursive
		return ssb.ExploreRecursive(
			selector.RecursionLimitNone(),
			ssb.ExploreAll(
				ssb.ExploreRecursiveEdge(),
			),
		).Node(), nil
	}

	// build a level-limited BFS selector
	return ssb.ExploreRecursive(
		selector.RecursionLimitDepth(int64(maxDepth)),
		ssb.ExploreAll(
			ssb.ExploreRecursiveEdge(),
		),
	).Node(), nil
}

// BuildDFSSelector creates an IPLD selector for DFS traversal
func BuildDFSSelector() ipld.Node {
	// standard recursive exploration with builder
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	return ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreAll(
			ssb.ExploreRecursiveEdge(),
		),
	).Node()
}

// BuildAdaptiveSelector creates a selector that adapts to the DAG structure
func BuildAdaptiveSelector(node datamodel.Node) (ipld.Node, error) {
	// check if this looks like a HAMT node
	if IsLikelyHAMT(node) {
		// use deeper BFS for HAMT structures
		return BuildBFSSelector(3)
	}

	// default to shallow BFS
	return BuildBFSSelector(1)
}

// IsLikelyHAMT attempts to detect HAMT-sharded directory structures
func IsLikelyHAMT(node datamodel.Node) bool {
	// hamt nodes typically have:
	// - a specific set of field names (e.g., "HashMurmur3", "Fanout")
	// - multiple hash-like keys as children

	if node.Kind() != datamodel.Kind_Map {
		return false
	}

	// check for HAMT-specific fields
	hamtIndicators := []string{"HashMurmur3", "Fanout", "Buckets"}
	foundIndicators := 0

	iter := node.MapIterator()
	for !iter.Done() {
		key, _, err := iter.Next()
		if err != nil {
			return false
		}

		keyStr, err := key.AsString()
		if err != nil {
			continue
		}

		for _, indicator := range hamtIndicators {
			if keyStr == indicator {
				foundIndicators++
			}
		}

		// also check for hash-like keys (e.g., base58 strings)
		if len(keyStr) > 40 && isBase58Like(keyStr) {
			foundIndicators++
		}
	}

	// if we found multiple indicators, likely a HAMT
	return foundIndicators >= 2
}

// isBase58Like checks if a string looks like a base58 hash
func isBase58Like(s string) bool {
	// simple heuristic: check if string contains only base58 characters
	for _, c := range s {
		if !((c >= '1' && c <= '9') ||
			(c >= 'A' && c <= 'H') ||
			(c >= 'J' && c <= 'N') ||
			(c >= 'P' && c <= 'Z') ||
			(c >= 'a' && c <= 'k') ||
			(c >= 'm' && c <= 'z')) {
			return false
		}
	}
	return true
}

// MissingBlockHandler handles missing blocks during traversal
type MissingBlockHandler interface {
	// OnMissingBlock is called when a block is not available from current provider
	OnMissingBlock(c datamodel.Link) error
	// ShouldFallback returns whether to attempt fallback retrieval
	ShouldFallback() bool
	// GetMissingBlocks returns all blocks that were missing
	GetMissingBlocks() []datamodel.Link
}

// SimpleMissingBlockHandler is a basic implementation of MissingBlockHandler
type SimpleMissingBlockHandler struct {
	missingBlocks []datamodel.Link
	fallback      bool
}

// NewMissingBlockHandler creates a new missing block handler
func NewMissingBlockHandler(enableFallback bool) MissingBlockHandler {
	return &SimpleMissingBlockHandler{
		missingBlocks: make([]datamodel.Link, 0),
		fallback:      enableFallback,
	}
}

// OnMissingBlock records a missing block
func (h *SimpleMissingBlockHandler) OnMissingBlock(c datamodel.Link) error {
	h.missingBlocks = append(h.missingBlocks, c)
	return nil
}

// ShouldFallback returns whether fallback is enabled
func (h *SimpleMissingBlockHandler) ShouldFallback() bool {
	return h.fallback && len(h.missingBlocks) > 0
}

// GetMissingBlocks returns the list of missing blocks
func (h *SimpleMissingBlockHandler) GetMissingBlocks() []datamodel.Link {
	return h.missingBlocks
}

// String returns a string representation of the traversal mode
func (m TraversalMode) String() string {
	switch m {
	case TraversalDFS:
		return "DFS"
	case TraversalBFS:
		return "BFS"
	case TraversalBFSAdaptive:
		return "BFS-Adaptive"
	default:
		return fmt.Sprintf("Unknown(%d)", m)
	}
}

// RetrievalState tracks the state of a retrieval including which blocks have been received
type RetrievalState struct {
	// blocks that have been successfully retrieved
	receivedBlocks map[string]struct{}
	// blocks that were attempted but missing from the provider
	missingBlocks []datamodel.Link
}

// NewRetrievalState creates a new retrieval state tracker
func NewRetrievalState() *RetrievalState {
	return &RetrievalState{
		receivedBlocks: make(map[string]struct{}),
		missingBlocks:  make([]datamodel.Link, 0),
	}
}

// MarkReceived marks a block as successfully received
func (rs *RetrievalState) MarkReceived(c datamodel.Link) {
	rs.receivedBlocks[c.String()] = struct{}{}
}

// MarkMissing records a block that was missing during traversal
func (rs *RetrievalState) MarkMissing(c datamodel.Link) {
	rs.missingBlocks = append(rs.missingBlocks, c)
}

// HasReceived returns whether a block was already received
func (rs *RetrievalState) HasReceived(c datamodel.Link) bool {
	_, ok := rs.receivedBlocks[c.String()]
	return ok
}

// GetMissingBlocks returns all blocks that were missing
func (rs *RetrievalState) GetMissingBlocks() []datamodel.Link {
	return rs.missingBlocks
}

// HasMissingBlocks returns whether any blocks are missing
func (rs *RetrievalState) HasMissingBlocks() bool {
	return len(rs.missingBlocks) > 0
}

// MissingBlockTrackingLinkSystem wraps a LinkSystem to track which blocks
// are missing during traversal, allowing for fallback retrieval
type MissingBlockTrackingLinkSystem struct {
	inner *ipld.LinkSystem
	state *RetrievalState
}

// NewMissingBlockTrackingLinkSystem creates a wrapper around a LinkSystem
// that tracks missing blocks during load operations
func NewMissingBlockTrackingLinkSystem(lsys *ipld.LinkSystem, state *RetrievalState) *MissingBlockTrackingLinkSystem {
	return &MissingBlockTrackingLinkSystem{
		inner: lsys,
		state: state,
	}
}

// WrapStorageReadOpener wraps the LinkSystem's StorageReadOpener to track
// blocks as they are loaded and record any that are missing
func (m *MissingBlockTrackingLinkSystem) WrapStorageReadOpener() {
	originalOpener := m.inner.StorageReadOpener
	if originalOpener == nil {
		return
	}

	m.inner.StorageReadOpener = func(lc ipld.LinkContext, l datamodel.Link) (io.Reader, error) {
		reader, err := originalOpener(lc, l)
		if err != nil {
			// record this block as missing
			m.state.MarkMissing(l)
			return nil, err
		}
		// record this block as successfully received
		m.state.MarkReceived(l)
		return reader, nil
	}
}

// GetInner returns the wrapped LinkSystem
func (m *MissingBlockTrackingLinkSystem) GetInner() *ipld.LinkSystem {
	return m.inner
}

// ContinueOnErrorReader wraps a StorageReadOpener to continue traversal even when
// blocks are missing. This allows collecting ALL missing blocks in a single pass
// rather than bailing on the first error.
type ContinueOnErrorReader struct {
	inner         ipld.BlockReadOpener
	state         *RetrievalState
	returnDummies bool // if true, return dummy data to allow traversal to continue
}

// NewContinueOnErrorReader creates a reader that tracks missing blocks but
// allows traversal to continue
func NewContinueOnErrorReader(inner ipld.BlockReadOpener, state *RetrievalState, returnDummies bool) *ContinueOnErrorReader {
	return &ContinueOnErrorReader{
		inner:         inner,
		state:         state,
		returnDummies: returnDummies,
	}
}

// Read wraps the inner reader and tracks missing blocks
func (c *ContinueOnErrorReader) Read(lc ipld.LinkContext, l datamodel.Link) (io.Reader, error) {
	reader, err := c.inner(lc, l)
	if err != nil {
		// record this block as missing
		c.state.MarkMissing(l)

		// always return the error - we can't fake block content
		// the caller needs to handle continuing traversal with partial data
		return nil, err
	}
	// record this block as successfully received
	c.state.MarkReceived(l)
	return reader, nil
}

// PartialRetrievalError indicates that a retrieval completed with some blocks
// successfully retrieved but some blocks missing
type PartialRetrievalError struct {
	// the original error from the retrieval
	Err error
	// blocks that were successfully retrieved
	ReceivedBlocks []datamodel.Link
	// blocks that were requested but missing
	MissingBlocks []datamodel.Link
	// number of bytes successfully retrieved
	BytesRetrieved uint64
	// number of blocks successfully retrieved
	BlocksRetrieved uint64
}

func (e *PartialRetrievalError) Error() string {
	return fmt.Sprintf("partial retrieval: %d blocks (%d bytes) retrieved, %d blocks missing: %v",
		e.BlocksRetrieved, e.BytesRetrieved, len(e.MissingBlocks), e.Err)
}

func (e *PartialRetrievalError) Unwrap() error {
	return e.Err
}

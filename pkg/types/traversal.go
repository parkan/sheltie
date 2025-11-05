package types

import (
	"fmt"

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

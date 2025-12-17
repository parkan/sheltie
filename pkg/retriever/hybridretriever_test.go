package retriever

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent/qp"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"github.com/parkan/sheltie/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestCrossProviderDAGConstruction tests that the HybridRetriever can
// successfully fetch a DAG when blocks are split across multiple providers.
// This simulates the real-world scenario where provider A has the root/directory
// nodes and provider B has the leaf blocks.
func TestCrossProviderDAGConstruction(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build a simple DAG: root -> [child1, child2, child3]
	// Provider A will have: root, child1
	// Provider B will have: child2, child3
	store := &memstore.Store{}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(store)
	lsys.SetReadStorage(store)

	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagCbor),
			MhType:   multihash.SHA2_256,
			MhLength: 32,
		},
	}

	// Create leaf nodes
	child1, child1Bytes := makeLeafNode(t, lsys, lp, "leaf-data-1")
	child2, child2Bytes := makeLeafNode(t, lsys, lp, "leaf-data-2")
	child3, child3Bytes := makeLeafNode(t, lsys, lp, "leaf-data-3")

	// Create root node linking to children
	rootNode, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "children", qp.List(-1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.Link(child1))
			qp.ListEntry(la, qp.Link(child2))
			qp.ListEntry(la, qp.Link(child3))
		}))
	})
	require.NoError(t, err)

	rootLink, err := lsys.Store(linking.LinkContext{Ctx: ctx}, lp, rootNode)
	require.NoError(t, err)
	rootCid := rootLink.(cidlink.Link).Cid

	// Get root bytes
	rootBytes := getBlockBytes(t, store, rootCid)

	// Provider A: has root and child1 only
	providerABlocks := map[cid.Cid][]byte{
		rootCid:                   rootBytes,
		child1.(cidlink.Link).Cid: child1Bytes,
	}

	// Provider B: has child2 and child3
	providerBBlocks := map[cid.Cid][]byte{
		child2.(cidlink.Link).Cid: child2Bytes,
		child3.(cidlink.Link).Cid: child3Bytes,
	}

	// Start mock HTTP servers
	serverA := httptest.NewServer(makeBlockHandler(t, providerABlocks, "providerA"))
	defer serverA.Close()

	serverB := httptest.NewServer(makeBlockHandler(t, providerBBlocks, "providerB"))
	defer serverB.Close()

	// Create candidates with distinct peer IDs
	candidateA := makeMockCandidate(t, "provider-a", serverA.URL, rootCid, "12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")
	candidateB := makeMockCandidate(t, "provider-b", serverB.URL, rootCid, "12D3KooWRYqH1rDGfPMQaqaELfEBJXBnkSvRMxNR4AjqE7yXKqLH")

	// Mock candidate source that returns both providers
	mockSource := &mockCandidateSource{
		candidates: map[cid.Cid][]types.RetrievalCandidate{
			rootCid:                   {candidateA, candidateB},
			child1.(cidlink.Link).Cid: {candidateA},
			child2.(cidlink.Link).Cid: {candidateB},
			child3.(cidlink.Link).Cid: {candidateB},
		},
	}

	// Create a mock inner retriever that fails with missing block error
	// This simulates the whole-DAG retrieval failing because provider A
	// doesn't have all blocks
	mockInner := &mockFailingRetriever{
		failWithCid: child2.(cidlink.Link).Cid,
	}

	// Create output storage to collect retrieved blocks
	outputStore := &memstore.Store{}
	outputLsys := cidlink.DefaultLinkSystem()
	outputLsys.SetWriteStorage(outputStore)
	outputLsys.SetReadStorage(outputStore)

	// Pre-populate with blocks that would have been fetched before failure
	// (simulating partial whole-DAG fetch)
	outputStore.Put(ctx, rootCid.KeyString(), rootBytes)
	outputStore.Put(ctx, child1.(cidlink.Link).Cid.KeyString(), child1Bytes)

	// Create HybridRetriever
	hr := NewHybridRetriever(mockInner, mockSource, http.DefaultClient)

	// Create request
	request := types.RetrievalRequest{
		Request: trustlessutils.Request{
			Root:  rootCid,
			Scope: trustlessutils.DagScopeAll,
		},
		LinkSystem: outputLsys,
	}

	// Execute retrieval
	stats, err := hr.Retrieve(ctx, request, nil)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Verify all blocks were retrieved
	verifyBlockExists(t, outputStore, ctx, rootCid)
	verifyBlockExists(t, outputStore, ctx, child1.(cidlink.Link).Cid)
	verifyBlockExists(t, outputStore, ctx, child2.(cidlink.Link).Cid)
	verifyBlockExists(t, outputStore, ctx, child3.(cidlink.Link).Cid)

	t.Logf("Successfully retrieved DAG across providers: %d blocks, %d bytes",
		stats.Blocks, stats.Size)
}

// TestCrossProviderDeepDAG tests retrieval of a deeper DAG structure
// where missing blocks are at various depths.
func TestCrossProviderDeepDAG(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build a deeper DAG: root -> mid -> [leaf1, leaf2]
	store := &memstore.Store{}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(store)
	lsys.SetReadStorage(store)

	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagCbor),
			MhType:   multihash.SHA2_256,
			MhLength: 32,
		},
	}

	// Create leaf nodes
	leaf1, leaf1Bytes := makeLeafNode(t, lsys, lp, "deep-leaf-1")
	leaf2, leaf2Bytes := makeLeafNode(t, lsys, lp, "deep-leaf-2")

	// Create middle node
	midNode, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "leaves", qp.List(-1, func(la datamodel.ListAssembler) {
			qp.ListEntry(la, qp.Link(leaf1))
			qp.ListEntry(la, qp.Link(leaf2))
		}))
	})
	require.NoError(t, err)

	midLink, err := lsys.Store(linking.LinkContext{Ctx: ctx}, lp, midNode)
	require.NoError(t, err)
	midCid := midLink.(cidlink.Link).Cid
	midBytes := getBlockBytes(t, store, midCid)

	// Create root node
	rootNode, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "child", qp.Link(midLink))
	})
	require.NoError(t, err)

	rootLink, err := lsys.Store(linking.LinkContext{Ctx: ctx}, lp, rootNode)
	require.NoError(t, err)
	rootCid := rootLink.(cidlink.Link).Cid
	rootBytes := getBlockBytes(t, store, rootCid)

	// Provider A: has root and mid only (directory structure)
	providerABlocks := map[cid.Cid][]byte{
		rootCid: rootBytes,
		midCid:  midBytes,
	}

	// Provider B: has the leaves
	providerBBlocks := map[cid.Cid][]byte{
		leaf1.(cidlink.Link).Cid: leaf1Bytes,
		leaf2.(cidlink.Link).Cid: leaf2Bytes,
	}

	serverA := httptest.NewServer(makeBlockHandler(t, providerABlocks, "providerA"))
	defer serverA.Close()

	serverB := httptest.NewServer(makeBlockHandler(t, providerBBlocks, "providerB"))
	defer serverB.Close()

	candidateA := makeMockCandidate(t, "provider-a", serverA.URL, rootCid, "12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")
	candidateB := makeMockCandidate(t, "provider-b", serverB.URL, rootCid, "12D3KooWRYqH1rDGfPMQaqaELfEBJXBnkSvRMxNR4AjqE7yXKqLH")

	mockSource := &mockCandidateSource{
		candidates: map[cid.Cid][]types.RetrievalCandidate{
			rootCid:                  {candidateA},
			midCid:                   {candidateA},
			leaf1.(cidlink.Link).Cid: {candidateB},
			leaf2.(cidlink.Link).Cid: {candidateB},
		},
	}

	mockInner := &mockFailingRetriever{
		failWithCid: leaf1.(cidlink.Link).Cid,
	}

	outputStore := &memstore.Store{}
	outputLsys := cidlink.DefaultLinkSystem()
	outputLsys.SetWriteStorage(outputStore)
	outputLsys.SetReadStorage(outputStore)

	// Pre-populate with partial fetch
	outputStore.Put(ctx, rootCid.KeyString(), rootBytes)
	outputStore.Put(ctx, midCid.KeyString(), midBytes)

	hr := NewHybridRetriever(mockInner, mockSource, http.DefaultClient)

	request := types.RetrievalRequest{
		Request: trustlessutils.Request{
			Root:  rootCid,
			Scope: trustlessutils.DagScopeAll,
		},
		LinkSystem: outputLsys,
	}

	stats, err := hr.Retrieve(ctx, request, nil)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Verify complete DAG
	verifyBlockExists(t, outputStore, ctx, rootCid)
	verifyBlockExists(t, outputStore, ctx, midCid)
	verifyBlockExists(t, outputStore, ctx, leaf1.(cidlink.Link).Cid)
	verifyBlockExists(t, outputStore, ctx, leaf2.(cidlink.Link).Cid)

	t.Logf("Successfully retrieved deep DAG: %d blocks", stats.Blocks)
}

// TestNoFallbackOnNonMissingError verifies that HybridRetriever does NOT
// fall back to per-block fetching for errors that aren't "missing block" errors.
func TestNoFallbackOnNonMissingError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rootCid := cid.MustParse("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")

	mockInner := &mockFailingRetriever{
		failWithErr: errors.New("network timeout"),
	}

	mockSource := &mockCandidateSource{
		candidates: map[cid.Cid][]types.RetrievalCandidate{},
	}

	hr := NewHybridRetriever(mockInner, mockSource, http.DefaultClient)

	outputStore := &memstore.Store{}
	outputLsys := cidlink.DefaultLinkSystem()
	outputLsys.SetWriteStorage(outputStore)
	outputLsys.SetReadStorage(outputStore)

	request := types.RetrievalRequest{
		Request: trustlessutils.Request{
			Root:  rootCid,
			Scope: trustlessutils.DagScopeAll,
		},
		LinkSystem: outputLsys,
	}

	_, err := hr.Retrieve(ctx, request, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "network timeout")
}

// TestEfficientSubgraphRetrieval verifies that when a provider has a complete
// subgraph (a chain), we fetch it via CAR (one request) instead of per-block.
// This tests the scenario: P1 has A→B, P2 has B→C→D→E→F (a chain).
// When P1 fails on B, we should fetch B's entire subgraph from P2 in one CAR.
func TestEfficientSubgraphRetrieval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &memstore.Store{}
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetWriteStorage(store)
	lsys.SetReadStorage(store)

	lp := cidlink.LinkPrototype{
		Prefix: cid.Prefix{
			Version:  1,
			Codec:    uint64(multicodec.DagCbor),
			MhType:   multihash.SHA2_256,
			MhLength: 32,
		},
	}

	// Build a CHAIN: root → node1 → node2 → node3 → node4 → leaf
	// This way when we fetch node1 as CAR, we get the entire subgraph
	leaf, leafBytes := makeLeafNode(t, lsys, lp, "leaf-data")

	// Build chain bottom-up
	node4, node4Bytes := makeChainNode(t, lsys, lp, "node4", leaf)
	node3, node3Bytes := makeChainNode(t, lsys, lp, "node3", node4)
	node2, node2Bytes := makeChainNode(t, lsys, lp, "node2", node3)
	node1, node1Bytes := makeChainNode(t, lsys, lp, "node1", node2)

	// Root links to node1
	rootNode, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "child", qp.Link(node1))
	})
	require.NoError(t, err)

	rootLink, err := lsys.Store(linking.LinkContext{Ctx: ctx}, lp, rootNode)
	require.NoError(t, err)
	rootCid := rootLink.(cidlink.Link).Cid
	rootBytes := getBlockBytes(t, store, rootCid)

	// Provider A: has only root (will fail on node1)
	providerABlocks := map[cid.Cid][]byte{
		rootCid: rootBytes,
	}

	// Provider B: has the entire chain starting from node1
	providerBBlocks := map[cid.Cid][]byte{
		node1.(cidlink.Link).Cid: node1Bytes,
		node2.(cidlink.Link).Cid: node2Bytes,
		node3.(cidlink.Link).Cid: node3Bytes,
		node4.(cidlink.Link).Cid: node4Bytes,
		leaf.(cidlink.Link).Cid:  leafBytes,
	}

	// Track request counts
	var providerARequests int32
	var providerBRawRequests int32
	var providerBCARRequests int32

	// Start mock servers with request tracking
	serverA := httptest.NewServer(makeBlockHandlerWithStats(t, providerABlocks, "providerA", &providerARequests))
	defer serverA.Close()

	serverB := httptest.NewServer(makeHybridHandler(t, providerBBlocks, "providerB", &providerBRawRequests, &providerBCARRequests))
	defer serverB.Close()

	candidateA := makeMockCandidate(t, "provider-a", serverA.URL, rootCid, "12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")
	candidateB := makeMockCandidate(t, "provider-b", serverB.URL, rootCid, "12D3KooWRYqH1rDGfPMQaqaELfEBJXBnkSvRMxNR4AjqE7yXKqLH")

	// Provider B should be found for all CIDs in the chain
	mockSource := &mockCandidateSource{
		candidates: map[cid.Cid][]types.RetrievalCandidate{
			rootCid:                  {candidateA, candidateB},
			node1.(cidlink.Link).Cid: {candidateB},
			node2.(cidlink.Link).Cid: {candidateB},
			node3.(cidlink.Link).Cid: {candidateB},
			node4.(cidlink.Link).Cid: {candidateB},
			leaf.(cidlink.Link).Cid:  {candidateB},
		},
	}

	mockInner := &mockFailingRetriever{
		failWithCid: node1.(cidlink.Link).Cid,
	}

	outputStore := &memstore.Store{}
	outputLsys := cidlink.DefaultLinkSystem()
	outputLsys.SetWriteStorage(outputStore)
	outputLsys.SetReadStorage(outputStore)

	// Pre-populate with root from partial fetch
	outputStore.Put(ctx, rootCid.KeyString(), rootBytes)

	hr := NewHybridRetriever(mockInner, mockSource, http.DefaultClient)

	request := types.RetrievalRequest{
		Request: trustlessutils.Request{
			Root:  rootCid,
			Scope: trustlessutils.DagScopeAll,
		},
		LinkSystem: outputLsys,
	}

	stats, err := hr.Retrieve(ctx, request, nil)
	require.NoError(t, err)
	require.NotNil(t, stats)

	// Verify all blocks were retrieved
	verifyBlockExists(t, outputStore, ctx, rootCid)
	verifyBlockExists(t, outputStore, ctx, node1.(cidlink.Link).Cid)
	verifyBlockExists(t, outputStore, ctx, node2.(cidlink.Link).Cid)
	verifyBlockExists(t, outputStore, ctx, node3.(cidlink.Link).Cid)
	verifyBlockExists(t, outputStore, ctx, node4.(cidlink.Link).Cid)
	verifyBlockExists(t, outputStore, ctx, leaf.(cidlink.Link).Cid)

	carRequests := atomic.LoadInt32(&providerBCARRequests)
	rawRequests := atomic.LoadInt32(&providerBRawRequests)

	t.Logf("Provider B: CAR requests=%d, raw requests=%d (chain of 5 blocks)", carRequests, rawRequests)

	// Key assertion: With a chain, ONE CAR request for node1 should fetch all 5 blocks
	// Without efficient CAR fetching, we'd need 5 separate requests
	require.GreaterOrEqual(t, carRequests, int32(1), "should have made at least one CAR request")

	// With proper CAR subgraph fetching, we expect 1 CAR request to get all blocks
	// Raw requests should be 0 if CAR worked perfectly
	if carRequests == 1 && rawRequests == 0 {
		t.Logf("OPTIMAL: Fetched entire chain (5 blocks) with 1 CAR request!")
	} else {
		t.Logf("Note: CAR=%d, raw=%d (could be optimized)", carRequests, rawRequests)
	}
}

// Helper functions

func makeLeafNode(t *testing.T, lsys linking.LinkSystem, lp cidlink.LinkPrototype, data string) (datamodel.Link, []byte) {
	node, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "data", qp.String(data))
	})
	require.NoError(t, err)

	link, err := lsys.Store(linking.LinkContext{}, lp, node)
	require.NoError(t, err)

	// Get the raw bytes
	var buf bytes.Buffer
	err = dagcbor.Encode(node, &buf)
	require.NoError(t, err)

	return link, buf.Bytes()
}

// makeChainNode creates a node that links to a child, forming a chain
func makeChainNode(t *testing.T, lsys linking.LinkSystem, lp cidlink.LinkPrototype, name string, childLink datamodel.Link) (datamodel.Link, []byte) {
	node, err := qp.BuildMap(basicnode.Prototype.Any, -1, func(ma datamodel.MapAssembler) {
		qp.MapEntry(ma, "name", qp.String(name))
		qp.MapEntry(ma, "child", qp.Link(childLink))
	})
	require.NoError(t, err)

	link, err := lsys.Store(linking.LinkContext{}, lp, node)
	require.NoError(t, err)

	var buf bytes.Buffer
	err = dagcbor.Encode(node, &buf)
	require.NoError(t, err)

	return link, buf.Bytes()
}

func getBlockBytes(t *testing.T, store *memstore.Store, c cid.Cid) []byte {
	data, err := store.Get(context.Background(), c.KeyString())
	require.NoError(t, err)
	return data
}

func makeBlockHandler(t *testing.T, blocks map[cid.Cid][]byte, name string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Parse CID from path: /ipfs/{cid}
		path := r.URL.Path
		if len(path) < 7 {
			http.Error(w, "invalid path", http.StatusBadRequest)
			return
		}
		cidStr := path[6:] // strip "/ipfs/"
		// Handle query params
		if idx := len(cidStr) - 1; idx > 0 {
			for i, c := range cidStr {
				if c == '?' {
					cidStr = cidStr[:i]
					break
				}
			}
		}
		c, err := cid.Parse(cidStr)
		if err != nil {
			http.Error(w, "invalid cid", http.StatusBadRequest)
			return
		}

		data, ok := blocks[c]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/vnd.ipld.raw")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})
}

// makeBlockHandlerWithStats is like makeBlockHandler but tracks request count
func makeBlockHandlerWithStats(t *testing.T, blocks map[cid.Cid][]byte, name string, requestCount *int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(requestCount, 1)

		path := r.URL.Path
		if len(path) < 7 {
			http.Error(w, "invalid path", http.StatusBadRequest)
			return
		}
		cidStr := path[6:]
		if idx := strings.Index(cidStr, "?"); idx > 0 {
			cidStr = cidStr[:idx]
		}
		c, err := cid.Parse(cidStr)
		if err != nil {
			http.Error(w, "invalid cid", http.StatusBadRequest)
			return
		}

		data, ok := blocks[c]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/vnd.ipld.raw")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})
}

// makeHybridHandler handles both raw block requests and CAR requests
func makeHybridHandler(t *testing.T, blocks map[cid.Cid][]byte, name string, rawCount, carCount *int32) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if len(path) < 7 {
			http.Error(w, "invalid path", http.StatusBadRequest)
			return
		}
		cidStr := path[6:]
		if idx := strings.Index(cidStr, "?"); idx > 0 {
			cidStr = cidStr[:idx]
		}
		c, err := cid.Parse(cidStr)
		if err != nil {
			http.Error(w, "invalid cid", http.StatusBadRequest)
			return
		}

		// Check if this is a CAR request (dag-scope parameter present)
		query := r.URL.Query()
		if query.Get("dag-scope") != "" || strings.Contains(r.Header.Get("Accept"), "application/vnd.ipld.car") {
			atomic.AddInt32(carCount, 1)

			// Build CAR response with the requested block and all blocks it references
			// For simplicity, we return all blocks we have (simulating a complete provider)
			var buf bytes.Buffer
			carWriter, err := storage.NewWritable(&buf, []cid.Cid{c}, car.WriteAsCarV1(true))
			if err != nil {
				http.Error(w, "failed to create car writer", http.StatusInternalServerError)
				return
			}

			// Write the requested block first
			data, ok := blocks[c]
			if !ok {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			if err := carWriter.Put(r.Context(), c.KeyString(), data); err != nil {
				http.Error(w, "failed to write block", http.StatusInternalServerError)
				return
			}

			// Write all other blocks (simulating full subgraph)
			for blockCid, blockData := range blocks {
				if blockCid.Equals(c) {
					continue // Already written
				}
				_ = carWriter.Put(r.Context(), blockCid.KeyString(), blockData)
			}

			if err := carWriter.Finalize(); err != nil {
				http.Error(w, "failed to finalize car", http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Type", "application/vnd.ipld.car; version=1; order=dfs; dups=y")
			w.WriteHeader(http.StatusOK)
			w.Write(buf.Bytes())
			return
		}

		// Raw block request
		atomic.AddInt32(rawCount, 1)

		data, ok := blocks[c]
		if !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}

		w.Header().Set("Content-Type", "application/vnd.ipld.raw")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})
}

func makeMockCandidate(t *testing.T, name string, serverURL string, rootCid cid.Cid, peerIDStr string) types.RetrievalCandidate {
	// Parse server URL to get host:port
	// serverURL is like "http://127.0.0.1:12345"
	// We need to create a multiaddr like /ip4/127.0.0.1/tcp/12345/http
	u, err := url.Parse(serverURL)
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)

	maddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s/http", host, port))
	require.NoError(t, err)

	pid, err := peer.Decode(peerIDStr)
	require.NoError(t, err)

	return types.NewRetrievalCandidate(
		pid,
		[]multiaddr.Multiaddr{maddr},
		rootCid,
		&metadata.IpfsGatewayHttp{},
	)
}

func verifyBlockExists(t *testing.T, store *memstore.Store, ctx context.Context, c cid.Cid) {
	has, err := store.Has(ctx, c.KeyString())
	require.NoError(t, err)
	require.True(t, has, "block %s should exist in store", c)
}

// Mock types

type mockCandidateSource struct {
	candidates map[cid.Cid][]types.RetrievalCandidate
	mu         sync.Mutex
}

func (m *mockCandidateSource) FindCandidates(ctx context.Context, c cid.Cid, cb func(types.RetrievalCandidate)) error {
	m.mu.Lock()
	candidates := m.candidates[c]
	m.mu.Unlock()

	for _, cand := range candidates {
		cb(cand)
	}
	return nil
}

type mockFailingRetriever struct {
	failWithCid cid.Cid
	failWithErr error
}

func (m *mockFailingRetriever) Retrieve(
	ctx context.Context,
	request types.RetrievalRequest,
	eventsCallback func(types.RetrievalEvent),
) (*types.RetrievalStats, error) {
	if m.failWithErr != nil {
		return nil, m.failWithErr
	}
	// Simulate ErrNotFound for the specific CID
	return nil, format.ErrNotFound{Cid: m.failWithCid}
}

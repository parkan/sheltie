package blockbroker

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/ipfs/go-cid"
	"github.com/ipni/go-libipni/metadata"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/parkan/sheltie/pkg/types"
	"github.com/stretchr/testify/require"
)

// TestCIDMismatchRejection verifies that blocks with wrong content are rejected.
// This tests both bitrot and malicious content by flipping a single bit.
func TestCIDMismatchRejection(t *testing.T) {
	ctx := context.Background()

	// create valid block data
	correctData := []byte("this is valid block content that will be corrupted")
	mh, err := multihash.Sum(correctData, multihash.SHA2_256, -1)
	require.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, mh)

	// corrupt by flipping one bit (simulates bitrot or subtle malicious modification)
	corruptedData := make([]byte, len(correctData))
	copy(corruptedData, correctData)
	corruptedData[len(corruptedData)/2] ^= 0x01 // flip lowest bit in middle byte

	requestCount := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		w.Header().Set("Content-Type", "application/vnd.ipld.raw")
		w.WriteHeader(http.StatusOK)
		w.Write(corruptedData)
	}))
	defer server.Close()

	provider := makeTestCandidate(t, server.URL, c)
	session := NewSession(&mockRouting{providers: []types.RetrievalCandidate{provider}}, http.DefaultClient, false)
	defer session.Close()

	session.addProvider(provider)

	block, err := session.tryProviders(ctx, c, []types.RetrievalCandidate{provider})

	require.Nil(t, block, "corrupted block must not be returned")
	require.Error(t, err, "single bit flip must be detected")
	require.Contains(t, err.Error(), "cid mismatch")
	require.Equal(t, 1, requestCount)
}

// TestCorrectBlockAccepted verifies that blocks with correct content are accepted.
func TestCorrectBlockAccepted(t *testing.T) {
	ctx := context.Background()

	// create test data and its CID
	testData := []byte("hello world")
	mh, err := multihash.Sum(testData, multihash.SHA2_256, -1)
	require.NoError(t, err)
	c := cid.NewCidV1(cid.Raw, mh)

	// server returns correct data
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/vnd.ipld.raw")
		w.WriteHeader(http.StatusOK)
		w.Write(testData)
	}))
	defer server.Close()

	provider := makeTestCandidate(t, server.URL, c)
	session := NewSession(&mockRouting{providers: []types.RetrievalCandidate{provider}}, http.DefaultClient, false)
	defer session.Close()

	session.SeedProviders(ctx, c)

	block, err := session.Get(ctx, c)
	require.NoError(t, err)
	require.Equal(t, testData, block.RawData())
	require.Equal(t, c, block.Cid())
}

// mockRouting implements types.CandidateSource for testing
type mockRouting struct {
	providers []types.RetrievalCandidate
}

func (m *mockRouting) FindCandidates(ctx context.Context, c cid.Cid, cb func(types.RetrievalCandidate)) error {
	for _, p := range m.providers {
		cb(p)
	}
	return nil
}

func makeTestCandidate(t *testing.T, serverURL string, rootCid cid.Cid) types.RetrievalCandidate {
	// parse server URL to extract host:port (e.g., "http://127.0.0.1:12345")
	u, err := url.Parse(serverURL)
	require.NoError(t, err)
	host, port, err := net.SplitHostPort(u.Host)
	require.NoError(t, err)

	// build multiaddr: /ip4/127.0.0.1/tcp/{port}/http
	maddr, err := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/%s/tcp/%s/http", host, port))
	require.NoError(t, err)

	// use a deterministic peer ID for testing
	pid, err := peer.Decode("12D3KooWBSTEYMLSu5FnQjshEVah9LFGEZoQt26eacCEVYfedWA4")
	require.NoError(t, err)

	return types.NewRetrievalCandidate(
		pid,
		[]multiaddr.Multiaddr{maddr},
		rootCid,
		&metadata.IpfsGatewayHttp{},
	)
}

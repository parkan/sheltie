// MODIFIED: 2025-10-30
// - Removed bitswap protocol support and test cases
// - Updated peer indexing after bitswap removal
// - Fixed http peer test case to use correct constant

package itest

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	carv2 "github.com/ipld/go-car/v2"
	"github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	trustlessutils "github.com/ipld/go-trustless-utils"
	"github.com/ipni/go-libipni/metadata"
	host "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	lpmock "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/parkan/sheltie/pkg/internal/itest/mocknet"
	"github.com/parkan/sheltie/pkg/internal/lp2ptransports"
	"github.com/parkan/sheltie/pkg/retriever"
	"github.com/parkan/sheltie/pkg/sheltie"
	"github.com/parkan/sheltie/pkg/types"
	"github.com/stretchr/testify/require"
)

const (
	graphsyncDirect  = 0
	httpDirect       = 1
	transportsDirect = 2
)

func TestDirectFetch(t *testing.T) {
	testCases := []struct {
		name       string
		directPeer int
	}{
		{
			name:       "direct graphsync peer",
			directPeer: graphsyncDirect,
		},
		{
			name:       "direct http peer",
			directPeer: httpDirect,
		},
		{
			name:       "peer responding on transports protocol",
			directPeer: transportsDirect,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			req := require.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			rndSeed := time.Now().UTC().UnixNano()
			t.Logf("random seed: %d", rndSeed)
			var rndReader io.Reader = rand.New(rand.NewSource(rndSeed))

			mrn := mocknet.NewMockRetrievalNet(ctx, t)
			mrn.AddGraphsyncPeers(1)
			mrn.AddHttpPeers(1)

			// generate separate 4MiB random unixfs file DAGs on both peers

			// graphsync peer (0)
			graphsyncMAs, err := peer.AddrInfoToP2pAddrs(mrn.Remotes[0].AddrInfo())
			req.NoError(err)
			srcData1 := unixfs.GenerateFile(t, mrn.Remotes[0].LinkSystem, rndReader, 4<<20)
			mocknet.SetupRetrieval(t, mrn.Remotes[0])

			// http peer (1)
			srcData2 := unixfs.GenerateFile(t, mrn.Remotes[1].LinkSystem, bytes.NewReader(srcData1.Content), 4<<20)
			req.Equal(srcData2.Root, srcData1.Root)
			httpMAs, err := peer.AddrInfoToP2pAddrs(mrn.Remotes[1].AddrInfo())
			req.NoError(err)

			transportsAddr, clear := handleTransports(t, mrn.MN, []lp2ptransports.Protocol{
				{
					Name:      "libp2p",
					Addresses: graphsyncMAs,
				},
				{
					Name:      "http",
					Addresses: httpMAs,
				},
			})
			req.NoError(mrn.MN.LinkAll())
			defer clear()

			var addr peer.AddrInfo
			var protocols []metadata.Protocol
			switch testCase.directPeer {
			case graphsyncDirect:
				addr = *mrn.Remotes[0].AddrInfo()
				// Graphsync speaks libp2p, so we can use nil to auto-discover via libp2p
				protocols = nil
			case httpDirect:
				addr = *mrn.Remotes[1].AddrInfo()
				// HTTP peers don't speak libp2p, so we must explicitly specify the protocol
				protocols = []metadata.Protocol{metadata.IpfsGatewayHttp{}}
			case transportsDirect:
				addr = transportsAddr
				// Transports protocol will advertise available protocols
				protocols = nil
			default:
				req.FailNow("unrecognized direct peer test")
			}

			directFinder := retriever.NewDirectCandidateSource([]types.Provider{{Peer: addr, Protocols: protocols}}, retriever.WithLibp2pCandidateDiscovery(mrn.Self))
			lassie, err := sheltie.NewSheltie(ctx, sheltie.WithCandidateSource(directFinder), sheltie.WithHost(mrn.Self), sheltie.WithGlobalTimeout(5*time.Second))
			req.NoError(err)
			outFile, err := os.CreateTemp(t.TempDir(), "sheltie-test-")
			req.NoError(err)
			defer func() {
				req.NoError(outFile.Close())
			}()
			outCar, err := storage.NewReadableWritable(outFile, []cid.Cid{srcData1.Root}, carv2.WriteAsCarV1(true))
			req.NoError(err)
			request, err := types.NewRequestForPath(outCar, srcData1.Root, "", trustlessutils.DagScopeAll, nil)
			req.NoError(err)
			_, err = lassie.Fetch(ctx, request)
			req.NoError(err)
			err = outCar.Finalize()
			req.NoError(err)
			outFile.Seek(0, io.SeekStart)
			// Open the CAR bytes as read-only storage
			reader, err := storage.OpenReadable(outFile)
			req.NoError(err)

			// Load our UnixFS data and compare it to the original
			linkSys := cidlink.DefaultLinkSystem()
			linkSys.SetReadStorage(reader)
			linkSys.NodeReifier = unixfsnode.Reify
			linkSys.TrustedStorage = true
			gotDir := unixfs.ToDirEntry(t, linkSys, srcData1.Root, true)
			unixfs.CompareDirEntries(t, srcData1, gotDir)
		})
	}
}

type transportsListener struct {
	t         *testing.T
	host      host.Host
	protocols []lp2ptransports.Protocol
}

func handleTransports(t *testing.T, mn lpmock.Mocknet, protocols []lp2ptransports.Protocol) (peer.AddrInfo, func()) {
	h, err := mn.GenPeer()
	require.NoError(t, err)

	p := &transportsListener{t, h, protocols}
	h.SetStreamHandler(lp2ptransports.TransportsProtocolID, p.handleNewQueryStream)
	return peer.AddrInfo{
			ID:    h.ID(),
			Addrs: h.Addrs(),
		}, func() {
			h.RemoveStreamHandler(lp2ptransports.TransportsProtocolID)
		}
}

// Called when the client opens a libp2p stream
func (l *transportsListener) handleNewQueryStream(s network.Stream) {
	defer s.Close()
	response := lp2ptransports.QueryResponse{Protocols: l.protocols}
	// Write the response to the client
	err := lp2ptransports.BindnodeRegistry.TypeToWriter(&response, s, dagcbor.Encode)
	require.NoError(l.t, err)
}

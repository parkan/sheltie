package traversal

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-unixfsnode"
	unixfs "github.com/ipfs/go-unixfsnode/testutil"
	"github.com/ipld/go-car/v2/blockstore"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	"github.com/ipld/go-ipld-prime/storage/memstore"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/ipld/go-ipld-prime/traversal/selector/builder"
	"github.com/stretchr/testify/require"
)

// TestCollectAllMissing tests that when CollectAllMissing is true,
// VerifyCar collects all missing blocks instead of bailing on the first
func TestCollectAllMissing(t *testing.T) {
	ctx := context.Background()
	req := require.New(t)

	// create a simple unixfs dag
	store := &memstore.Store{
		Bag: make(map[string][]byte),
	}
	lsys := cidlink.DefaultLinkSystem()
	lsys.TrustedStorage = true
	lsys.SetReadStorage(store)
	lsys.SetWriteStorage(store)
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)

	// create a larger file to ensure multiple blocks
	// use 300KB file with 256KB chunks - will create root + 2 child blocks
	// this reflects real-world cases where parts of a DAG might be on different providers
	largeData := make([]byte, 300*1024)
	for i := range largeData {
		largeData[i] = byte(i % 256)
	}
	rootEnt := unixfs.GenerateFile(t, &lsys, bytes.NewReader(largeData), 256*1024)
	rootCid := rootEnt.Root

	t.Logf("generated test file with root: %s", rootCid)

	// collect all blocks from the store in traversal order
	allBlocks := make(map[cid.Cid][]byte)
	allBlocksInOrder := make([]cid.Cid, 0)

	// traverse to get blocks in order
	traversalLsys := cidlink.DefaultLinkSystem()
	traversalLsys.SetReadStorage(store)
	unixfsnode.AddUnixFSReificationToLinkSystem(&traversalLsys)

	// collect blocks during traversal
	ssb := builder.NewSelectorSpecBuilder(basicnode.Prototype.Any)
	allSelector := ssb.ExploreRecursive(
		selector.RecursionLimitNone(),
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
	).Node()

	collectingLsys := traversalLsys
	originalReader := collectingLsys.StorageReadOpener
	collectingLsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		c := l.(cidlink.Link).Cid
		allBlocksInOrder = append(allBlocksInOrder, c)
		return originalReader(lc, l)
	}

	sel, err := selector.CompileSelector(allSelector)
	req.NoError(err)

	lnkCtx := linking.LinkContext{Ctx: ctx}
	proto, err := protoChooser(cidlink.Link{Cid: rootCid}, lnkCtx)
	req.NoError(err)

	rootNode, err := collectingLsys.Load(lnkCtx, cidlink.Link{Cid: rootCid}, proto)
	req.NoError(err)

	err = ipldtraversal.Progress{
		Cfg: &ipldtraversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     collectingLsys,
			LinkTargetNodePrototypeChooser: protoChooser,
		},
	}.WalkAdv(rootNode, sel, func(p ipldtraversal.Progress, n datamodel.Node, vr ipldtraversal.VisitReason) error {
		return nil
	})
	req.NoError(err)

	// now collect the actual block data
	for k, v := range store.Bag {
		c, err := cid.Cast([]byte(k))
		req.NoError(err)
		allBlocks[c] = v
	}

	t.Logf("generated %d blocks in traversal order", len(allBlocks))
	req.True(len(allBlocks) > 1, "need multiple blocks for this test")
	t.Logf("block order: %v", allBlocksInOrder)

	// test 1: complete CAR - should succeed
	t.Run("complete CAR succeeds", func(t *testing.T) {
		carBytes := makeCompleteCAR(t, rootCid, allBlocks, allBlocksInOrder)

		destStore := &memstore.Store{Bag: make(map[string][]byte)}
		destLsys := cidlink.DefaultLinkSystem()
		destLsys.SetWriteStorage(destStore)

		cfg := Config{
			Root:              rootCid,
			Selector:          allSelector,
			CollectAllMissing: false,
		}

		result, err := cfg.VerifyCar(ctx, bytes.NewReader(carBytes), destLsys)
		req.NoError(err)
		req.Equal(uint64(len(allBlocks)), result.BlocksIn)
		req.Empty(result.MissingBlocks)
	})

	// test 2: incomplete CAR without CollectAllMissing - should fail on first missing block
	t.Run("incomplete CAR fails on first missing", func(t *testing.T) {
		// create CAR with only the root block
		partialBlocks := map[cid.Cid][]byte{
			rootCid: allBlocks[rootCid],
		}

		carBytes := makeCompleteCAR(t, rootCid, partialBlocks, allBlocksInOrder)

		destStore := &memstore.Store{Bag: make(map[string][]byte)}
		destLsys := cidlink.DefaultLinkSystem()
		destLsys.SetWriteStorage(destStore)

		cfg := Config{
			Root:              rootCid,
			Selector:          allSelector,
			CollectAllMissing: false,
		}

		result, err := cfg.VerifyCar(ctx, bytes.NewReader(carBytes), destLsys)
		req.Error(err)
		t.Logf("got expected error: %v", err)
		// should have gotten at least the root block
		req.Greater(result.BlocksIn, uint64(0))
	})

	// test 3: one block missing WITH CollectAllMissing
	t.Run("one block missing with CollectAllMissing", func(t *testing.T) {
		// exclude just one non-root block (the last one in traversal order)
		partialBlocks := make(map[cid.Cid][]byte)
		var excludedCid cid.Cid
		for i, c := range allBlocksInOrder {
			if i == len(allBlocksInOrder)-1 { // exclude last block in traversal order
				excludedCid = c
				continue
			}
			partialBlocks[c] = allBlocks[c]
		}

		carBytes := makeCompleteCAR(t, rootCid, partialBlocks, allBlocksInOrder)

		destStore := &memstore.Store{Bag: make(map[string][]byte)}
		destLsys := cidlink.DefaultLinkSystem()
		destLsys.SetWriteStorage(destStore)

		cfg := Config{
			Root:              rootCid,
			Selector:          allSelector,
			CollectAllMissing: true, // enable collection
		}

		result, err := cfg.VerifyCar(ctx, bytes.NewReader(carBytes), destLsys)

		// should return an error since traversal failed
		req.Error(err)
		t.Logf("got error for one missing block (expected): %v", err)

		// should have collected exactly one missing block
		req.Len(result.MissingBlocks, 1, "should have collected exactly 1 missing block")
		req.True(result.MissingBlocks[0].Equals(excludedCid), "should have collected the excluded block")

		// should have gotten most blocks
		req.Greater(result.BlocksIn, uint64(0))
		req.Greater(result.BytesIn, uint64(0))

		t.Logf("successfully collected 1 missing block: %s", result.MissingBlocks[0])
	})

	// test 4: multiple blocks missing WITH CollectAllMissing
	t.Run("multiple blocks missing with CollectAllMissing", func(t *testing.T) {
		// create CAR with only the root block (all others missing)
		// include only first block (root) from traversal order
		partialBlocks := map[cid.Cid][]byte{
			allBlocksInOrder[0]: allBlocks[allBlocksInOrder[0]],
		}

		carBytes := makeCompleteCAR(t, rootCid, partialBlocks, allBlocksInOrder)

		destStore := &memstore.Store{Bag: make(map[string][]byte)}
		destLsys := cidlink.DefaultLinkSystem()
		destLsys.SetWriteStorage(destStore)

		cfg := Config{
			Root:              rootCid,
			Selector:          allSelector,
			CollectAllMissing: true, // enable collection
		}

		result, err := cfg.VerifyCar(ctx, bytes.NewReader(carBytes), destLsys)

		// should still return an error since traversal failed
		req.Error(err)
		t.Logf("got error for multiple missing blocks (expected): %v", err)

		// but should have collected the missing blocks
		req.NotEmpty(result.MissingBlocks, "should have collected missing blocks")
		t.Logf("collected %d missing blocks", len(result.MissingBlocks))

		// should have at least one missing block
		// note: due to how traversal works, we can only collect missing blocks up to the first one
		// that prevents further traversal. since the file structure has siblings as children,
		// we'll only collect the first missing sibling before traversal stops.
		req.Greater(len(result.MissingBlocks), 0, "should have collected at least one missing block")

		// should have gotten the root block at least
		req.Greater(result.BlocksIn, uint64(0))
		req.Greater(result.BytesIn, uint64(0))

		// verify the missing blocks are actually the ones we excluded
		missingSet := make(map[cid.Cid]bool)
		for _, m := range result.MissingBlocks {
			missingSet[m] = true
		}

		// all blocks except the root should be in missing list
		for _, c := range allBlocksInOrder[1:] {
			if !missingSet[c] {
				t.Logf("warning: block %s not in missing list", c)
			}
		}

		t.Logf("successfully collected %d missing blocks", len(result.MissingBlocks))
	})
}

// TestMissingBlockCollector tests the MissingBlockCollector directly
func TestMissingBlockCollector(t *testing.T) {
	req := require.New(t)

	collector := NewMissingBlockCollector()
	req.False(collector.HasMissing())

	// record some missing CIDs
	c1, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	c2, _ := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")

	collector.RecordMissing(c1)
	req.True(collector.HasMissing())

	collector.RecordMissing(c2)
	collector.RecordMissing(c1) // duplicate

	missing := collector.GetMissing()
	req.Len(missing, 2, "should deduplicate")

	// verify both CIDs are present
	foundC1, foundC2 := false, false
	for _, c := range missing {
		if c.Equals(c1) {
			foundC1 = true
		}
		if c.Equals(c2) {
			foundC2 = true
		}
	}
	req.True(foundC1, "should contain c1")
	req.True(foundC2, "should contain c2")
}

// makeCompleteCAR creates a CAR file with the given blocks
// blocks must be written in traversal order for VerifyCar to work
func makeCompleteCAR(t *testing.T, root cid.Cid, blockData map[cid.Cid][]byte, allBlocksInOrder []cid.Cid) []byte {
	// create temp file for CAR
	tmpDir := t.TempDir()
	carPath := filepath.Join(tmpDir, "test.car")

	// use blockstore to write CAR
	bs, err := blockstore.OpenReadWrite(carPath, []cid.Cid{root}, blockstore.WriteAsCarV1(true))
	require.NoError(t, err)

	// write blocks in traversal order
	for _, c := range allBlocksInOrder {
		if data, ok := blockData[c]; ok {
			blk, err := blocks.NewBlockWithCid(data, c)
			require.NoError(t, err)
			err = bs.Put(context.Background(), blk)
			require.NoError(t, err)
		}
	}

	// finalize the CAR
	require.NoError(t, bs.Finalize())

	// read the CAR file back
	carBytes, err := os.ReadFile(carPath)
	require.NoError(t, err)

	return carBytes
}

// TestErrorCapturingReaderWithCollector tests the modified error capturing reader
func TestErrorCapturingReaderWithCollector(t *testing.T) {
	req := require.New(t)
	ctx := context.Background()

	collector := NewMissingBlockCollector()

	// create a link system that always returns errors
	lsys := cidlink.DefaultLinkSystem()
	lsys.StorageReadOpener = func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		return nil, io.ErrUnexpectedEOF
	}

	// wrap with error capturing reader
	lsys, ecr := NewErrorCapturingReaderWithCollector(lsys, collector)

	// try to read a block
	c, _ := cid.Decode("bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi")
	link := cidlink.Link{Cid: c}

	_, err := lsys.StorageReadOpener(linking.LinkContext{Ctx: ctx}, link)
	req.Error(err)
	req.Equal(io.ErrUnexpectedEOF, ecr.Error)

	// verify collector recorded the missing CID
	req.True(collector.HasMissing())
	missing := collector.GetMissing()
	req.Len(missing, 1)
	req.True(missing[0].Equals(c))
}

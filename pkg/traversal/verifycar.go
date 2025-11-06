// vendored and modified from github.com/ipld/go-trustless-utils@v0.4.1/traversal
// modifications: added support for collecting all missing blocks when CollectAllMissing is enabled

package traversal

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"

	// include all the codecs we care about
	dagpb "github.com/ipld/go-codec-dagpb"
	_ "github.com/ipld/go-ipld-prime/codec/cbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagcbor"
	_ "github.com/ipld/go-ipld-prime/codec/dagjson"
	_ "github.com/ipld/go-ipld-prime/codec/json"
	_ "github.com/ipld/go-ipld-prime/codec/raw"
	"github.com/multiformats/go-multihash"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	format "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-unixfsnode"
	"github.com/ipld/go-car/v2"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/linking/preload"
	"github.com/ipld/go-ipld-prime/node/basicnode"
	ipldtraversal "github.com/ipld/go-ipld-prime/traversal"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"go.uber.org/multierr"
)

var (
	ErrMalformedCar    = errors.New("malformed CAR")
	ErrBadVersion      = errors.New("bad CAR version")
	ErrBadRoots        = errors.New("CAR root CID mismatch")
	ErrUnexpectedBlock = errors.New("unexpected block in CAR")
	ErrExtraneousBlock = errors.New("extraneous block in CAR")
	ErrMissingBlock    = errors.New("missing block in CAR")
)

type BlockStream interface {
	Next(ctx context.Context) (blocks.Block, error)
}

var protoChooser = dagpb.AddSupportToChooser(basicnode.Chooser)

type Config struct {
	Root               cid.Cid        // the single root we expect to appear in the CAR and that we use to run our traversal against
	AllowCARv2         bool           // if true, allow CARv2 files to be received, otherwise strictly only allow CARv1
	Selector           datamodel.Node // the selector to execute, starting at the provided Root, to verify the contents of the CAR
	CheckRootsMismatch bool           // check if roots match expected behavior
	ExpectDuplicatesIn bool           // handles whether the incoming stream has duplicates
	WriteDuplicatesOut bool           // handles whether duplicates should be written a second time as blocks
	MaxBlocks          uint64         // set a budget for the traversal
	OnBlockIn          func(uint64)   // a callback whenever a block is read the incoming source, recording the number of bytes in the block data
	CollectAllMissing  bool           // if true, collect all missing blocks instead of bailing on first error
}

// TraversalResult provides the results of a successful traversal. Byte counting
// is performed on the raw block data, not any CAR container bytes.
type TraversalResult struct {
	// LastPath is the final path visited in a traversal, it can be used to
	// compare against the expected path to determine whether the traversal was
	// "complete"
	LastPath  datamodel.Path
	BlocksIn  uint64
	BytesIn   uint64
	BlocksOut uint64
	BytesOut  uint64
	// MissingBlocks contains CIDs that were requested but not found in the CAR
	// only populated when CollectAllMissing is true
	MissingBlocks []cid.Cid
}

// VerifyCar reads a CAR from the provided reader, verifies the contents are
// strictly what is specified by this Config and writes the blocks to the
// provided LinkSystem. It returns the number of blocks and bytes written to the
// LinkSystem. The LinkSystem may be used to load duplicate blocks in the case
// that duplicates are not expected from the CAR being verified but need to be
// written back out to the LinkSystem.
//
// Verification is performed according to the CAR construction rules contained
// within the Trustless, and Path Gateway specifications:
//
// * https://specs.ipfs.tech/http-gateways/trustless-gateway/
//
// * https://specs.ipfs.tech/http-gateways/path-gateway/
func (cfg Config) VerifyCar(
	ctx context.Context,
	rdr io.Reader,
	lsys linking.LinkSystem,
) (TraversalResult, error) {
	cbr, err := car.NewBlockReader(rdr, car.WithTrustedCAR(false))
	if err != nil {
		return TraversalResult{}, multierr.Combine(ErrMalformedCar, err)
	}

	switch cbr.Version {
	case 1:
	case 2:
		if !cfg.AllowCARv2 {
			return TraversalResult{}, ErrBadVersion
		}
	default:
		return TraversalResult{}, ErrBadVersion
	}

	if cfg.CheckRootsMismatch && (len(cbr.Roots) != 1 || cbr.Roots[0] != cfg.Root) {
		return TraversalResult{}, ErrBadRoots
	}
	return cfg.VerifyBlockStream(ctx, blockReaderStream{cbr}, lsys)
}

// VerifyBlockStream reads blocks from a BlockStream and verifies the stream of
// blocks are strictly what is specified by this Config and writes the blocks to
// the provided LinkSystem. It returns the number of blocks and bytes written to
// the LinkSystem. The LinkSystem may be used to load duplicate blocks in the
// case that duplicates are not expected from the BlockStream being verified but
// need to be written back out to the LinkSystem.
//
// Verification is performed according to the CAR construction rules contained
// within the Trustless, and Path Gateway specifications:
//
// * https://specs.ipfs.tech/http-gateways/trustless-gateway/
//
// * https://specs.ipfs.tech/http-gateways/path-gateway/
func (cfg Config) VerifyBlockStream(
	ctx context.Context,
	bs BlockStream,
	lsys linking.LinkSystem,
) (TraversalResult, error) {
	bt := &writeTracker{onBlockIn: cfg.OnBlockIn}
	lsys.TrustedStorage = true // we can rely on the CAR decoder to check CID integrity
	unixfsnode.AddUnixFSReificationToLinkSystem(&lsys)

	var missingCollector *MissingBlockCollector
	if cfg.CollectAllMissing {
		missingCollector = NewMissingBlockCollector()
		lsys.StorageReadOpener = cfg.nextBlockReadOpenerWithMissingCollector(ctx, bs, bt, lsys, missingCollector)
	} else {
		lsys.StorageReadOpener = cfg.nextBlockReadOpener(ctx, bs, bt, lsys)
	}

	// perform the traversal
	lastPath, err := cfg.Traverse(ctx, lsys, nil, missingCollector)

	result := TraversalResult{
		LastPath:  lastPath,
		BlocksIn:  bt.blocksIn,
		BytesIn:   bt.bytesIn,
		BlocksOut: bt.blocksOut,
		BytesOut:  bt.bytesOut,
	}

	if cfg.CollectAllMissing && missingCollector != nil {
		result.MissingBlocks = missingCollector.GetMissing()
		// if we collected missing blocks, return them along with the error
		if len(result.MissingBlocks) > 0 && err != nil {
			// consume any remaining blocks in the stream
			for {
				_, streamErr := bs.Next(ctx)
				if streamErr != nil {
					break
				}
			}
			return result, err
		}
	}

	if err != nil {
		return result, traversalError(err)
	}

	// make sure we don't have any extraneous data beyond what the traversal needs
	_, err = bs.Next(ctx)
	if err == nil {
		return result, ErrExtraneousBlock
	} else if !errors.Is(err, io.EOF) {
		return result, err
	}

	return result, nil
}

// Traverse performs a traversal using the Config's Selector, starting at the
// Config's Root, using the provided LinkSystem and optional Preloader.
//
// The traversal will capture any errors that occur during traversal, including
// block load errors that may not otherwise be captured by a standard
// go-ipld-prime traversal (such as those encountered by ADLs that are not
// propagated).
//
// Returns the last path visited during the traversal, or an error if the
// traversal failed.
func (cfg Config) Traverse(
	ctx context.Context,
	lsys linking.LinkSystem,
	preloader preload.Loader,
	missingCollector *MissingBlockCollector,
) (datamodel.Path, error) {
	sel, err := selector.CompileSelector(cfg.Selector)
	if err != nil {
		return datamodel.Path{}, err
	}

	var ecr *ErrorCapturingReader
	if missingCollector != nil {
		// use missing collector instead of error capturing reader
		lsys, ecr = NewErrorCapturingReaderWithCollector(lsys, missingCollector)
	} else {
		lsys, ecr = NewErrorCapturingReader(lsys)
	}

	// run traversal in this goroutine
	progress := ipldtraversal.Progress{
		Cfg: &ipldtraversal.Config{
			Ctx:                            ctx,
			LinkSystem:                     lsys,
			LinkTargetNodePrototypeChooser: protoChooser,
			Preloader:                      preloader,
		},
	}
	if cfg.MaxBlocks > 0 {
		progress.Budget = &ipldtraversal.Budget{
			LinkBudget: int64(cfg.MaxBlocks) - 1, // first block is already loaded
			NodeBudget: math.MaxInt64,
		}
	}

	rootNode, err := loadNode(ctx, cfg.Root, lsys)
	if err != nil {
		return datamodel.Path{}, fmt.Errorf("failed to load root node: %w", err)
	}

	progress.LastBlock.Link = cidlink.Link{Cid: cfg.Root}
	var lastPath datamodel.Path
	visitor := func(p ipldtraversal.Progress, n datamodel.Node, vr ipldtraversal.VisitReason) error {
		lastPath = p.Path
		if vr == ipldtraversal.VisitReason_SelectionMatch {
			return unixfsnode.BytesConsumingMatcher(p, n)
		}
		return nil
	}

	if err := progress.WalkAdv(rootNode, sel, visitor); err != nil {
		return datamodel.Path{}, err
	}

	if ecr.Error != nil && (missingCollector == nil || !cfg.CollectAllMissing) {
		return datamodel.Path{}, fmt.Errorf("block load failed during traversal: %w", ecr.Error)
	}

	return lastPath, nil
}

func loadNode(ctx context.Context, rootCid cid.Cid, lsys linking.LinkSystem) (datamodel.Node, error) {
	lnk := cidlink.Link{Cid: rootCid}
	lnkCtx := linking.LinkContext{Ctx: ctx}
	proto, err := protoChooser(lnk, lnkCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to choose prototype for CID %s: %w", rootCid.String(), err)
	}
	rootNode, err := lsys.Load(lnkCtx, lnk, proto)
	if err != nil {
		return nil, fmt.Errorf("failed to load root CID: %w", err)
	}
	return rootNode, nil
}

func asIdentity(c cid.Cid) (digest []byte, ok bool, err error) {
	dmh, err := multihash.Decode(c.Hash())
	if err != nil {
		return nil, false, err
	}
	ok = dmh.Code == multihash.IDENTITY
	digest = dmh.Digest
	return digest, ok, nil
}

// nextBlockReadOpener is a linking.BlockReadOpener that, for each call, will
// read the next block from the provided BlockStream, verify it matches the
// expected CID, and write it to the provided LinkSystem. It will then return
// a reader for the block data.
//
// It is also able to handle the case where duplicates are not expected from
// the incoming stream (the dups=n incoming case), so need to be loaded back
// from the LinkSystem (which we assume has them from the first write and is
// able to provide them back again), and also the case where duplicate write
// calls are required to the provided LinkSystem (the dups=y outgoing case).
func (cfg *Config) nextBlockReadOpener(
	ctx context.Context,
	bs BlockStream,
	bt *writeTracker,
	lsys linking.LinkSystem,
) linking.BlockReadOpener {
	seen := make(map[cid.Cid]struct{})
	return func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		cid := l.(cidlink.Link).Cid

		if digest, ok, err := asIdentity(cid); ok {
			return io.NopCloser(bytes.NewReader(digest)), nil
		} else if err != nil {
			return nil, err
		}

		var data []byte
		var err error
		if _, ok := seen[cid]; ok {
			if cfg.ExpectDuplicatesIn {
				// duplicate block, but in this case we are expecting the stream to have it
				data, err = readNextBlock(ctx, bs, cid)
				if err != nil {
					return nil, err
				}
				bt.recordBlockIn(data)
				if !cfg.WriteDuplicatesOut {
					return bytes.NewReader(data), nil
				}
			} else {
				// duplicate block, rely on the supplied LinkSystem to have stored this
				rdr, err := lsys.StorageReadOpener(lc, l)
				if !cfg.WriteDuplicatesOut {
					return rdr, err
				}
				data, err = io.ReadAll(rdr)
				if err != nil {
					return nil, err
				}
			}
		} else {
			seen[cid] = struct{}{}
			data, err = readNextBlock(ctx, bs, cid)
			if err != nil {
				return nil, err
			}
			bt.recordBlockIn(data)
		}
		bt.recordBlockOut(data)
		w, wc, err := lsys.StorageWriteOpener(lc)
		if err != nil {
			return nil, err
		}
		rdr := bytes.NewReader(data)
		if _, err := io.Copy(w, rdr); err != nil {
			return nil, err
		}
		if err := wc(l); err != nil {
			return nil, err
		}
		if _, err := rdr.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return io.NopCloser(rdr), nil
	}
}

// nextBlockReadOpenerWithMissingCollector is like nextBlockReadOpener but
// collects missing blocks instead of returning errors immediately
func (cfg *Config) nextBlockReadOpenerWithMissingCollector(
	ctx context.Context,
	bs BlockStream,
	bt *writeTracker,
	lsys linking.LinkSystem,
	collector *MissingBlockCollector,
) linking.BlockReadOpener {
	seen := make(map[cid.Cid]struct{})
	return func(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
		cid := l.(cidlink.Link).Cid

		if digest, ok, err := asIdentity(cid); ok {
			return io.NopCloser(bytes.NewReader(digest)), nil
		} else if err != nil {
			return nil, err
		}

		var data []byte
		var err error
		if _, ok := seen[cid]; ok {
			if cfg.ExpectDuplicatesIn {
				// duplicate block, but in this case we are expecting the stream to have it
				data, err = readNextBlock(ctx, bs, cid)
				if err != nil {
					collector.RecordMissing(cid)
					return nil, err
				}
				bt.recordBlockIn(data)
				if !cfg.WriteDuplicatesOut {
					return bytes.NewReader(data), nil
				}
			} else {
				// duplicate block, rely on the supplied LinkSystem to have stored this
				rdr, err := lsys.StorageReadOpener(lc, l)
				if !cfg.WriteDuplicatesOut {
					return rdr, err
				}
				data, err = io.ReadAll(rdr)
				if err != nil {
					collector.RecordMissing(cid)
					return nil, err
				}
			}
		} else {
			seen[cid] = struct{}{}
			data, err = readNextBlock(ctx, bs, cid)
			if err != nil {
				collector.RecordMissing(cid)
				return nil, err
			}
			bt.recordBlockIn(data)
		}
		bt.recordBlockOut(data)
		w, wc, err := lsys.StorageWriteOpener(lc)
		if err != nil {
			return nil, err
		}
		rdr := bytes.NewReader(data)
		if _, err := io.Copy(w, rdr); err != nil {
			return nil, err
		}
		if err := wc(l); err != nil {
			return nil, err
		}
		if _, err := rdr.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		return io.NopCloser(rdr), nil
	}
}

func readNextBlock(ctx context.Context, bs BlockStream, expected cid.Cid) ([]byte, error) {
	blk, err := bs.Next(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, format.ErrNotFound{Cid: expected}
		}
		return nil, multierr.Combine(ErrMalformedCar, err)
	}

	// compare by multihash only
	if !bytes.Equal(blk.Cid().Hash(), expected.Hash()) {
		return nil, fmt.Errorf("%w: %s != %s", ErrUnexpectedBlock, blk.Cid(), expected)
	}

	return blk.RawData(), nil
}

type writeTracker struct {
	onBlockIn func(uint64)

	blocksIn  uint64
	blocksOut uint64
	bytesIn   uint64
	bytesOut  uint64
}

func (bt *writeTracker) recordBlockIn(data []byte) {
	bt.blocksIn++
	bc := uint64(len(data))
	bt.bytesIn += bc
	if bt.onBlockIn != nil {
		bt.onBlockIn(bc)
	}
}

func (bt *writeTracker) recordBlockOut(data []byte) {
	bt.blocksOut++
	bt.bytesOut += uint64(len(data))
}

func traversalError(original error) error {
	err := original
	for {
		if v, ok := err.(interface{ NotFound() bool }); ok && v.NotFound() {
			return multierr.Combine(ErrMissingBlock, err)
		}
		if err = errors.Unwrap(err); err == nil {
			return original
		}
	}
}

type blockReaderStream struct {
	cbr *car.BlockReader
}

func (brs blockReaderStream) Next(ctx context.Context) (blocks.Block, error) {
	return brs.cbr.Next()
}

package extractor

import (
	"context"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
)

// threshold above which blocks are streamed directly instead of buffered
const largeBlockThreshold = 1 << 20 // 1 MiB

// StreamingCarReader reads a CAR stream and extracts content.
// Small blocks are buffered for decoding; large blocks stream directly to disk.
type StreamingCarReader struct {
	extractor *Extractor
	expected  map[cid.Cid]struct{}
	onBlock   func(int)
}

// NewStreamingCarReader creates a streaming CAR reader.
func NewStreamingCarReader(ext *Extractor, rootCid cid.Cid) *StreamingCarReader {
	return &StreamingCarReader{
		extractor: ext,
		expected:  map[cid.Cid]struct{}{rootCid: {}},
	}
}

// OnBlock registers a callback for progress tracking.
func (r *StreamingCarReader) OnBlock(cb func(int)) {
	r.onBlock = cb
}

// ReadAndExtract reads the CAR stream and extracts content.
func (r *StreamingCarReader) ReadAndExtract(ctx context.Context, rdr io.Reader) (uint64, uint64, error) {
	br, err := car.NewBlockReader(rdr)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to create block reader: %w", err)
	}

	var blockCount uint64
	var byteCount uint64

	for {
		if ctx.Err() != nil {
			return blockCount, byteCount, ctx.Err()
		}

		c, blockRdr, size, err := br.NextReader()
		if err == io.EOF {
			break
		}
		if err != nil {
			return blockCount, byteCount, fmt.Errorf("failed to read block: %w", err)
		}

		// check if expected or already processed
		if _, ok := r.expected[c]; !ok {
			if r.extractor.IsProcessed(c) {
				// duplicate, drain and skip
				io.Copy(io.Discard, blockRdr)
				continue
			}
			logger.Debugw("unexpected block in CAR", "cid", c, "size", size)
		}
		delete(r.expected, c)

		// decide how to handle based on size and knowledge
		if size > largeBlockThreshold {
			if r.extractor.IsKnownLeaf(c) {
				// stream directly to file with verification
				n, err := r.extractor.StreamLeaf(ctx, c, blockRdr, int64(size))
				if err != nil {
					return blockCount, byteCount, fmt.Errorf("streaming leaf %s: %w", c, err)
				}
				byteCount += uint64(n)
			} else {
				// large block but parent unknown - spool to temp
				n, err := r.extractor.SpoolOrphan(ctx, c, blockRdr)
				if err != nil {
					return blockCount, byteCount, fmt.Errorf("spooling orphan %s: %w", c, err)
				}
				byteCount += uint64(n)
			}
		} else {
			// buffer small block for decoding
			data, err := io.ReadAll(blockRdr)
			if err != nil {
				return blockCount, byteCount, fmt.Errorf("reading block %s: %w", c, err)
			}

			// verify CID
			computed, err := c.Prefix().Sum(data)
			if err != nil {
				return blockCount, byteCount, fmt.Errorf("hashing block: %w", err)
			}
			if !computed.Equals(c) {
				return blockCount, byteCount, fmt.Errorf("CID mismatch: expected %s, got %s", c, computed)
			}

			blk, err := blocks.NewBlockWithCid(data, c)
			if err != nil {
				return blockCount, byteCount, fmt.Errorf("creating block: %w", err)
			}

			children, err := r.extractor.ProcessBlock(ctx, blk)
			if err != nil {
				logger.Warnw("extractor failed", "cid", c, "err", err)
			}

			// add children to expected set
			for _, child := range children {
				if !r.extractor.IsProcessed(child) && !isIdentityCid(child) {
					r.expected[child] = struct{}{}
				}
			}

			byteCount += uint64(len(data))
		}

		blockCount++
		if r.onBlock != nil {
			r.onBlock(int(size))
		}
	}

	if len(r.expected) > 0 {
		missing := make([]cid.Cid, 0, len(r.expected))
		for c := range r.expected {
			missing = append(missing, c)
		}
		return blockCount, byteCount, &IncompleteError{Missing: missing}
	}

	return blockCount, byteCount, nil
}

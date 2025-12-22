package extractor

import (
	"context"
	"fmt"
	"io"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2"
)

// ExtractingCarReader reads a CAR stream and extracts UnixFS content directly
// to disk without storing blocks in memory. It uses a frontier-based approach
// to track expected blocks and verify the DAG is complete.
type ExtractingCarReader struct {
	extractor *Extractor
	expected  map[cid.Cid]struct{} // blocks we expect to receive
	onBlock   func(int)            // callback for each block (bytes)
}

// NewExtractingCarReader creates a reader that extracts from a CAR stream.
func NewExtractingCarReader(ext *Extractor, rootCid cid.Cid) *ExtractingCarReader {
	return &ExtractingCarReader{
		extractor: ext,
		expected:  map[cid.Cid]struct{}{rootCid: {}},
	}
}

// OnBlock registers a callback invoked for each block processed.
func (r *ExtractingCarReader) OnBlock(cb func(int)) {
	r.onBlock = cb
}

// ReadAndExtract reads the CAR stream and extracts content to disk.
// Returns the number of blocks and bytes processed.
func (r *ExtractingCarReader) ReadAndExtract(ctx context.Context, rdr io.Reader) (uint64, uint64, error) {
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

		block, err := br.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return blockCount, byteCount, fmt.Errorf("failed to read block: %w", err)
		}

		c := block.Cid()
		data := block.RawData()

		// verify the block is expected (part of the DAG)
		if _, ok := r.expected[c]; !ok {
			// unexpected block - could be duplicate or out-of-scope
			// in streaming CAR with dups=y, duplicates are expected
			// check if already processed
			if r.extractor.processed[c] {
				// duplicate, skip
				continue
			}
			// might be a block we haven't seen the parent for yet
			// in streaming mode, blocks can arrive before their parent
			logger.Debugw("unexpected block in CAR", "cid", c)
		}
		delete(r.expected, c)

		// verify CID matches content
		computed, err := c.Prefix().Sum(data)
		if err != nil {
			return blockCount, byteCount, fmt.Errorf("failed to compute CID for block: %w", err)
		}
		if !computed.Equals(c) {
			return blockCount, byteCount, fmt.Errorf("CID mismatch: expected %s, got %s", c, computed)
		}

		// process block through extractor
		blk, err := blocks.NewBlockWithCid(data, c)
		if err != nil {
			return blockCount, byteCount, fmt.Errorf("failed to create block: %w", err)
		}

		children, err := r.extractor.ProcessBlock(ctx, blk)
		if err != nil {
			logger.Warnw("extractor failed", "cid", c, "err", err)
			// continue processing other blocks
		}

		// add children to expected set (skip identity CIDs - already handled inline)
		for _, child := range children {
			if !r.extractor.processed[child] && !isIdentityCid(child) {
				r.expected[child] = struct{}{}
			}
		}

		blockCount++
		byteCount += uint64(len(data))

		if r.onBlock != nil {
			r.onBlock(len(data))
		}
	}

	// check for missing blocks
	if len(r.expected) > 0 {
		missing := make([]cid.Cid, 0, len(r.expected))
		for c := range r.expected {
			missing = append(missing, c)
		}
		return blockCount, byteCount, &IncompleteError{Missing: missing}
	}

	return blockCount, byteCount, nil
}

// IncompleteError indicates the CAR stream ended before all expected blocks
// were received. The Missing field contains CIDs that were not found.
type IncompleteError struct {
	Missing []cid.Cid
}

func (e *IncompleteError) Error() string {
	return fmt.Sprintf("incomplete CAR: missing %d blocks", len(e.Missing))
}

// IsMissing returns the missing CIDs if error is IncompleteError.
func IsMissing(err error) ([]cid.Cid, bool) {
	if ie, ok := err.(*IncompleteError); ok {
		return ie.Missing, true
	}
	return nil, false
}

package extractor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/storage"
)

// notFoundError implements the interface expected by IPLD storage
type notFoundError struct{}

func (notFoundError) Error() string   { return "not found" }
func (notFoundError) NotFound() bool  { return true }

var errNotFound = notFoundError{}

// ExtractingStore wraps an Extractor to implement storage interfaces.
// It processes blocks through the extractor as they arrive and also
// keeps them in memory for the traversal to read back.
type ExtractingStore struct {
	extractor *Extractor
	blocks    map[cid.Cid][]byte
	mu        sync.RWMutex

	onPut     func(int)
	onPutSync bool
}

// NewExtractingStore creates a storage that extracts UnixFS content as blocks arrive.
func NewExtractingStore(ext *Extractor) *ExtractingStore {
	return &ExtractingStore{
		extractor: ext,
		blocks:    make(map[cid.Cid][]byte),
	}
}

// OnPut registers a callback for when blocks are stored.
func (s *ExtractingStore) OnPut(cb func(int), sync bool) {
	s.onPut = cb
	s.onPutSync = sync
}

// Put stores a block and processes it through the extractor.
func (s *ExtractingStore) Put(ctx context.Context, key string, data []byte) error {
	c, err := cidFromKey(key)
	if err != nil {
		return err
	}

	// store for read-back during traversal
	s.mu.Lock()
	s.blocks[c] = data
	s.mu.Unlock()

	// process through extractor
	block, err := blocks.NewBlockWithCid(data, c)
	if err != nil {
		return fmt.Errorf("failed to create block: %w", err)
	}

	_, err = s.extractor.ProcessBlock(ctx, block)
	if err != nil {
		logger.Warnw("extractor processing failed", "cid", c, "err", err)
		// don't fail the whole retrieval, just log
	}

	if s.onPut != nil {
		if s.onPutSync {
			s.onPut(len(data))
		} else {
			go s.onPut(len(data))
		}
	}

	return nil
}

// Get retrieves a block from the in-memory store.
func (s *ExtractingStore) Get(ctx context.Context, key string) ([]byte, error) {
	c, err := cidFromKey(key)
	if err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	data, ok := s.blocks[c]
	if !ok {
		return nil, errNotFound
	}
	return data, nil
}

// Has checks if a block exists in the store.
func (s *ExtractingStore) Has(ctx context.Context, key string) (bool, error) {
	c, err := cidFromKey(key)
	if err != nil {
		return false, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.blocks[c]
	return ok, nil
}

// GetStream returns a reader for block data.
func (s *ExtractingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	data, err := s.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Close closes the extractor.
func (s *ExtractingStore) Close() error {
	return s.extractor.Close()
}

// LinkSystem returns a LinkSystem configured for this store.
func (s *ExtractingStore) LinkSystem() linking.LinkSystem {
	lsys := cidlink.DefaultLinkSystem()
	lsys.SetReadStorage(s)
	lsys.SetWriteStorage(s)
	return lsys
}

func cidFromKey(key string) (cid.Cid, error) {
	// keys are typically the CID string or binary
	c, err := cid.Parse(key)
	if err != nil {
		// try as binary
		_, c, err = cid.CidFromBytes([]byte(key))
		if err != nil {
			return cid.Undef, fmt.Errorf("invalid key: %w", err)
		}
	}
	return c, nil
}

// interface assertions
var (
	_ storage.ReadableStorage  = (*ExtractingStore)(nil)
	_ storage.WritableStorage  = (*ExtractingStore)(nil)
	_ storage.StreamingReadableStorage = (*ExtractingStore)(nil)
)

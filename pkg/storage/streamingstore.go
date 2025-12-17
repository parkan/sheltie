package storage

import (
	"bytes"
	"context"
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/parkan/sheltie/pkg/types"
)

var _ types.ReadableWritableStorage = (*StreamingStore)(nil)

// StreamingStore is a storage that streams blocks directly to output without
// retaining block data. It maintains only a set of seen CIDs for deduplication
// of requests (not output).
//
// Get() always returns NotFound since block data is not retained.
// Has() checks the seen set to determine if a block was already written.
// Put() writes directly to output and records the CID as seen.
type StreamingStore struct {
	seen      map[cid.Cid]struct{}
	outWriter linking.BlockWriteOpener
	mu        sync.RWMutex
	closed    bool
}

// NewStreamingStore creates a new streaming storage that writes blocks directly
// to the provided BlockWriteOpener without retaining block data.
func NewStreamingStore(outWriter linking.BlockWriteOpener) *StreamingStore {
	return &StreamingStore{
		seen:      make(map[cid.Cid]struct{}),
		outWriter: outWriter,
	}
}

// Has returns true if the CID has been written to this store.
// This is used for request deduplication, not output deduplication.
func (s *StreamingStore) Has(ctx context.Context, key string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return false, errClosed
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return false, err
	}

	_, ok := s.seen[c]
	return ok, nil
}

// Get always returns NotFound. StreamingStore does not retain block data -
// it streams directly to output. Callers needing block data must fetch from
// the network.
func (s *StreamingStore) Get(ctx context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errClosed
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, err
	}

	return nil, carstorage.ErrNotFound{Cid: c}
}

// GetStream always returns NotFound. StreamingStore does not retain block data.
func (s *StreamingStore) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errClosed
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, err
	}

	return nil, carstorage.ErrNotFound{Cid: c}
}

// Put writes the block directly to the output stream and records the CID as seen.
// If the block has already been written (based on CID), this is a no-op.
func (s *StreamingStore) Put(ctx context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return errClosed
	}

	c, err := cid.Cast([]byte(key))
	if err != nil {
		return err
	}

	// Skip if already written
	if _, ok := s.seen[c]; ok {
		return nil
	}

	// Write to output
	if err := s.writeToOutput(ctx, c, data); err != nil {
		return err
	}

	// Mark as seen
	s.seen[c] = struct{}{}
	return nil
}

// Close marks the store as closed. Further operations will return errClosed.
func (s *StreamingStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// Seen returns the number of unique blocks that have been written.
func (s *StreamingStore) Seen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.seen)
}

func (s *StreamingStore) writeToOutput(ctx context.Context, c cid.Cid, data []byte) error {
	w, commit, err := s.outWriter(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}

	n, err := bytes.NewBuffer(data).WriteTo(w)
	if err != nil {
		return err
	}
	if n != int64(len(data)) {
		return io.ErrShortWrite
	}

	return commit(cidlink.Link{Cid: c})
}

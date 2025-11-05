package storage

import (
	"bytes"
	"context"
	"io"

	"github.com/ipfs/go-cid"
	carstorage "github.com/ipld/go-car/v2/storage"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	ipldstorage "github.com/ipld/go-ipld-prime/storage"
)

var _ ipldstorage.WritableStorage = (*WriteOnlyCarSink)(nil)
var _ ipldstorage.ReadableStorage = (*WriteOnlyCarSink)(nil)

// WriteOnlyCarSink is a minimal storage that only writes blocks to a CAR writer.
// For use with --dups flag where we don't need to read blocks back.
type WriteOnlyCarSink struct {
	writer linking.BlockWriteOpener
}

// NewWriteOnlyCarSink creates a storage that only writes, never reads
func NewWriteOnlyCarSink(writer linking.BlockWriteOpener) *WriteOnlyCarSink {
	return &WriteOnlyCarSink{writer: writer}
}

// Put writes a block directly to the CAR output
func (w *WriteOnlyCarSink) Put(ctx context.Context, key string, data []byte) error {
	// handle identity CIDs
	if _, ok, err := AsIdentity(key); ok {
		return nil // don't write identity CIDs
	} else if err != nil {
		return err
	}

	// write directly to the CAR writer
	return writeToSink(ctx, w.writer, key, data)
}

// Has always returns false to allow duplicates
func (w *WriteOnlyCarSink) Has(ctx context.Context, key string) (bool, error) {
	// always return false - we allow duplicates
	return false, nil
}

// Get always returns not found - we don't store blocks
func (w *WriteOnlyCarSink) Get(ctx context.Context, key string) ([]byte, error) {
	// handle identity CIDs
	if digest, ok, err := AsIdentity(key); ok {
		return digest, nil
	} else if err != nil {
		return nil, err
	}

	// we don't store blocks, always return not found
	keyCid, err := cid.Cast([]byte(key))
	if err != nil {
		return nil, err
	}
	return nil, carstorage.ErrNotFound{Cid: keyCid}
}

// GetStream always returns not found
func (w *WriteOnlyCarSink) GetStream(ctx context.Context, key string) (io.ReadCloser, error) {
	data, err := w.Get(ctx, key)
	if err != nil {
		return nil, err
	}
	// this will only happen for identity CIDs
	return io.NopCloser(bytes.NewReader(data)), nil
}

// writeToSink is a helper to write a block to the CAR writer
func writeToSink(ctx context.Context, writer linking.BlockWriteOpener, key string, data []byte) error {
	cid, err := cid.Cast([]byte(key))
	if err != nil {
		return err
	}
	w, committer, err := writer(linking.LinkContext{Ctx: ctx})
	if err != nil {
		return err
	}
	if _, err := w.Write(data); err != nil {
		return err
	}
	return committer(cidlink.Link{Cid: cid})
}
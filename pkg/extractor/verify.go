package extractor

import (
	"fmt"
	"hash"
	"io"

	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// VerifyingReader wraps an io.Reader and verifies the CID matches content.
// Verification happens when the reader is exhausted (EOF).
type VerifyingReader struct {
	r        io.Reader
	expected cid.Cid
	hasher   hash.Hash
	verified bool
	err      error
}

// NewVerifyingReader creates a reader that verifies content matches the CID.
func NewVerifyingReader(r io.Reader, expected cid.Cid) (*VerifyingReader, error) {
	prefix := expected.Prefix()
	hasher, err := multihash.GetHasher(prefix.MhType)
	if err != nil {
		return nil, fmt.Errorf("unsupported hash type %d: %w", prefix.MhType, err)
	}
	return &VerifyingReader{
		r:        r,
		expected: expected,
		hasher:   hasher,
	}, nil
}

func (v *VerifyingReader) Read(p []byte) (int, error) {
	if v.err != nil {
		return 0, v.err
	}

	n, err := v.r.Read(p)
	if n > 0 {
		v.hasher.Write(p[:n])
	}

	if err == io.EOF {
		// verify on EOF
		v.err = v.verify()
		if v.err != nil {
			return n, v.err
		}
		v.verified = true
		return n, io.EOF
	}

	if err != nil {
		v.err = err
	}
	return n, err
}

func (v *VerifyingReader) verify() error {
	prefix := v.expected.Prefix()

	// build multihash from digest
	digest := v.hasher.Sum(nil)
	mh, err := multihash.Encode(digest, prefix.MhType)
	if err != nil {
		return fmt.Errorf("failed to encode multihash: %w", err)
	}

	// build CID from multihash
	computed := cid.NewCidV1(prefix.Codec, mh)
	if !computed.Equals(v.expected) {
		return fmt.Errorf("CID mismatch: expected %s, got %s", v.expected, computed)
	}
	return nil
}

// Verified returns true if the content was fully read and verified.
func (v *VerifyingReader) Verified() bool {
	return v.verified
}

// TeeVerifyingWriter wraps a writer and verifies the CID of written content.
type TeeVerifyingWriter struct {
	w        io.Writer
	expected cid.Cid
	hasher   hash.Hash
	written  int64
}

// NewTeeVerifyingWriter creates a writer that hashes content as it's written.
func NewTeeVerifyingWriter(w io.Writer, expected cid.Cid) (*TeeVerifyingWriter, error) {
	prefix := expected.Prefix()
	hasher, err := multihash.GetHasher(prefix.MhType)
	if err != nil {
		return nil, fmt.Errorf("unsupported hash type %d: %w", prefix.MhType, err)
	}
	return &TeeVerifyingWriter{
		w:        w,
		expected: expected,
		hasher:   hasher,
	}, nil
}

func (t *TeeVerifyingWriter) Write(p []byte) (int, error) {
	n, err := t.w.Write(p)
	if n > 0 {
		t.hasher.Write(p[:n])
		t.written += int64(n)
	}
	return n, err
}

// Verify checks that the written content matches the expected CID.
func (t *TeeVerifyingWriter) Verify() error {
	prefix := t.expected.Prefix()
	digest := t.hasher.Sum(nil)
	mh, err := multihash.Encode(digest, prefix.MhType)
	if err != nil {
		return fmt.Errorf("failed to encode multihash: %w", err)
	}
	computed := cid.NewCidV1(prefix.Codec, mh)
	if !computed.Equals(t.expected) {
		return fmt.Errorf("CID mismatch: expected %s, got %s", t.expected, computed)
	}
	return nil
}

// Written returns the number of bytes written.
func (t *TeeVerifyingWriter) Written() int64 {
	return t.written
}

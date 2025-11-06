// vendored and modified from github.com/ipld/go-trustless-utils@v0.4.1/traversal
// modifications: added MissingBlockCollector support

package traversal

import (
	"io"
	"sync"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/linking"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

// ErrorCapturingReader captures any errors that occur during block loading
// and makes them available via the Error property.
//
// This is useful for capturing errors that occur during traversal, which are
// not currently surfaced by the traversal package, see:
//
//	https://github.com/ipld/go-ipld-prime/pull/524
type ErrorCapturingReader struct {
	sro       linking.BlockReadOpener
	Error     error
	collector *MissingBlockCollector
}

func NewErrorCapturingReader(lsys linking.LinkSystem) (linking.LinkSystem, *ErrorCapturingReader) {
	ecr := &ErrorCapturingReader{sro: lsys.StorageReadOpener}
	lsys.StorageReadOpener = ecr.StorageReadOpener
	return lsys, ecr
}

func NewErrorCapturingReaderWithCollector(lsys linking.LinkSystem, collector *MissingBlockCollector) (linking.LinkSystem, *ErrorCapturingReader) {
	ecr := &ErrorCapturingReader{
		sro:       lsys.StorageReadOpener,
		collector: collector,
	}
	lsys.StorageReadOpener = ecr.StorageReadOpener
	return lsys, ecr
}

func (ecr *ErrorCapturingReader) StorageReadOpener(lc linking.LinkContext, l datamodel.Link) (io.Reader, error) {
	r, err := ecr.sro(lc, l)
	if err != nil {
		if ecr.Error == nil {
			ecr.Error = err
		}
		// if we have a collector, record the missing CID
		if ecr.collector != nil {
			if cidLink, ok := l.(cidlink.Link); ok {
				ecr.collector.RecordMissing(cidLink.Cid)
			}
		}
	}
	return r, err
}

// MissingBlockCollector tracks all missing blocks encountered during traversal
type MissingBlockCollector struct {
	mu      sync.Mutex
	missing map[cid.Cid]struct{}
}

func NewMissingBlockCollector() *MissingBlockCollector {
	return &MissingBlockCollector{
		missing: make(map[cid.Cid]struct{}),
	}
}

func (m *MissingBlockCollector) RecordMissing(c cid.Cid) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.missing[c] = struct{}{}
}

func (m *MissingBlockCollector) GetMissing() []cid.Cid {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]cid.Cid, 0, len(m.missing))
	for c := range m.missing {
		result = append(result, c)
	}
	return result
}

func (m *MissingBlockCollector) HasMissing() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.missing) > 0
}

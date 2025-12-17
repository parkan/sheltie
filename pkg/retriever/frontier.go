package retriever

import (
	"bytes"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	dagpb "github.com/ipld/go-codec-dagpb"
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/codec/dagcbor"
	"github.com/ipld/go-ipld-prime/codec/dagjson"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/node/basicnode"
)

// Frontier tracks CIDs to fetch and CIDs already seen during a streaming traversal.
// It implements a stack-based (DFS) approach to minimize memory usage.
type Frontier struct {
	pending []cid.Cid            // Stack of CIDs to fetch (DFS order)
	seen    map[cid.Cid]struct{} // CIDs we've already processed
}

// NewFrontier creates a new frontier starting with the given root CID.
func NewFrontier(root cid.Cid) *Frontier {
	return &Frontier{
		pending: []cid.Cid{root},
		seen:    make(map[cid.Cid]struct{}),
	}
}

// Empty returns true if there are no more CIDs to fetch.
func (f *Frontier) Empty() bool {
	return len(f.pending) == 0
}

// Pop removes and returns the next CID to fetch.
// For DFS, this pops from the end of the stack.
func (f *Frontier) Pop() cid.Cid {
	if len(f.pending) == 0 {
		return cid.Undef
	}
	n := len(f.pending) - 1
	c := f.pending[n]
	f.pending = f.pending[:n]
	return c
}

// Seen returns true if the CID has already been processed.
func (f *Frontier) Seen(c cid.Cid) bool {
	_, ok := f.seen[c]
	return ok
}

// MarkSeen marks a CID as processed.
func (f *Frontier) MarkSeen(c cid.Cid) {
	f.seen[c] = struct{}{}
}

// PushAll adds multiple CIDs to the frontier (in reverse order for DFS).
// CIDs that have already been seen are skipped.
func (f *Frontier) PushAll(cids []cid.Cid) {
	// Add in reverse order so first child is processed first (DFS)
	for i := len(cids) - 1; i >= 0; i-- {
		c := cids[i]
		if _, ok := f.seen[c]; !ok {
			f.pending = append(f.pending, c)
		}
	}
}

// Size returns the number of pending CIDs.
func (f *Frontier) Size() int {
	return len(f.pending)
}

// SeenCount returns the number of CIDs that have been marked as seen.
func (f *Frontier) SeenCount() int {
	return len(f.seen)
}

// ExtractLinks extracts all CID links from a block based on its codec.
// Supported codecs: dag-pb, dag-cbor, dag-json, raw (no links).
func ExtractLinks(block blocks.Block) ([]cid.Cid, error) {
	c := block.Cid()
	codec := c.Prefix().Codec

	switch codec {
	case cid.Raw:
		// Raw blocks have no links
		return nil, nil

	case cid.DagProtobuf:
		return extractDagPBLinks(block.RawData())

	case cid.DagCBOR:
		return extractDagCBORLinks(block.RawData())

	case cid.DagJSON:
		return extractDagJSONLinks(block.RawData())

	default:
		// Unknown codec - try generic CBOR decoding, fall back to no links
		links, err := extractDagCBORLinks(block.RawData())
		if err != nil {
			return nil, nil
		}
		return links, nil
	}
}

// extractDagPBLinks extracts links from a dag-pb encoded block.
func extractDagPBLinks(data []byte) ([]cid.Cid, error) {
	// Use dagpb to decode the protobuf
	nb := dagpb.Type.PBNode.NewBuilder()
	if err := dagpb.DecodeBytes(nb, data); err != nil {
		return nil, err
	}
	node := nb.Build().(dagpb.PBNode)

	var cids []cid.Cid
	iter := node.Links.ListIterator()
	for !iter.Done() {
		_, linkNode, err := iter.Next()
		if err != nil {
			return nil, err
		}

		link := linkNode.(dagpb.PBLink)
		cids = append(cids, link.Hash.Link().(cidlink.Link).Cid)
	}

	return cids, nil
}

// extractDagCBORLinks extracts CID links from a dag-cbor encoded block.
func extractDagCBORLinks(data []byte) ([]cid.Cid, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagcbor.Decode(nb, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	node := nb.Build()
	return collectLinks(node), nil
}

// extractDagJSONLinks extracts CID links from a dag-json encoded block.
func extractDagJSONLinks(data []byte) ([]cid.Cid, error) {
	nb := basicnode.Prototype.Any.NewBuilder()
	if err := dagjson.Decode(nb, bytes.NewReader(data)); err != nil {
		return nil, err
	}
	node := nb.Build()
	return collectLinks(node), nil
}

// collectLinks recursively walks an IPLD node and collects all CID links.
func collectLinks(node ipld.Node) []cid.Cid {
	var links []cid.Cid

	switch node.Kind() {
	case ipld.Kind_Link:
		if link, err := node.AsLink(); err == nil {
			if cl, ok := link.(cidlink.Link); ok {
				links = append(links, cl.Cid)
			}
		}

	case ipld.Kind_Map:
		iter := node.MapIterator()
		for !iter.Done() {
			_, v, err := iter.Next()
			if err != nil {
				break
			}
			links = append(links, collectLinks(v)...)
		}

	case ipld.Kind_List:
		iter := node.ListIterator()
		for !iter.Done() {
			_, v, err := iter.Next()
			if err != nil {
				break
			}
			links = append(links, collectLinks(v)...)
		}
	}

	return links
}

package extractor

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/ipfs/go-unixfsnode/data"
	dagpb "github.com/ipld/go-codec-dagpb"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

var logger = log.Logger("sheltie/extractor")

// Extractor handles streaming extraction of UnixFS content to disk.
// It processes blocks one at a time and writes file/directory content
// as it arrives, without buffering entire files in memory.
type Extractor struct {
	outputDir string

	// track open file writers for chunked files
	// key is the root CID of the file being written
	openFiles map[cid.Cid]*fileWriter

	// track the current path context for each CID
	// this maps CID -> path so we know where to write
	pathContext map[cid.Cid]string

	// track file root for chunks (including intermediate nodes)
	// maps chunk/intermediate CID -> file ROOT CID
	fileRoot map[cid.Cid]cid.Cid

	// track processed CIDs to avoid duplicate processing
	processed map[cid.Cid]bool

	// pending raw blocks that arrived before their parent File node
	// maps CID -> raw block data
	pendingChunks map[cid.Cid][]byte
}

// chunkPosition tracks where a chunk should be written in a file
type chunkPosition struct {
	offset int64
	size   int64
}

type fileWriter struct {
	path      string
	file      *os.File
	expected  int64
	positions map[cid.Cid][]chunkPosition // CID -> all offsets where it appears
	pending   int                         // number of positions still needing data
}

// ChildLink represents a link to a child block with its name (for directories)
type ChildLink struct {
	Cid  cid.Cid
	Name string // empty for file chunks
}

// New creates a new Extractor that writes to the given output directory.
func New(outputDir string) (*Extractor, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	return &Extractor{
		outputDir:     outputDir,
		openFiles:     make(map[cid.Cid]*fileWriter),
		pathContext:   make(map[cid.Cid]string),
		fileRoot:      make(map[cid.Cid]cid.Cid),
		processed:     make(map[cid.Cid]bool),
		pendingChunks: make(map[cid.Cid][]byte),
	}, nil
}

// SetRootPath sets the path context for the root CID.
// Call this before processing to set the base name for single-file extractions.
// The name is sanitized to prevent path traversal.
func (e *Extractor) SetRootPath(rootCid cid.Cid, name string) {
	sanitized := sanitizeName(name)
	if sanitized == "" {
		// fall back to CID string if name is invalid
		sanitized = rootCid.String()
	}
	e.pathContext[rootCid] = sanitized
}

// ProcessBlock decodes a block and processes it according to its UnixFS type.
// Returns child CIDs that should be fetched next.
func (e *Extractor) ProcessBlock(ctx context.Context, block blocks.Block) ([]cid.Cid, error) {
	c := block.Cid()

	// skip already-processed blocks (handles duplicates in CAR stream)
	if e.processed[c] {
		logger.Debugw("ProcessBlock: skipping duplicate", "cid", c)
		return nil, nil
	}

	// check if this block belongs to an open file (chunk or intermediate node)
	rootCid, isFileChild := e.fileRoot[c]

	// try to decode as DAG-PB
	node, err := decodeBlock(block)
	if err != nil {
		// not DAG-PB - if it's a chunk, write it; otherwise treat as standalone raw
		if isFileChild {
			e.processed[c] = true
			return nil, e.writeChunk(c, rootCid, block.RawData())
		}
		return nil, e.handleRawBlock(c, block.RawData())
	}

	// check if it has UnixFS data
	if !node.FieldData().Exists() {
		// DAG-PB without UnixFS data - extract links only
		e.processed[c] = true
		return e.extractPBLinks(node), nil
	}

	ufsData, err := data.DecodeUnixFSData(node.Data.Must().Bytes())
	if err != nil {
		// not valid UnixFS - if it's a chunk, write it; otherwise treat as raw
		if isFileChild {
			e.processed[c] = true
			return nil, e.writeChunk(c, rootCid, block.RawData())
		}
		return nil, e.handleRawBlock(c, block.RawData())
	}

	dataType := ufsData.FieldDataType().Int()
	switch dataType {
	case data.Data_Directory:
		return e.processDirectory(c, node)
	case data.Data_File:
		// if this File is a child of another file, it's an intermediate node
		if isFileChild {
			return e.processIntermediateFile(c, rootCid, node, ufsData)
		}
		return e.processFile(c, node, ufsData)
	case data.Data_Raw:
		// Raw type with UnixFS wrapper - extract the data
		if isFileChild {
			e.processed[c] = true
			rawData := ufsData.FieldData().Must().Bytes()
			return nil, e.writeChunk(c, rootCid, rawData)
		}
		return nil, e.handleRawBlock(c, block.RawData())
	case data.Data_Symlink:
		return nil, e.processSymlink(c, ufsData)
	default:
		logger.Warnw("unsupported UnixFS type", "cid", c, "type", dataType)
		return nil, nil
	}
}

func (e *Extractor) processDirectory(c cid.Cid, node dagpb.PBNode) ([]cid.Cid, error) {
	path := e.getPath(c)
	if path == "" {
		path = c.String()
	}

	fullPath := filepath.Join(e.outputDir, path)
	if err := os.MkdirAll(fullPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", fullPath, err)
	}
	logger.Debugw("created directory", "path", fullPath)

	// extract child links with names
	var children []cid.Cid
	linksIter := node.Links.Iterator()
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid
		name := ""
		if link.Name.Exists() {
			name = link.Name.Must().String()
		}
		if name == "" {
			name = linkCid.String()
		}
		childPath, err := e.safePath(path, name)
		if err != nil {
			logger.Warnw("skipping unsafe path", "cid", linkCid, "name", name, "err", err)
			continue
		}
		e.pathContext[linkCid] = childPath
		children = append(children, linkCid)
	}

	e.processed[c] = true
	return children, nil
}

func (e *Extractor) processFile(c cid.Cid, node dagpb.PBNode, ufsData data.UnixFSData) ([]cid.Cid, error) {
	path := e.getPath(c)
	if path == "" {
		path = c.String()
	}

	fullPath := filepath.Join(e.outputDir, path)

	// ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory: %w", err)
	}

	// check if this is a single-block file or has children
	linksIter := node.Links.Iterator()
	if linksIter.Done() {
		// single block file - write inline data directly
		var fileData []byte
		if ufsData.FieldData().Exists() {
			fileData = ufsData.FieldData().Must().Bytes()
		}
		if err := os.WriteFile(fullPath, fileData, 0644); err != nil {
			return nil, fmt.Errorf("failed to write file %s: %w", fullPath, err)
		}
		logger.Debugw("wrote single-block file", "path", fullPath, "bytes", len(fileData))
		e.processed[c] = true
		return nil, nil
	}

	// chunked file - create file and track for chunk writes
	f, err := os.Create(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}

	var expectedSize int64
	if ufsData.FieldFileSize().Exists() {
		expectedSize = ufsData.FieldFileSize().Must().Int()
	}

	fw := &fileWriter{
		path:      fullPath,
		file:      f,
		expected:  expectedSize,
		positions: make(map[cid.Cid][]chunkPosition),
	}
	e.openFiles[c] = fw

	// write any inline data first (at offset 0)
	inlineOffset := int64(0)
	if ufsData.FieldData().Exists() {
		inlineData := ufsData.FieldData().Must().Bytes()
		if len(inlineData) > 0 {
			n, err := f.WriteAt(inlineData, 0)
			if err != nil {
				f.Close()
				return nil, fmt.Errorf("failed to write inline data: %w", err)
			}
			inlineOffset = int64(n)
		}
	}

	// build position map from links and blocksizes
	offset := inlineOffset
	linksIter = node.Links.Iterator()
	idx := 0
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid

		// get chunk size from blocksizes
		var chunkSize int64
		sizes := ufsData.FieldBlockSizes()
		if int64(idx) < sizes.Length() {
			sizeVal, _ := sizes.LookupByIndex(int64(idx))
			if sizeVal != nil {
				chunkSize, _ = sizeVal.AsInt()
			}
		}

		fw.positions[linkCid] = append(fw.positions[linkCid], chunkPosition{offset: offset, size: chunkSize})
		fw.pending++
		e.fileRoot[linkCid] = c

		offset += chunkSize
		idx++
	}

	// return deduplicated CIDs to fetch
	seen := make(map[cid.Cid]bool)
	var children []cid.Cid
	for linkCid := range fw.positions {
		if !seen[linkCid] {
			seen[linkCid] = true
			children = append(children, linkCid)
			// check if this chunk arrived before us (pending)
			if pendingData, ok := e.pendingChunks[linkCid]; ok {
				if err := e.writeChunk(linkCid, c, pendingData); err != nil {
					logger.Warnw("failed to write pending chunk", "cid", linkCid, "err", err)
				}
				delete(e.pendingChunks, linkCid)
			}
		}
	}

	logger.Debugw("started chunked file", "path", fullPath, "chunks", fw.pending, "uniqueCIDs", len(children), "expectedSize", expectedSize)
	e.processed[c] = true
	return children, nil
}

// processIntermediateFile handles File nodes that are children of another File.
// These are intermediate nodes in multi-level chunked files.
func (e *Extractor) processIntermediateFile(c, rootCid cid.Cid, node dagpb.PBNode, ufsData data.UnixFSData) ([]cid.Cid, error) {
	fw, ok := e.openFiles[rootCid]
	if !ok {
		return nil, fmt.Errorf("no open file for intermediate node %s (root %s)", c, rootCid)
	}

	// get this intermediate node's position(s) from the parent
	myPositions := fw.positions[c]
	if len(myPositions) == 0 {
		return nil, fmt.Errorf("intermediate node %s has no position in file", c)
	}
	// intermediate nodes should appear exactly once (they're internal structure, not data)
	baseOffset := myPositions[0].offset

	// remove this intermediate from positions and pending (it doesn't write data itself)
	delete(fw.positions, c)
	fw.pending -= len(myPositions)

	// write any inline data at base offset
	inlineLen := int64(0)
	if ufsData.FieldData().Exists() {
		inlineData := ufsData.FieldData().Must().Bytes()
		if len(inlineData) > 0 {
			_, err := fw.file.WriteAt(inlineData, baseOffset)
			if err != nil {
				return nil, fmt.Errorf("failed to write intermediate inline data: %w", err)
			}
			inlineLen = int64(len(inlineData))
		}
	}

	// add children to positions with offsets starting at baseOffset + inlineLen
	offset := baseOffset + inlineLen
	linksIter := node.Links.Iterator()
	idx := 0
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid

		// get chunk size from blocksizes
		var chunkSize int64
		sizes := ufsData.FieldBlockSizes()
		if int64(idx) < sizes.Length() {
			sizeVal, _ := sizes.LookupByIndex(int64(idx))
			if sizeVal != nil {
				chunkSize, _ = sizeVal.AsInt()
			}
		}

		fw.positions[linkCid] = append(fw.positions[linkCid], chunkPosition{offset: offset, size: chunkSize})
		fw.pending++
		e.fileRoot[linkCid] = rootCid

		offset += chunkSize
		idx++
	}

	// return deduplicated new CIDs to fetch
	seen := make(map[cid.Cid]bool)
	var children []cid.Cid
	linksIter = node.Links.Iterator()
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid
		if !seen[linkCid] {
			seen[linkCid] = true
			children = append(children, linkCid)
			// check if this chunk arrived before us (pending)
			if pendingData, ok := e.pendingChunks[linkCid]; ok {
				if err := e.writeChunk(linkCid, rootCid, pendingData); err != nil {
					logger.Warnw("failed to write pending chunk", "cid", linkCid, "err", err)
				}
				delete(e.pendingChunks, linkCid)
			}
		}
	}

	delete(e.fileRoot, c)
	e.processed[c] = true

	logger.Debugw("processed intermediate file node", "cid", c, "root", rootCid, "newChildren", len(children), "baseOffset", baseOffset)
	return children, nil
}

func (e *Extractor) writeChunk(chunkCid, fileCid cid.Cid, rawData []byte) error {
	fw, ok := e.openFiles[fileCid]
	if !ok {
		return fmt.Errorf("no open file for chunk %s (file %s)", chunkCid, fileCid)
	}

	positions := fw.positions[chunkCid]
	if len(positions) == 0 {
		return fmt.Errorf("chunk %s has no positions in file", chunkCid)
	}

	// for raw blocks, the entire block is file data
	// for DAG-PB blocks, we need to extract the UnixFS data
	chunkData := rawData

	// try to decode as DAG-PB to extract UnixFS data
	nb := dagpb.Type.PBNode.NewBuilder()
	if err := dagpb.DecodeBytes(nb, rawData); err == nil {
		pbNode := nb.Build().(dagpb.PBNode)
		if pbNode.FieldData().Exists() {
			ufsData, err := data.DecodeUnixFSData(pbNode.Data.Must().Bytes())
			if err == nil && ufsData.FieldData().Exists() {
				chunkData = ufsData.FieldData().Must().Bytes()
			}
		}
	}

	// write to ALL positions where this chunk appears
	for _, pos := range positions {
		_, err := fw.file.WriteAt(chunkData, pos.offset)
		if err != nil {
			return fmt.Errorf("failed to write chunk at offset %d: %w", pos.offset, err)
		}
		fw.pending--
	}

	// remove from positions map (this CID is done)
	delete(fw.positions, chunkCid)
	delete(e.fileRoot, chunkCid)

	// check if file is complete
	if fw.pending == 0 {
		e.closeFile(fileCid)
	}

	return nil
}

func (e *Extractor) handleRawBlock(c cid.Cid, rawData []byte) error {
	// check if this is a chunk for an open file
	if rootCid, ok := e.fileRoot[c]; ok {
		e.processed[c] = true
		return e.writeChunk(c, rootCid, rawData)
	}

	// if we have a path context, this is a standalone file (not a chunk)
	// otherwise it might be a chunk that arrived before its parent
	path := e.getPath(c)
	if path != "" {
		// has path context - write as standalone file
		fullPath := filepath.Join(e.outputDir, path)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			return fmt.Errorf("failed to create parent directory: %w", err)
		}
		if err := os.WriteFile(fullPath, rawData, 0644); err != nil {
			return fmt.Errorf("failed to write raw block %s: %w", fullPath, err)
		}
		logger.Debugw("wrote raw block", "path", fullPath, "bytes", len(rawData))
		e.processed[c] = true
		return nil
	}

	// no path context and not in fileRoot - save as pending chunk
	// this block arrived before its parent File node
	e.pendingChunks[c] = rawData
	return nil
}

func (e *Extractor) processSymlink(c cid.Cid, ufsData data.UnixFSData) error {
	// symlinks are not in MVP scope, log and skip
	logger.Warnw("symlink extraction not implemented", "cid", c)
	return nil
}

func (e *Extractor) extractPBLinks(node dagpb.PBNode) []cid.Cid {
	var links []cid.Cid
	linksIter := node.Links.Iterator()
	for !linksIter.Done() {
		_, link := linksIter.Next()
		linkCid := link.Hash.Link().(cidlink.Link).Cid
		links = append(links, linkCid)
	}
	return links
}

func (e *Extractor) getPath(c cid.Cid) string {
	if path, ok := e.pathContext[c]; ok {
		return path
	}
	return ""
}

// sanitizeName cleans a filename to prevent path traversal attacks.
// Returns the sanitized name and whether it was modified.
func sanitizeName(name string) string {
	// reject empty names
	if name == "" {
		return ""
	}
	// use only the base name (strips any directory components)
	name = filepath.Base(name)
	// reject . and ..
	if name == "." || name == ".." {
		return ""
	}
	// strip leading/trailing whitespace
	name = strings.TrimSpace(name)
	return name
}

// safePath joins a base path and name, ensuring the result stays within outputDir.
// Returns the joined path and an error if the path would escape.
func (e *Extractor) safePath(basePath, name string) (string, error) {
	sanitized := sanitizeName(name)
	if sanitized == "" {
		return "", fmt.Errorf("invalid filename: %q", name)
	}
	joined := filepath.Join(basePath, sanitized)
	// verify the result is still under outputDir
	fullPath := filepath.Join(e.outputDir, joined)
	absOutput, err := filepath.Abs(e.outputDir)
	if err != nil {
		return "", err
	}
	absFull, err := filepath.Abs(fullPath)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(absFull, absOutput+string(filepath.Separator)) && absFull != absOutput {
		return "", fmt.Errorf("path traversal attempt: %q", name)
	}
	return joined, nil
}

func (e *Extractor) closeFile(c cid.Cid) {
	if fw, ok := e.openFiles[c]; ok {
		fw.file.Close()
		logger.Debugw("closed file", "path", fw.path, "expected", fw.expected)
		delete(e.openFiles, c)
	}
}

// Close closes any open file handles. Call when extraction is complete.
func (e *Extractor) Close() error {
	var errs []error
	for c, fw := range e.openFiles {
		if err := fw.file.Close(); err != nil {
			errs = append(errs, fmt.Errorf("closing %s: %w", fw.path, err))
		}
		if fw.pending > 0 {
			logger.Warnw("incomplete file", "path", fw.path, "pendingChunks", fw.pending, "expected", fw.expected)
		}
		delete(e.openFiles, c)
	}
	if len(e.pendingChunks) > 0 {
		logger.Warnw("orphan pending chunks", "count", len(e.pendingChunks))
		e.pendingChunks = make(map[cid.Cid][]byte) // clear to free memory
	}
	if len(errs) > 0 {
		return fmt.Errorf("close errors: %v", errs)
	}
	return nil
}

func decodeBlock(block blocks.Block) (dagpb.PBNode, error) {
	c := block.Cid()
	// only DAG-PB blocks (codec 0x70) can be decoded as PBNode
	if c.Prefix().Codec != cid.DagProtobuf {
		return nil, fmt.Errorf("not a DAG-PB block: codec %x", c.Prefix().Codec)
	}

	// use dagpb prototype to get correct node type
	nb := dagpb.Type.PBNode.NewBuilder()
	if err := dagpb.DecodeBytes(nb, block.RawData()); err != nil {
		return nil, err
	}
	node := nb.Build()
	pbNode, ok := node.(dagpb.PBNode)
	if !ok {
		return nil, fmt.Errorf("decoded node is not PBNode: %T", node)
	}
	return pbNode, nil
}

// ensure Extractor satisfies any interface we define
var _ io.Closer = (*Extractor)(nil)

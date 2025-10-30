package indexerlookup

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/parkan/sheltie/pkg/types"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-log/v2"
	"github.com/multiformats/go-multibase"
)

var (
	_ types.CandidateSource = (*IndexerCandidateSource)(nil)

	logger = log.Logger("sheltie/indexerlookup")
)

type IndexerCandidateSource struct {
	*options
}

func NewCandidateSource(o ...Option) (*IndexerCandidateSource, error) {
	opts, err := newOptions(o...)
	if err != nil {
		return nil, err
	}
	return &IndexerCandidateSource{
		options: opts,
	}, nil
}

func (idxf *IndexerCandidateSource) FindCandidates(ctx context.Context, c cid.Cid, cb func(types.RetrievalCandidate)) error {
	req, err := idxf.newFindHttpRequest(ctx, c)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	logger.Debugw("sending outgoing request", "url", req.URL, "accept", req.Header.Get("Accept"))
	resp, err := idxf.httpClient.Do(req)
	if err != nil {
		logger.Debugw("Failed to perform lookup", "err", err)
		return err
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		return idxf.decodeDelegatedRoutingResponse(ctx, c, resp.Body, cb)
	case http.StatusNotFound:
		return nil
	case http.StatusTooManyRequests:
		retryAfter := resp.Header.Get("Retry-After")
		logger.Debugw("Rate limited by delegated routing server", "retry-after", retryAfter)
		return fmt.Errorf("rate limited (429), retry after: %s", retryAfter)
	default:
		return fmt.Errorf("provider lookup failed: %v", http.StatusText(resp.StatusCode))
	}
}

func (idxf *IndexerCandidateSource) newFindHttpRequest(ctx context.Context, c cid.Cid) (*http.Request, error) {
	endpoint := idxf.findByDelegatedRoutingEndpoint(c)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	if idxf.httpUserAgent != "" {
		req.Header.Set("User-Agent", idxf.httpUserAgent)
	}
	return req, nil
}

func (idxf *IndexerCandidateSource) decodeDelegatedRoutingResponse(ctx context.Context, c cid.Cid, from io.ReadCloser, cb func(types.RetrievalCandidate)) error {
	// Read the entire response body
	body, err := io.ReadAll(from)
	if err != nil {
		return fmt.Errorf("failed to read response body: %w", err)
	}

	// Parse as delegated routing response
	var response DelegatedRoutingResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("failed to unmarshal delegated routing response: %w", err)
	}

	// Process each provider
	for _, provider := range response.Providers {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Convert to AddrInfo
			addrInfo, err := provider.ToAddrInfo()
			if err != nil {
				logger.Debugw("Failed to convert provider to AddrInfo, skipping", "id", provider.ID, "err", err)
				continue
			}

			// Convert protocols to metadata
			md, err := provider.ToMetadata()
			if err != nil {
				logger.Debugw("Failed to convert provider metadata, skipping", "id", provider.ID, "err", err)
				continue
			}

			// Create candidate and callback
			candidate := types.RetrievalCandidate{
				MinerPeer: *addrInfo,
				RootCid:   c,
				Metadata:  md,
			}
			cb(candidate)
		}
	}

	return nil
}

// findByDelegatedRoutingEndpoint constructs the delegated routing API endpoint for a CID
// Normalizes CID to v1 base32 for better HTTP caching as per the spec
func (idxf *IndexerCandidateSource) findByDelegatedRoutingEndpoint(c cid.Cid) string {
	// Convert to CIDv1 for better HTTP caching
	cidV1 := cid.NewCidV1(c.Type(), c.Hash())

	// Encode as base32 (the recommended format for HTTP)
	cidStr, err := cidV1.StringOfBase(multibase.Base32)
	if err != nil {
		// Fallback to default string representation if base32 encoding fails
		cidStr = cidV1.String()
		logger.Debugw("Failed to encode CID as base32, using default", "err", err)
	}

	return idxf.httpEndpoint.JoinPath("routing", "v1", "providers", cidStr).String()
}

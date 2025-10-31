package events

import (
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/parkan/sheltie/pkg/types"
)

// Identifier returns the peer ID of the storage provider if this retrieval was
// requested via peer ID
func Identifier(evt types.RetrievalEvent) string {
	spEvent, spOk := evt.(EventWithProviderID)
	if spOk && spEvent.ProviderId() != peer.ID("") {
		return spEvent.ProviderId().String()
	}
	return ""
}

func collectProtocols(candidates []types.RetrievalCandidate) []multicodec.Code {
	allProtocols := make(map[multicodec.Code]struct{})
	for _, candidate := range candidates {
		for _, protocol := range candidate.Metadata.Protocols() {
			allProtocols[protocol] = struct{}{}
		}
	}
	allProtocolsArr := make([]multicodec.Code, 0, len(allProtocols))
	for protocol := range allProtocols {
		allProtocolsArr = append(allProtocolsArr, protocol)
	}
	return allProtocolsArr
}

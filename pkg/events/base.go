package events

import (
	"time"

	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multicodec"
	"github.com/parkan/sheltie/pkg/types"
)

type retrievalEvent struct {
	eventTime   time.Time
	retrievalId types.RetrievalID
	rootCid     cid.Cid
}

func (r retrievalEvent) Time() time.Time                { return r.eventTime }
func (r retrievalEvent) RetrievalId() types.RetrievalID { return r.retrievalId }
func (r retrievalEvent) RootCid() cid.Cid               { return r.rootCid }

type providerRetrievalEvent struct {
	retrievalEvent
	providerId peer.ID
	endpoint   string
}

func (e providerRetrievalEvent) ProviderId() peer.ID { return e.providerId }
func (e providerRetrievalEvent) Endpoint() string    { return e.endpoint }

type EventWithProviderID interface {
	types.RetrievalEvent
	ProviderId() peer.ID
}

type EventWithEndpoint interface {
	types.RetrievalEvent
	Endpoint() string
}

type EventWithCandidates interface {
	types.RetrievalEvent
	Candidates() []types.RetrievalCandidate
}

type EventWithProtocol interface {
	types.RetrievalEvent
	Protocol() multicodec.Code
}

type EventWithProtocols interface {
	types.RetrievalEvent
	Protocols() []multicodec.Code
}

type EventWithErrorMessage interface {
	types.RetrievalEvent
	ErrorMessage() string
}

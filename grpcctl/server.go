package grpcctl

import (
	"context"
	"github.com/Azure/azure-storage-azcopy/v10/grpcctl/internal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"sync"
	"time"
)

var GlobalServer = func() *Server {
	out := &Server{
		subscriptions: make(map[any]map[uuid.UUID]any),
		subLock:       &sync.RWMutex{},
	}

	return out
}()

var GlobalGRPCServer = func() *grpc.Server {
	s := grpc.NewServer()

	internal.RegisterAzCopyControlServer(s, GlobalServer)

	return s
}()

type Server struct {
	// map[In][Func] = set
	// Uses random IDs instead of functions because we can't hash those
	subscriptions map[any]map[uuid.UUID]any
	subLock       *sync.RWMutex
	internal.UnimplementedAzCopyControlServer
}

func Subscribe[I any](s *Server, event EventSubscriber[I]) uuid.UUID {
	var in I
	maxSubs := validSubscriptionCounts[in]
	if int64(len(s.subscriptions[in])) >= maxSubs {
		return uuid.Nil
	}

	if _, ok := s.subscriptions[in]; !ok {
		s.subscriptions[in] = make(map[uuid.UUID]any)
	}

	// Add the event to the table
	s.subLock.Lock()
	eventId := uuid.New()
	s.subscriptions[in][eventId] = event
	s.subLock.Unlock()

	return eventId
}

func Unsubscribe[I any](s *Server, event uuid.UUID) {
	var in I
	delete(s.subscriptions[in], event)
}

func fireEvent[I any](s *Server, input *I) {
	var in I

	s.subLock.RLock()
	for _, fun := range s.subscriptions[in] {
		fn, ok := fun.(EventSubscriber[I])
		if !ok {
			continue
		}

		// Run it async, nothing important is output (yet)
		go fn(input)
	}
	s.subLock.RUnlock()
}

func (s *Server) InjectToken(ctx context.Context, tok *internal.Token) (*internal.EmptyReply, error) {
	ok := "ok"

	// Fire events
	fireEvent(s, &OAuthTokenUpdate{
		Token:  tok.GetBearerToken(),
		Live:   time.Unix(tok.GetUnixLive(), 0),
		Expiry: time.Unix(tok.GetUnixExpiry(), 0),
		Wiggle: time.Duration(tok.GetWiggle()) * time.Second,
	})

	return &internal.EmptyReply{Status: &ok}, nil
}

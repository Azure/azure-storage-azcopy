package grpcctl

import (
	"context"
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/grpcctl/internal"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"reflect"
	"sync"
	"time"
)

var JobLog func(string)

var GlobalServer = func() *Server {
	out := &Server{
		subscriptions: make(map[string]map[uuid.UUID]EventSubscriberRaw),
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
	subscriptions map[string]map[uuid.UUID]EventSubscriberRaw
	subLock       *sync.RWMutex
	internal.UnimplementedAzCopyControlServer
}

func fetchSubscriptionName[I any](_ ...I) string {
	var in I
	return reflect.TypeOf(in).Name()
}

func Subscribe[I any](s *Server, event EventSubscriber[I]) uuid.UUID {
	maxSubs := validSubscriptionCounts[fetchSubscriptionName[I]()]
	if int64(len(s.subscriptions[fetchSubscriptionName[I]()])) >= maxSubs {
		return uuid.Nil
	}

	if _, ok := s.subscriptions[fetchSubscriptionName[I]()]; !ok {
		s.subscriptions[fetchSubscriptionName[I]()] = make(map[uuid.UUID]EventSubscriberRaw)
	}

	// Add the event to the table
	s.subLock.Lock()
	eventId := uuid.New()
	s.subscriptions[fetchSubscriptionName[I]()][eventId] = func(a any) {
		event(a.(*I))
	}
	s.subLock.Unlock()

	return eventId
}

func Unsubscribe[I any](s *Server, event uuid.UUID) {
	delete(s.subscriptions[fetchSubscriptionName[I]()], event)
}

func fireEvent[I any](s *Server, input *I, name string) {
	if JobLog != nil {
		JobLog("Received event " + name)
	}

	s.subLock.RLock()
	for _, fun := range s.subscriptions[fetchSubscriptionName[I]()] {
		if JobLog != nil {
			JobLog("Firing event " + name)
		}

		// Run it async, nothing important is output (yet)
		go fun(input)
	}
	s.subLock.RUnlock()
}

func (s *Server) InjectToken(ctx context.Context, tok *internal.Token) (*internal.EmptyReply, error) {
	ok := "ok"

	ev := &OAuthTokenUpdate{
		Token:  tok.GetBearerToken(),
		Live:   time.Unix(tok.GetUnixLive(), 0),
		Expiry: time.Unix(tok.GetUnixExpiry(), 0),
		Wiggle: time.Duration(tok.GetWiggle()) * time.Second,
	}

	// Warn about obvious cases where the token was completely invalid
	if ev.Token == "" {
		if JobLog != nil {
			JobLog("Received an empty OAuth token via GRPC")
		}
		return &internal.EmptyReply{}, errors.New("token was empty")
	}

	if ev.Live.After(ev.Expiry.Add(-ev.Wiggle)) || time.Now().After(ev.Expiry.Add(-ev.Wiggle)) {
		if JobLog != nil {
			JobLog("Received a pre-expired OAuth token via GRPC")
		}
		return &internal.EmptyReply{}, fmt.Errorf("token has expired before it was received (now: %v live: %v exp: %v), ")
	}

	// Fire events
	fireEvent(s, ev, "oauthtoken")

	return &internal.EmptyReply{Status: &ok}, nil
}

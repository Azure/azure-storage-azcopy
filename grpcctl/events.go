package grpcctl

import "time"

type EventSubscriber[I any] func(*I)

type EventSubscriberRaw func(any)

// 0 = none, -1 = infinite
var validSubscriptionCounts = map[string]int64{
	fetchSubscriptionName(OAuthTokenUpdate{}): 1,
}

// Inputs

// EventSubscriber = func(OAuthTokenUpdate) EmptyResponse
type OAuthTokenUpdate struct {
	Token  string
	Live   time.Time
	Expiry time.Time
	Wiggle time.Duration
}

// Outputs

type EmptyResponse struct{}

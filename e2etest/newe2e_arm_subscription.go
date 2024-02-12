package e2etest

import "net/url"

type ARMSubscription struct {
	*ARMClient
	SubscriptionID string
}

func (s *ARMSubscription) ManagementURI() url.URL {
	baseURI := s.ARMClient.ManagementURI()
	newURI := baseURI.JoinPath("subscriptions", s.SubscriptionID)

	return *newURI
}

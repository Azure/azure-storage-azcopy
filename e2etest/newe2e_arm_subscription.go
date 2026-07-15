package e2etest

import (
	"path/filepath"
)

type ARMSubscription struct {
	ParentSubject[*ARMClient] // should be ARMClient
	SubscriptionID            string
}

func (s *ARMSubscription) CanonicalPath() string {
	return filepath.Join(s.ParentSubject.CanonicalPath(), "subscriptions", s.SubscriptionID)
}

func (s *ARMSubscription) NewResourceGroupClient(rgName string) *ARMResourceGroup {
	return &ARMResourceGroup{
		ParentSubject: ParentSubject[*ARMSubscription]{
			parent: s,
			root:   s.root,
		},
		ResourceGroupName: rgName,
	}
}

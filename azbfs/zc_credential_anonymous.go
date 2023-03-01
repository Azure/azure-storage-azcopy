package azbfs

import (
	"context"

	"github.com/Azure/azure-pipeline-go/pipeline"
)

// Credential represent any credential type; it is used to create a credential policy Factory.
type Credential interface {
	pipeline.Factory
	credentialMarker()
}

//////////////////////////////

// NewAnonymousCredential creates an anonymous credential for use with HTTP(S) requests that read public resource
// or for use with Shared Access Signatures (SAS).
func NewAnonymousCredential() Credential {
	return anonymousCredentialFactory
}

var anonymousCredentialFactory Credential = &anonymousCredentialPolicyFactory{} // Singleton

// anonymousCredentialPolicyFactory is the credential's policy factory.
type anonymousCredentialPolicyFactory struct {
}

// New creates a credential policy object.
//nolint:unused
func (f *anonymousCredentialPolicyFactory) New(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.Policy {
	// Note: We are not deleting this "unused" code since this is a publicly exported function, we do not want to break
	// anyone that has a dependency on the azbfs library (like blobfuse).
	return &anonymousCredentialPolicy{next: next}
}

// credentialMarker is a package-internal method that exists just to satisfy the Credential interface.
//nolint:unused
func (*anonymousCredentialPolicyFactory) credentialMarker() {}

// anonymousCredentialPolicy is the credential's policy object.
type anonymousCredentialPolicy struct {
	next pipeline.Policy
}

// Do implements the credential's policy interface.
func (p anonymousCredentialPolicy) Do(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
	// For anonymous credentials, this is effectively a no-op
	return p.next.Do(ctx, request)
}

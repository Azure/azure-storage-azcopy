package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
)

func WorkloadIdentitySetup(a Asserter) {
	// Run only in environments that support and are set up for Workload Identity (ex: Azure Pipeline, Azure Kubernetes Service)
	if os.Getenv("NEW_E2E_ENVIRONMENT") != "AzurePipeline" {
		return // This is OK to skip, because other tests also skip if it isn't present.
	}

	tc, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{})
	a.NoError("Workload identity failed to spawn", err, true)
	_, err = tc.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{common.StorageScope},
	})
	a.NoError("Workload identity failed to fetch token", err, true)
}

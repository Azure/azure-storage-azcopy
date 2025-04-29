package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
)

func WorkloadIdentitySetup(a Asserter) {
	// Run only in environments that support and are set up for Workload Identity (ex: Azure Pipeline, Azure Kubernetes Service)
	if os.Getenv("NEW_E2E_ENVIRONMENT") != "TestEnvironmentAzurePipelines" {
		return // This is OK to skip, because other tests also skip if it isn't present.
	}

	workloadInfo := GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.DynamicOAuth.Workload
	// Get the value of the AZURE_FEDERATED_TOKEN environment variable
	token := workloadInfo.FederatedToken
	a.AssertNow("idToken must be specified to authenticate with workload identity", Empty{Invert: true}, token)
	// Write the token to a temporary file
	// Create a temporary file to store the token
	file, err := os.CreateTemp("", "azure_federated_token.txt")
	a.AssertNow("Error creating temporary file", IsNil{}, err)
	defer file.Close()

	// Write the token to the temporary file
	_, err = file.WriteString(token)
	a.AssertNow("Error writing to temporary file", IsNil{}, err)

	tc, err := azidentity.NewWorkloadIdentityCredential(&azidentity.WorkloadIdentityCredentialOptions{
		TenantID:      workloadInfo.TenantId,
		ClientID:      workloadInfo.ClientId,
		TokenFilePath: file.Name(),
	})
	a.NoError("Workload identity failed to spawn", err, true)
	_, err = tc.GetToken(ctx, policy.TokenRequestOptions{
		Scopes:    []string{common.StorageScope},
		EnableCAE: true,
	})
	a.NoError("Workload identity failed to fetch token", err, true)
}

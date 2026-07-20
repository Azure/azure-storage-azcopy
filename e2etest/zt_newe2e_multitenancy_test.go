package e2etest

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
)

func init() {
	suiteManager.RegisterSuite(&MultitenancySuite{})
}

type MultitenancySuite struct {
	credA, credB azcore.TokenCredential
}

func (s *MultitenancySuite) SetupSuite(a *ScenarioVariationManager) {
	if runInteractiveTest == nil || !*runInteractiveTest {
		a.Skip("interactive login disabled")
	}

	cfg := GlobalConfig.StaticMultitenantAcctInfo

	type tokenDefinition struct {
		name     string
		tenantID string

		record azidentity.AuthenticationRecord
		cred   *azidentity.InteractiveBrowserCredential
	}

	tokens := []tokenDefinition{
		{
			name:     ternary.Iff(cfg.TenantA.Name == "", "Tenant A", cfg.TenantA.Name),
			tenantID: cfg.TenantA.TenantID,
		},
		{
			name:     ternary.Iff(cfg.TenantB.Name == "", "Tenant B", cfg.TenantB.Name),
			tenantID: cfg.TenantB.TenantID,
		},
	}

	for idx := range tokens {
		def := &tokens[idx]
		opts := &azidentity.InteractiveBrowserCredentialOptions{
			TenantID: def.tenantID,
		}

		a.Log("Setting up credential for %s (%s)", def.name, def.tenantID)

		var err error
		def.cred, err = azidentity.NewInteractiveBrowserCredential(opts)
		a.NoError("create interactive browser credential", err)

		opts.AuthenticationRecord, err = def.cred.Authenticate(ctx, &policy.TokenRequestOptions{
			Scopes:    cred.DefaultAuthenticateScopes,
			EnableCAE: true,
		})
		a.NoError("authenticate interactive browser credential", err)
	}

	s.credA = tokens[0].cred
	s.credB = tokens[1].cred
}

// Scenario_BidirectionalTx tests that AzCopy can use two different credentials
// (different tenants) for source and destination in a single transfer
// across Blob, File, and BlobFS services.
func (s *MultitenancySuite) Scenario_BidirectionalTx(a *ScenarioVariationManager) {
	cfg := GlobalConfig.StaticMultitenantAcctInfo

	locationA := ResolveVariation(a, []common.Location{
		common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.BlobFS(),
	})
	locationB := ResolveVariation(a, []common.Location{
		common.ELocation.Blob(),
		common.ELocation.File(),
		common.ELocation.BlobFS(),
	})

	// Manually grab our storage accounts
	acctA := &AzureAccountResourceManager{
		InternalAccountName: cfg.TenantA.AccountName,
		InternalAccountType: EAccountType.Standard(),
		TokenCredential:     s.credA,
	}
	acctB := &AzureAccountResourceManager{
		InternalAccountName: cfg.TenantB.AccountName,
		InternalAccountType: EAccountType.Standard(),
		TokenCredential:     s.credB,
	}

	if a.Dryrun() {
		return // we are done resolving at this point
	}

	// Prepare our resources
	CombinedResourceMapping := ObjectResourceMappingFlat{
		"a": ResourceDefinitionObject{Body: NewStringObjectContentContainer("a")},
		"b": ResourceDefinitionObject{Body: NewStringObjectContentContainer("b")},
		"c": ResourceDefinitionObject{Body: NewStringObjectContentContainer("c")},
		"d": ResourceDefinitionObject{Body: NewStringObjectContentContainer("d")},
		"e": ResourceDefinitionObject{Body: NewStringObjectContentContainer("e")},
		"f": ResourceDefinitionObject{Body: NewStringObjectContentContainer("f")},
	}

	ccA := CreateResource[ContainerResourceManager](a, acctA.GetService(a, locationA), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"a": CombinedResourceMapping["a"],
			"b": CombinedResourceMapping["b"],
			"c": CombinedResourceMapping["c"],
		},
	})
	ccB := CreateResource[ContainerResourceManager](a, acctB.GetService(a, locationB), ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"d": CombinedResourceMapping["d"],
			"e": CombinedResourceMapping["e"],
			"f": CombinedResourceMapping["f"],
		},
	})

	a.Log("containers and objects created on both accounts")

	// Copy A → B
	a.Log("copying from %s to %s", cfg.TenantA.AccountName, cfg.TenantB.AccountName)
	RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{CreateAzCopyTarget(ccA, EExplicitCredentialType.PublicAuth(), a), CreateAzCopyTarget(ccB, EExplicitCredentialType.PublicAuth(), a)},
		Flags: CopySyncCommonFlags{
			Recursive: pointerTo(true),
			SrcCred:   pointerTo("tokenSrc"),
			DstCred:   pointerTo("tokenDst"),
		},
		Environment: &AzCopyEnvironment{
			KeyringConfig: map[string]KeyringEntry{
				"tokenSrc": {cfg.TenantA.TenantID, s.credA},
				"tokenDst": {cfg.TenantB.TenantID, s.credB},
			},
		},
	})

	ValidateResource(a, ccB, ResourceDefinitionContainer{
		Objects: CombinedResourceMapping,
	}, true)

	// Copy B → A (reverse direction)
	a.Log("copying from %s to %s", cfg.TenantB.AccountName, cfg.TenantA.AccountName)
	RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbCopy,
		Targets: []ResourceManager{CreateAzCopyTarget(ccB, EExplicitCredentialType.PublicAuth(), a), CreateAzCopyTarget(ccA, EExplicitCredentialType.PublicAuth(), a)},
		Flags: CopySyncCommonFlags{
			Recursive: pointerTo(true),
			SrcCred:   pointerTo("tokenSrc"),
			DstCred:   pointerTo("tokenDst"),
		},
		Environment: &AzCopyEnvironment{
			KeyringConfig: map[string]KeyringEntry{
				"tokenSrc": {cfg.TenantB.TenantID, s.credB},
				"tokenDst": {cfg.TenantA.TenantID, s.credA},
			},
		},
	})

	ValidateResource(a, ccA, ResourceDefinitionContainer{
		Objects: CombinedResourceMapping,
	}, true)
}

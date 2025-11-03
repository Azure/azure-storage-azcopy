package e2etest

import (
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/google/uuid"
)

// AccountRegistry is a set of accounts that are intended to be initialized when the tests start running.
// Suites and tests should not add to this pool.
// todo: long-term, support flexible static configuration of accounts.
var AccountRegistry = map[string]AccountResourceManager{} // For reusing accounts across testing

func GetAccount(a Asserter, AccountName string) AccountResourceManager {
	targetAccount, ok := AccountRegistry[AccountName]

	if d, isDryrunner := a.(DryrunAsserter); isDryrunner && d.Dryrun() {
		if !ok { // panic, because a dryrunning asserter will ignore assertions, and this test can't work.
			panic(fmt.Errorf("%s is not an available account in the registry", AccountName))
		}

		return &MockAccountResourceManager{accountName: targetAccount.AccountName(), accountType: targetAccount.AccountType()}
	}

	a.AssertNow(fmt.Sprintf("%s is not an available account in the registry", AccountName), Equal{}, ok, true)

	return targetAccount
}

type CreateAccountOptions struct {
	// ParentResourceGroup overrides CommonARMResourceGroup as a default.
	// If using a custom resource group, that RG should be automagically cleaned up.
	// The default RG automatically cleans up when the suite stops running.
	ParentResourceGroup *ARMResourceGroup
	// CustomName will be suffixed with the last section of a UUID
	CustomName *string

	// ParamMutator is intended for one-off excursions into boilerplate land
	ParamMutator func(createParams *ARMStorageAccountCreateParams)
}

func CreateAccount(a Asserter, accountType AccountType, options *CreateAccountOptions) AccountResourceManager {
	if d, isDryrunner := a.(DryrunAsserter); isDryrunner && d.Dryrun() {
		return &MockAccountResourceManager{accountType: accountType}
	}

	opts := DerefOrZero(options)

	uuidSegments := strings.Split(uuid.NewString(), "-")

	accountARMClient := &ARMStorageAccount{
		ARMResourceGroup: CommonARMResourceGroup,
		AccountName:      DerefOrDefault(opts.CustomName, "azcopynewe2e") + uuidSegments[len(uuidSegments)-1],
	}

	accountARMDefinition := ARMStorageAccountCreateParams{
		Location: "West US 2", // todo configurable
		Properties: &ARMStorageAccountCreateProperties{
			Tags: map[string]string{"Az.Sec.DisableAllowSharedKeyAccess::Skip": "Needed for AzCopy testing"},
		},
	}

	switch accountType { // https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal#storage-account-type-parameters
	case EAccountType.Standard():
		accountARMDefinition.Kind = service.AccountKindStorageV2
		accountARMDefinition.Sku = ARMStorageAccountSKUStandardLRS
	case EAccountType.HierarchicalNamespaceEnabled():
		accountARMDefinition.Kind = service.AccountKindStorageV2
		accountARMDefinition.Sku = ARMStorageAccountSKUStandardLRS
		accountARMDefinition.Properties.IsHnsEnabled = pointerTo(true)
	case EAccountType.PremiumBlockBlobs():
		accountARMDefinition.Kind = service.AccountKindBlockBlobStorage
		accountARMDefinition.Sku = ARMStorageAccountSKUPremiumLRS
	case EAccountType.PremiumFileShares():
		accountARMDefinition.Kind = service.AccountKindFileStorage
		accountARMDefinition.Sku = ARMStorageAccountSKUPremiumLRS
	case EAccountType.PremiumPageBlobs():
		accountARMDefinition.Kind = service.AccountKindStorageV2
		accountARMDefinition.Sku = ARMStorageAccountSKUPremiumLRS
	default:
		a.Error(fmt.Sprintf("%s is not currently supported for account creation", accountType))
	}

	if opts.ParamMutator != nil { // If the
		opts.ParamMutator(&accountARMDefinition)
	}

	_, err := accountARMClient.Create(accountARMDefinition)
	a.NoError("ARM create account call", err, true)
	keys, err := accountARMClient.GetKeys()
	a.NoError("ARM get keys call", err, true)

	acct := &AzureAccountResourceManager{
		InternalAccountName: accountARMClient.AccountName,
		InternalAccountKey:  keys.Keys[0].Value, // todo find useful key
		InternalAccountType: accountType,
		ArmClient:           accountARMClient,
	}

	if rt, ok := a.(ResourceTracker); ok {
		rt.TrackCreatedAccount(acct)
	}

	return acct
}

func DeleteAccount(a Asserter, arm AccountResourceManager) {
	switch arm.AccountType() {
	case EAccountType.Standard(), EAccountType.PremiumPageBlobs(), EAccountType.PremiumFileShares(),
		EAccountType.PremiumBlockBlobs(), EAccountType.PremiumHNSEnabled(), EAccountType.HierarchicalNamespaceEnabled():
		azureAcct, ok := arm.(*AzureAccountResourceManager)
		a.Assert("account manager must be azure account", Equal{}, ok, true)

		armAcct := azureAcct.ManagementClient()
		a.Assert("cannot delete an account that does not have a management client associated", Not{IsNil{}}, armAcct)

		a.NoError("delete account", armAcct.Delete())
	default:
		a.Error(fmt.Sprintf("account type %s is not yet supported", arm.AccountType()))
	}
}

const (
	PrimaryStandardAcct  string = "PrimaryStandard"
	PrimaryHNSAcct       string = "PrimaryHNS"
	PremiumPageBlobAcct  string = "PremiumPageBlob"
	PremiumFileShareAcct string = "PremiumFileShare"
)

func AccountRegistryInitHook(a Asserter) {
	if GlobalConfig.StaticResources() {
		// ===== Shorthand accesses =====
		acctInfo := GlobalConfig.E2EAuthConfig.StaticStgAcctInfo
		lookupInfo := GlobalConfig.E2EAuthConfig.StaticStgAcctInfo.AccountKeyLookup
		resGroup := CommonARMClient.
			NewSubscriptionClient(lookupInfo.SubscriptionID).
			NewResourceGroupClient(lookupInfo.ResourceGroup)
		canLookup := GlobalConfig.CanLookupStaticAcctKeys()

		// ===== Account registration logic =====
		registerAccount := func(registryKey, acctName, acctKey string, acctType AccountType) {
			if acctName == "" {
				return // nothing to do, period
			}

			resMan := &AzureAccountResourceManager{
				InternalAccountName: acctName,
				InternalAccountKey:  acctKey,
				InternalAccountType: acctType,
			}

			resourcePrepared := acctKey == ""

			if !resourcePrepared {
				if canLookup {
					armClient := resGroup.NewStorageAccountARMClient(acctName)
					keys, err := armClient.GetKeys()

					if err == nil {
						resourcePrepared = true
						resMan.InternalAccountKey = keys.Keys[0].Value
						// todo: having an arm client doesn't always mean that we've _created_ the account this run. Enable assigning this in case we need it.
					}
				}

				// If we still don't have a key, we need to fall back to OAuth.
				// Notably, a lookup could fail, so we don't want this to be an else.
				if !resourcePrepared {
					a.Log("account %s falling back to OAuth-only mode. SAS tests using this account will be converted to OAuth tests.", registryKey)
					if PrimaryOAuthCache.tc != nil {
						// todo account side magicks
						resourcePrepared = true
					}
				}
			}

			if resourcePrepared { // we don't want to add an account that can never work
				AccountRegistry[registryKey] = resMan
			}
		}

		// ===== Define accounts =====
		registerAccount(
			PrimaryStandardAcct,
			acctInfo.Standard.AccountName,
			acctInfo.Standard.AccountKey,
			EAccountType.Standard(),
		)

		registerAccount(
			PrimaryHNSAcct,
			acctInfo.HNS.AccountName,
			acctInfo.HNS.AccountKey,
			EAccountType.HierarchicalNamespaceEnabled(),
		)

		registerAccount(
			PremiumPageBlobAcct,
			acctInfo.PremiumPage.AccountName,
			acctInfo.PremiumPage.AccountKey,
			EAccountType.PremiumPageBlobs(),
		)

		registerAccount(
			PremiumFileShareAcct,
			acctInfo.PremiumFileShare.AccountName,
			acctInfo.PremiumFileShare.AccountKey,
			EAccountType.PremiumFileShares(),
		)
	} else {
		// Create standard accounts
		AccountRegistry[PrimaryStandardAcct] = CreateAccount(a, EAccountType.Standard(), nil)
		AccountRegistry[PrimaryHNSAcct] = CreateAccount(a, EAccountType.HierarchicalNamespaceEnabled(), nil)
		AccountRegistry[PremiumPageBlobAcct] = CreateAccount(a, EAccountType.PremiumPageBlobs(), nil)
		AccountRegistry[PremiumFileShareAcct] = CreateAccount(a, EAccountType.PremiumFileShares(), nil)
	}
}

func AccountRegistryCleanupHook(a Asserter) {
	if GlobalConfig.StaticResources() {
		return // no need to attempt cleanup
	}

	for _, v := range AccountRegistry {
		if acct, ok := v.(*AzureAccountResourceManager); ok && acct.ManagementClient() != nil {
			managementClient := acct.ManagementClient()
			a.Assert("Delete account", NoError{}, managementClient.Delete())
		}
	}
}

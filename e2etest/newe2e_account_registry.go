package e2etest

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	"github.com/google/uuid"
	"strings"
)

// AccountRegistry is a set of accounts that are intended to be initialized when the tests start running.
// Suites and tests should not add to this pool.
// todo: long-term, support flexible static configuration of accounts.
var AccountRegistry = map[string]AccountResourceManager{} // For re-using accounts across testing

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
	}

	switch accountType { // https://learn.microsoft.com/en-us/azure/storage/common/storage-account-create?tabs=azure-portal#storage-account-type-parameters
	case EAccountType.Standard():
		accountARMDefinition.Kind = service.AccountKindStorageV2
		accountARMDefinition.Sku = ARMStorageAccountSKUStandardLRS
	case EAccountType.HierarchicalNamespaceEnabled():
		accountARMDefinition.Kind = service.AccountKindStorageV2
		accountARMDefinition.Sku = ARMStorageAccountSKUStandardLRS
		accountARMDefinition.Properties = &ARMStorageAccountCreateProperties{
			IsHnsEnabled: pointerTo(true),
		}
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
	a.NoError("ARM create account call", err)
	keys, err := accountARMClient.GetKeys()
	a.NoError("ARM get keys call", err)

	acct := &AzureAccountResourceManager{
		accountName: accountARMClient.AccountName,
		accountKey:  keys.Keys[0].Value, // todo find useful key
		accountType: accountType,
		armClient:   accountARMClient,
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
	PrimaryStandardAcct string = "PrimaryStandard"
	PrimaryHNSAcct      string = "PrimaryHNS"
)

func AccountRegistryInitHook(a Asserter) {
	if GlobalConfig.StaticResources() {
		acctInfo := GlobalConfig.E2EAuthConfig.StaticStgAcctInfo

		AccountRegistry[PrimaryStandardAcct] = &AzureAccountResourceManager{
			accountName: acctInfo.Standard.AccountName,
			accountKey:  acctInfo.Standard.AccountKey,
			accountType: EAccountType.Standard(),
		}
		AccountRegistry[PrimaryHNSAcct] = &AzureAccountResourceManager{
			accountName: acctInfo.HNS.AccountName,
			accountKey:  acctInfo.HNS.AccountKey,
			accountType: EAccountType.HierarchicalNamespaceEnabled(),
		}
	} else {
		// Create standard accounts
		AccountRegistry[PrimaryStandardAcct] = CreateAccount(a, EAccountType.Standard(), nil)
		AccountRegistry[PrimaryHNSAcct] = CreateAccount(a, EAccountType.HierarchicalNamespaceEnabled(), nil)
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

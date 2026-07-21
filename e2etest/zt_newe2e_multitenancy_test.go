package e2etest

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/google/uuid"
)

func init() {
	suiteManager.RegisterSuite(&MultitenancySuite{})
}

type MultitenancySuite struct {
	credA, credB cred.Token
	keyring      cred.Keyring

	azcopyEnvironment AzCopyEnvironment
}

const (
	MultitenancyNicknameA = "TenantA"
	MultitenancyNicknameB = "TenantB"
)

func (s *MultitenancySuite) SetupSuite(a *ScenarioVariationManager) {
	if runInteractiveTest == nil || !*runInteractiveTest {
		a.Skip("interactive tests not requested")
		return // don't register interactive tests if they are to be skipped by default.
	}

	env := AzCopyEnvironment{
		LoginCacheName: pointerTo("AzCopyMultitenancyInteractiveTest"),
		ManualLogin:    true,
	}
	s.azcopyEnvironment = env // copy before we write, so that we don't overwrite per-test details

	cfg := GlobalConfig.StaticMultitenantAcctInfo

	// Try to load existing credentials from keyring first, to skip interactive login
	keyring, err := cred.GetOSKeyring(cred.GetOSKeyringOptions{
		OSKeyringCacheName: s.azcopyEnvironment.LoginCacheName,
	})
	if err == nil {
		s.keyring = keyring
		var credAOk, credBOk bool
		s.credA, credAOk = s.keyring.GetToken(MultitenancyNicknameA)
		s.credB, credBOk = s.keyring.GetToken(MultitenancyNicknameB)

		if credAOk && credBOk {
			a.Log("found existing credentials for %q and %q in keyring, skipping interactive login",
				MultitenancyNicknameA, MultitenancyNicknameB)
			return
		}

		a.Log("keyring loaded but missing credentials, falling through to interactive login")
		// Reset partial state so we don't end up with one working cred and one broken
		s.credA = nil
		s.credB = nil
	} else {
		a.Log("failed to load keyring: %v, will perform interactive login", err)
	}

	RunAzCopy(a, AzCopyCommand{
		Verb:        AzCopyVerbLogin,
		Environment: &env,
		Flags: LoginFlags{
			LoginType: pointerTo(enum.EAutoLoginType.Interactive()),
			TenantID:  &cfg.TenantA.TenantID,
			Nickname:  PtrOf("TenantA"),
		},
	})

	RunAzCopy(a, AzCopyCommand{
		Verb:        AzCopyVerbLogin,
		Environment: &env,
		Flags: LoginFlags{
			LoginType: pointerTo(enum.EAutoLoginType.Interactive()),
			TenantID:  &cfg.TenantB.TenantID,
			Nickname:  PtrOf("TenantB"),
		},
	})

	s.keyring, err = cred.GetOSKeyring(cred.GetOSKeyringOptions{
		OSKeyringCacheName: s.azcopyEnvironment.LoginCacheName,
	})
	a.NoError("fetch keyring", err, true)

	var ok bool
	s.credA, ok = s.keyring.GetToken(MultitenancyNicknameA)
	a.Assert("must retrieve tenant A cred", Equal{}, ok, true)
	s.credB, ok = s.keyring.GetToken(MultitenancyNicknameB)
	a.Assert("must retrieve tenant B cred", Equal{}, ok, true)
}

func (s *MultitenancySuite) Scenario_E2ELoginTest(a *ScenarioVariationManager) {
	cfg := GlobalConfig.StaticMultitenantAcctInfo
	env := s.azcopyEnvironment

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

	azCopyVerb := ResolveVariation(a, []AzCopyVerb{AzCopyVerbCopy, AzCopyVerbSync})

	var fetchTc = func(t cred.Token) azcore.TokenCredential {
		tc, err := t.TokenCredential(ctx)
		a.NoError("failed to retrieve tokencredential for "+t.Header().Nickname, err, true)
		return tc
	}

	nameA, typeA := resolveMultitenantAccount(a, locationA, cfg.TenantA.AccountName, cfg.TenantA.HNSAccountName)
	acctA := &AzureAccountResourceManager{
		InternalAccountName: nameA,
		InternalAccountType: typeA,
		TokenCredential:     fetchTc(s.credA),
	}
	nameB, typeB := resolveMultitenantAccount(a, locationB, cfg.TenantB.AccountName, cfg.TenantB.HNSAccountName)
	acctB := &AzureAccountResourceManager{
		InternalAccountName: nameB,
		InternalAccountType: typeB,
		TokenCredential:     fetchTc(s.credB),
	}

	if a.Dryrun() {
		return
	}

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

	// File->Blob does not work oauth
	if !(locationA == common.ELocation.File() && locationB != common.ELocation.File()) {
		a.Log("containers and objects created on both accounts")
		a.Log("running %s from %s to %s", azCopyVerb, cfg.TenantA.AccountName, cfg.TenantB.AccountName)
		RunAzCopy(a, AzCopyCommand{
			Verb:    azCopyVerb,
			Targets: []ResourceManager{CreateAzCopyTarget(ccA, EExplicitCredentialType.OAuth(), a), CreateAzCopyTarget(ccB, EExplicitCredentialType.OAuth(), a)},
			Flags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
				SrcCred:   pointerTo(MultitenancyNicknameA),
				DstCred:   pointerTo(MultitenancyNicknameB),
			},
			Environment: &env,
		})

		ValidateResource(a, ccB, ResourceDefinitionContainer{
			Objects: CombinedResourceMapping,
		}, true)
	}

	if !(locationB == common.ELocation.File() && locationA != common.ELocation.File()) {
		a.Log("running %s from %s to %s (reverse direction)", azCopyVerb, cfg.TenantB.AccountName, cfg.TenantA.AccountName)
		RunAzCopy(a, AzCopyCommand{
			Verb:    azCopyVerb,
			Targets: []ResourceManager{CreateAzCopyTarget(ccB, EExplicitCredentialType.OAuth(), a), CreateAzCopyTarget(ccA, EExplicitCredentialType.OAuth(), a)},
			Flags: CopySyncCommonFlags{
				Recursive: pointerTo(true),
				SrcCred:   pointerTo(MultitenancyNicknameB),
				DstCred:   pointerTo(MultitenancyNicknameA),
			},
			Environment: &env,
		})

		ValidateResource(a, ccA, ResourceDefinitionContainer{
			Objects: CombinedResourceMapping,
		}, true)
	}
}

func (s *MultitenancySuite) Scenario_NamedCredentialList(a *ScenarioVariationManager) {
	if runInteractiveTest == nil || !*runInteractiveTest {
		a.Skip("interactive tests not requested")
		return
	}

	cfg := GlobalConfig.StaticMultitenantAcctInfo
	env := s.azcopyEnvironment

	location := common.ELocation.Blob()

	fetchTc := func(t cred.Token) azcore.TokenCredential {
		tc, err := t.TokenCredential(ctx)
		a.NoError("failed to retrieve tokencredential", err, true)
		return tc
	}

	name, acctType := resolveMultitenantAccount(a, location, cfg.TenantA.AccountName, cfg.TenantA.HNSAccountName)
	acct := &AzureAccountResourceManager{
		InternalAccountName: name,
		InternalAccountType: acctType,
		TokenCredential:     fetchTc(s.credA),
	}
	svc := acct.GetService(a, location)
	container := CreateResource[ContainerResourceManager](a, svc, ResourceDefinitionContainer{
		Objects: ObjectResourceMappingFlat{
			"obj1": ResourceDefinitionObject{Body: NewStringObjectContentContainer("content1")},
			"obj2": ResourceDefinitionObject{Body: NewStringObjectContentContainer("content2")},
		},
	})

	if a.Dryrun() {
		return
	}

	stdout, _ := RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbList,
		Targets: []ResourceManager{CreateAzCopyTarget(container, EExplicitCredentialType.OAuth(), a)},
		Flags: ListFlags{
			Cred: pointerTo(MultitenancyNicknameA),
		},
		Environment: &env,
	})

	ValidateListOutput(a, stdout, map[AzCopyOutputKey]cmd.AzCopyListObject{
		{Path: "obj1"}: {Path: "obj1", ContentLength: "8.00 B"},
		{Path: "obj2"}: {Path: "obj2", ContentLength: "8.00 B"},
	}, nil)
}

func (s *MultitenancySuite) Scenario_NamedCredentialRemove(a *ScenarioVariationManager) {
	if runInteractiveTest == nil || !*runInteractiveTest {
		a.Skip("interactive tests not requested")
		return
	}

	cfg := GlobalConfig.StaticMultitenantAcctInfo
	env := s.azcopyEnvironment

	location := common.ELocation.Blob()

	fetchTc := func(t cred.Token) azcore.TokenCredential {
		tc, err := t.TokenCredential(ctx)
		a.NoError("failed to retrieve tokencredential", err, true)
		return tc
	}

	name, acctType := resolveMultitenantAccount(a, location, cfg.TenantA.AccountName, cfg.TenantA.HNSAccountName)
	acct := &AzureAccountResourceManager{
		InternalAccountName: name,
		InternalAccountType: acctType,
		TokenCredential:     fetchTc(s.credA),
	}
	svc := acct.GetService(a, location)
	obj := CreateResource[ObjectResourceManager](a, svc, ResourceDefinitionObject{
		ObjectName: pointerTo("objToRemove"),
		Body:       NewStringObjectContentContainer("content"),
	})

	if a.Dryrun() {
		return
	}

	RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbRemove,
		Targets: []ResourceManager{CreateAzCopyTarget(obj, EExplicitCredentialType.OAuth(), a)},
		Flags: RemoveFlags{
			Cred:      pointerTo(MultitenancyNicknameA),
			Recursive: pointerTo(true),
		},
		Environment: &env,
	})

	ValidateResource(a, obj, ResourceDefinitionObject{
		ObjectShouldExist: pointerTo(false),
	}, false)
}

func (s *MultitenancySuite) Scenario_NamedCredentialSetProperties(a *ScenarioVariationManager) {
	if runInteractiveTest == nil || !*runInteractiveTest {
		a.Skip("interactive tests not requested")
		return
	}

	cfg := GlobalConfig.StaticMultitenantAcctInfo
	env := s.azcopyEnvironment

	location := common.ELocation.Blob()

	fetchTc := func(t cred.Token) azcore.TokenCredential {
		tc, err := t.TokenCredential(ctx)
		a.NoError("failed to retrieve tokencredential", err, true)
		return tc
	}

	name, acctType := resolveMultitenantAccount(a, location, cfg.TenantA.AccountName, cfg.TenantA.HNSAccountName)
	acct := &AzureAccountResourceManager{
		InternalAccountName: name,
		InternalAccountType: acctType,
		TokenCredential:     fetchTc(s.credA),
	}
	svc := acct.GetService(a, location)
	obj := CreateResource[ObjectResourceManager](a, svc, ResourceDefinitionObject{
		ObjectName: pointerTo("objToSetProps"),
		Body:       NewStringObjectContentContainer("content"),
	})

	if a.Dryrun() {
		return
	}

	RunAzCopy(a, AzCopyCommand{
		Verb:    AzCopyVerbSetProperties,
		Targets: []ResourceManager{CreateAzCopyTarget(obj, EExplicitCredentialType.OAuth(), a)},
		Flags: SetPropertiesFlags{
			Cred:     pointerTo(MultitenancyNicknameA),
			Metadata: pointerTo("mykey=myvalue"),
		},
		Environment: &env,
	})

	ValidateResource(a, obj, ResourceDefinitionObject{
		ObjectProperties: ObjectProperties{
			Metadata: common.Metadata{
				"mykey": pointerTo("myvalue"),
			},
		},
	}, false)
}

func (s *MultitenancySuite) Scenario_NamedCredentialMake(a *ScenarioVariationManager) {
	if runInteractiveTest == nil || !*runInteractiveTest {
		a.Skip("interactive tests not requested")
		return
	}

	cfg := GlobalConfig.StaticMultitenantAcctInfo
	env := s.azcopyEnvironment
	location := common.ELocation.Blob()

	fetchTc := func(t cred.Token) azcore.TokenCredential {
		tc, err := t.TokenCredential(ctx)
		a.NoError("failed to retrieve tokencredential", err, true)
		return tc
	}

	name, acctType := resolveMultitenantAccount(a, location, cfg.TenantA.AccountName, cfg.TenantA.HNSAccountName)
	acct := &AzureAccountResourceManager{
		InternalAccountName: name,
		InternalAccountType: acctType,
		TokenCredential:     fetchTc(s.credA),
	}
	svc := acct.GetService(a, location)

	containerName := uuid.NewString()
	containerURL := fmt.Sprintf("https://%s.blob.core.windows.net/%s", name, containerName)

	containerHandle := svc.GetContainer(containerName)
	a.Assert("container should not exist before make", Equal{}, containerHandle.Exists(), false)

	if a.Dryrun() {
		return
	}

	RunAzCopy(a, AzCopyCommand{
		Verb:           AzCopyVerbMake,
		PositionalArgs: []string{containerURL},
		Flags: MakeFlags{
			Cred: pointerTo(MultitenancyNicknameA),
		},
		Environment: &env,
	})

	a.Assert("container should exist after make", Equal{}, containerHandle.Exists(), true)
}

func resolveMultitenantAccount(a Asserter, location common.Location, name, hnsName string) (string, AccountType) {
	if location == common.ELocation.BlobFS() {
		if hnsName != "" {
			return hnsName, EAccountType.HierarchicalNamespaceEnabled()
		}
		a.Log("WARNING: BlobFS selected but no HNS account is configured for this tenant; falling back to the standard account. Features incompatible with the HNS endpoint (e.g. versioning) may fail.")
	}
	return name, EAccountType.Standard()
}

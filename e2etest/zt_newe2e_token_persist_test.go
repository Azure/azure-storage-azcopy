package e2etest

import (
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
)

func init() {
	suiteManager.RegisterEarlyRunSuite(&TokenPersistenceSuite{})
}

type TokenPersistenceSuite struct{}

func (*TokenPersistenceSuite) Scenario_InheritCred_Persist(a *ScenarioVariationManager) {
	credSource := ResolveVariation(a, []enum.AutoLoginType{enum.EAutoLoginType.PsCred(), enum.EAutoLoginType.AzCLI()})
	withSpecifiedTenantID := NamedResolveVariation(a, map[string]bool{
		"-withTenantID": true,
		"":              false,
	})
	cfgTenantID := GlobalConfig.GetTenantID()

	azcopyEnv := &AzCopyEnvironment{
		LoginCacheName: pointerTo(fmt.Sprintf("AzCopyPersist%sTest", credSource.String())),
		// InheritEnvironment now includes PATH
		ManualLogin: true,
	}

	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLogin,
			Flags: LoginFlags{
				LoginType: &credSource,
				TenantID:  ternary.Iff(withSpecifiedTenantID && cfgTenantID != "", &cfgTenantID, nil),
				Nickname:  PtrOf(cred.DefaultNickname),
			},
			Environment: azcopyEnv,
		})

	loginStatus, _ := RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLoginStatus,
			Flags: LoginStatusFlags{
				Method:   pointerTo(true),
				Endpoint: pointerTo(true),
				Tenant:   pointerTo(true),
				Nickname: PtrOf(cred.DefaultNickname),
			},
			Environment: azcopyEnv,
		})

	parsedStdout, ok := loginStatus.(*AzCopyParsedLoginStatusStdout)
	a.AssertNow("must be AzCopyParsedLoginStatusStdout", Equal{}, ok, true)

	if !a.Dryrun() {
		identity, ok := parsedStdout.status.Identities[cred.DefaultNickname]
		a.AssertNow("default identity not returned", Equal{}, ok, true)
		a.Assert("Login check failed", Equal{}, true, identity.Valid)

		a.Assert("Tenant not returned", Empty{Invert: true}, identity.TenantID) // Let's just do a little extra testing while we're at it, kill two birds with one stone.
		if withSpecifiedTenantID && cfgTenantID != "" {
			a.Assert("Tenant does not match", Equal{}, cfgTenantID, identity.TenantID)
		}
		a.Assert("Endpoint not returned", Empty{Invert: true}, identity.AADEndpoint)
		a.Assert("Incorrect auth mechanism", Equal{}, identity.AuthMethod, credSource.String())
	}

	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLogout,
			Flags: LogoutFlags{
				Nickname: PtrOf(cred.DefaultNickname),
			},
			Environment: azcopyEnv,
		})
}

func (*TokenPersistenceSuite) Scenario_MultiLogin_Persist(a *ScenarioVariationManager) {
	if runInteractiveTest == nil || !*runInteractiveTest {
		a.Skip("device code disabled")
	}

	cfgTenantID := GlobalConfig.GetTenantID()

	env := &AzCopyEnvironment{
		LoginCacheName: pointerTo("AzCopyMultiLoginInteractiveTest"),
		ManualLogin:    true,
	}

	a.Log("Logging in the first credential, interactive")
	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLogin,
			Flags: LoginFlags{
				LoginType: pointerTo(enum.EAutoLoginType.Interactive()),
				TenantID:  &cfgTenantID,
				Nickname:  PtrOf("testToken1"),
			},
			Environment: env,
		})

	a.Log("Logging in the second credential, interactive")
	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLogin,
			Flags: LoginFlags{
				LoginType: pointerTo(enum.EAutoLoginType.Interactive()),
				TenantID:  &cfgTenantID,
				Nickname:  PtrOf("testToken2"),
			},
			Environment: env,
		})

	loginStatus, _ := RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLoginStatus,
			Flags: LoginStatusFlags{
				Method:   pointerTo(true),
				Endpoint: pointerTo(true),
				Tenant:   pointerTo(true),
			},
			Environment: env,
		})

	parsedStdout, ok := loginStatus.(*AzCopyParsedLoginStatusStdout)
	a.AssertNow("must be AzCopyParsedLoginStatusStdout", Equal{}, ok, true)

	if !a.Dryrun() {
		identity1, ok := parsedStdout.status.Identities["testToken1"]
		a.AssertNow("token1 not returned", Equal{}, ok, true)
		a.Assert("token1 login check failed", Equal{}, true, identity1.Valid)
		a.Assert("token1 tenant not returned", Empty{Invert: true}, identity1.TenantID)
		a.Assert("token1 tenant does not match", Equal{}, cfgTenantID, identity1.TenantID)
		a.Assert("token1 endpoint not returned", Empty{Invert: true}, identity1.AADEndpoint)
		a.Assert("token1 incorrect auth method", Equal{}, identity1.AuthMethod, enum.EAutoLoginType.Interactive().String())

		identity2, ok := parsedStdout.status.Identities["testToken2"]
		a.AssertNow("token2 not returned", Equal{}, ok, true)
		a.Assert("token2 login check failed", Equal{}, true, identity2.Valid)
		a.Assert("token2 tenant not returned", Empty{Invert: true}, identity2.TenantID)
		a.Assert("token2 tenant does not match", Equal{}, cfgTenantID, identity2.TenantID)
		a.Assert("token2 endpoint not returned", Empty{Invert: true}, identity2.AADEndpoint)
		a.Assert("token2 incorrect auth method", Equal{}, identity2.AuthMethod, enum.EAutoLoginType.Interactive().String())
	}

	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLogout,
			Flags: LogoutFlags{
				Nickname: PtrOf("testToken1"),
			},
			Environment: env,
		})

	loginStatus, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLoginStatus,
			Flags: LoginStatusFlags{
				Method:   pointerTo(true),
				Endpoint: pointerTo(true),
				Tenant:   pointerTo(true),
			},
			Environment: env,
		})

	parsedStdout, ok = loginStatus.(*AzCopyParsedLoginStatusStdout)
	a.AssertNow("must be AzCopyParsedLoginStatusStdout", Equal{}, ok, true)

	if !a.Dryrun() {
		_, ok := parsedStdout.status.Identities["testToken1"]
		a.Assert("token1 should be removed after logout", Equal{}, false, ok)

		identity2, ok := parsedStdout.status.Identities["testToken2"]
		a.AssertNow("token2 not returned after token1 logout", Equal{}, ok, true)
		a.Assert("token2 should remain valid", Equal{}, true, identity2.Valid)
	}

	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLogout,
			Flags: LogoutFlags{
				Nickname: PtrOf("testToken2"),
			},
			Environment: env,
		})

	loginStatus, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLoginStatus,
			Flags: LoginStatusFlags{
				Method:   pointerTo(true),
				Endpoint: pointerTo(true),
				Tenant:   pointerTo(true),
			},
			Environment: env,
		})

	parsedStdout, ok = loginStatus.(*AzCopyParsedLoginStatusStdout)
	a.AssertNow("must be AzCopyParsedLoginStatusStdout", Equal{}, ok, true)

	if !a.Dryrun() {
		fmt.Println(parsedStdout.status.Identities)
		a.Assert("expected no identities after all logouts", Empty{}, parsedStdout.status.Identities)
	}
}

func (*TokenPersistenceSuite) Scenario_DeviceCode_Persist(a *ScenarioVariationManager) {
	if runInteractiveTest == nil || !*runInteractiveTest {
		a.Skip("device code disabled")
	}

	env := &AzCopyEnvironment{
		LoginCacheName: pointerTo("AzCopyDeviceCodeTest"),
		ManualLogin:    true,
	}

	cfgTenantID := GlobalConfig.GetTenantID()
	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLogin,
			Flags: LoginFlags{
				LoginType: pointerTo(enum.EAutoLoginType.Device()),
				TenantID:  ternary.Iff(cfgTenantID != "", &cfgTenantID, nil),
				Nickname:  PtrOf(cred.DefaultNickname),
			},
			Environment: env,
		})

	loginStatus, _ := RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLoginStatus,
			Flags: LoginStatusFlags{
				Method:   pointerTo(true),
				Endpoint: pointerTo(true),
				Tenant:   pointerTo(true),
				Nickname: PtrOf(cred.DefaultNickname),
			},
			Environment: env,
		})

	parsedStdout, ok := loginStatus.(*AzCopyParsedLoginStatusStdout)
	a.AssertNow("must be AzCopyParsedLoginStatusStdout", Equal{}, ok, true)

	if !a.Dryrun() {
		identity, ok := parsedStdout.status.Identities[cred.DefaultNickname]
		a.AssertNow("default identity not returned", Equal{}, ok, true)
		a.Assert("Login check failed", Equal{}, true, identity.Valid)

		a.Assert("Tenant not returned", Empty{Invert: true}, identity.TenantID) // Let's just do a little extra testing while we're at it, kill two birds with one stone.
		if cfgTenantID != "" {
			a.Assert("Tenant does not match", Equal{}, cfgTenantID, identity.TenantID)
		}
		a.Assert("Endpoint not returned", Empty{Invert: true}, identity.AADEndpoint)
		a.Assert("Incorrect auth mechanism", Equal{}, identity.AuthMethod, enum.EAutoLoginType.Device().String())
	}

	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb: AzCopyVerbLogout,
			Flags: LogoutFlags{
				Nickname: PtrOf(cred.DefaultNickname),
			},
			Environment: env,
		})
}

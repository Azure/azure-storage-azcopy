package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

func init() {
	suiteManager.RegisterEarlyRunSuite(&TokenPersistenceSuite{})
}

type TokenPersistenceSuite struct{}

func (*TokenPersistenceSuite) Scenario_InheritCred_Persist(a *ScenarioVariationManager) {
	credSource := ResolveVariation(a, []common.AutoLoginType{common.EAutoLoginType.PsCred(), common.EAutoLoginType.AzCLI()})
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
				TenantID:  common.Iff(withSpecifiedTenantID && cfgTenantID != "", &cfgTenantID, nil),
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
			},
			Environment: azcopyEnv,
		})

	parsedStdout, ok := loginStatus.(*AzCopyParsedLoginStatusStdout)
	a.AssertNow("must be AzCopyParsedLoginStatusStdout", Equal{}, ok, true)

	if !a.Dryrun() {
		status := parsedStdout.status
		a.Assert("Login check failed", Equal{}, true, status.Valid)

		a.Assert("Tenant not returned", Not{IsNil{}}, status.TenantID) // Let's just do a little extra testing while we're at it, kill two birds with one stone.
		if withSpecifiedTenantID && status.TenantID != nil && cfgTenantID != "" {
			a.Assert("Tenant does not match", Equal{}, cfgTenantID, *status.TenantID)
		}
		a.Assert("Endpoint not returned", Not{IsNil{}}, status.AADEndpoint)
		a.Assert("Auth mechanism not returned", Not{IsNil{}}, status.AuthMethod)
		if status.AuthMethod != nil {
			a.Assert("Incorrect auth mechanism", Equal{}, *status.AuthMethod, credSource.String())
		}
	}

	_, _ = RunAzCopy(a,
		AzCopyCommand{
			Verb:        AzCopyVerbLogout,
			Environment: azcopyEnv,
		})
}

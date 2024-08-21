package e2etest

import (
	"os"
	"testing"
)

//var InitHooks = map[string]func() TieredError{
//	"ARM OAuth spinup": nil,
//	"Account Registry Init": AccountRegistryInitHook,
//}
//
//var CleanupHooks = map[string]func() TieredError{
//	"Account Registry Teardown": AccountRegistryCleanupHook,
//	"ARM Oauth spindown":
//}

var FrameworkHooks = []TestFrameworkHook{
	{HookName: "Config", SetupHook: LoadConfigHook},
	{HookName: "OAuth Cache", SetupHook: SetupOAuthCache},
	{HookName: "ARM Client", SetupHook: SetupArmClient, TeardownHook: TeardownArmClient},
	{HookName: "Default accts", SetupHook: AccountRegistryInitHook, TeardownHook: AccountRegistryCleanupHook},
}

type TestFrameworkHook struct {
	HookName     string
	SetupHook    func(a Asserter) // todo: Early hooks are a bit boilerplate heavy! Let's fix this.
	TeardownHook func(a Asserter)
	Ran          bool
}

func TestNewE2E(t *testing.T) {
	a := &FrameworkAsserter{t: t}

	os.Setenv("NEW_E2E_APPLICATION_ID", "6e664d29-7531-4a2f-8ec8-f04ac1f99b6c")
	os.Setenv("NEW_E2E_TENANT_ID", "72f988bf-86f1-41af-91ab-2d7cd011db47")
	os.Setenv("NEW_E2E_CLIENT_SECRET", "OMx8Q~1fP~X-UTgKGBn_j3vNBP~tRYaarYqwwa-.")
	os.Setenv("NEW_E2E_STATIC_APPLICATION_ID", "6e664d29-7531-4a2f-8ec8-f04ac1f99b6c")
	os.Setenv("NEW_E2E_STATIC_TENANT_ID", "72f988bf-86f1-41af-91ab-2d7cd011db47")
	os.Setenv("NEW_E2E_APPLICATION_ID", "6e664d29-7531-4a2f-8ec8-f04ac1f99b6c")
	os.Setenv("NEW_E2E_STATIC_CLIENT_SECRET", "OMx8Q~1fP~X-UTgKGBn_j3vNBP~tRYaarYqwwa-.")
	os.Setenv("AZCOPY_E2E_LOG_OUTPUT", "/home/azureuser")
	os.Setenv("NEW_E2E_AZCOPY_PATH", "/home/azureuser/go/src/azure-storage-azcopy/azcopy")
	os.Setenv("NEW_E2E_STANDARD_ACCOUNT_NAME", "azcopye2epipeline")
	os.Setenv("NEW_E2E_STANDARD_ACCOUNT_KEY", "U6IPcw4JXZQFmLAQ1GgBWLIcW6phz3VdJGwR1amXhSFkP8FqzKxFMaqnkW3K8/JamTllYGGYtknXVOT8jCKVAg==")
	os.Setenv("NEW_E2E_HNS_ACCOUNT_NAME", "azcopye2ehnstest1")
	os.Setenv("NEW_E2E_HNS_ACCOUNT_NAME", "azcopye2ehnstest1")
	os.Setenv("NEW_E2E_SUBSCRIPTION_ID", "")

	t.Cleanup(func() {
		for i := len(FrameworkHooks) - 1; i >= 0; i-- {
			hook := FrameworkHooks[i]
			if hook.Ran && hook.TeardownHook != nil {
				t.Logf("Teardown hook %s running", hook.HookName)
				hook.TeardownHook(a)
			}
		}
	})

	for i := 0; i < len(FrameworkHooks); i++ {
		hook := &FrameworkHooks[i]
		hookName := hook.HookName

		t.Logf("Setup hook %s running", hookName)
		hook.SetupHook(a)
		hook.Ran = true

		if t.Failed() {
			break
		}
	}

	if !t.Failed() {
		suiteManager.RunSuites(t)
	}
}

//go:build !olde2etest

package e2etest

import (
	"testing"
)

//var InitHooks = map[string]func() TieredError{
//	"ARM OAuth spinup": nil,
//	"Account Registry OnStart": AccountRegistryInitHook,
//}
//
//var CleanupHooks = map[string]func() TieredError{
//	"Account Registry Teardown": AccountRegistryCleanupHook,
//	"ARM Oauth spindown":
//}

var FrameworkHooks = []TestFrameworkHook{
	{HookName: "Config", SetupHook: LoadConfigHook},
	{HookName: "Workload Identity Setup", SetupHook: WorkloadIdentitySetup},
	{HookName: "OAuth Cache", SetupHook: SetupOAuthCache},
	{HookName: "ARM Client", SetupHook: SetupArmClient, TeardownHook: TeardownArmClient},
	{HookName: "Default accts", SetupHook: AccountRegistryInitHook, TeardownHook: AccountRegistryCleanupHook},
	{HookName: "Synthetic Test Suite Registration", SetupHook: RegisterSyntheticStressTestHook},
}

type TestFrameworkHook struct {
	HookName     string
	SetupHook    func(a Asserter) // todo: Early hooks are a bit boilerplate heavy! Let's fix this.
	TeardownHook func(a Asserter)
	Ran          bool
}

func TestNewE2E(t *testing.T) {
	a := &FrameworkAsserter{t: t}

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

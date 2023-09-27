package e2etest

import (
	"log"
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
	SetupHook    func() TieredError
	TeardownHook func() TieredError
	Ran          bool
}

func TestMain(m *testing.M) {
	setupSuccessful := true
	for i := 0; i < len(FrameworkHooks) && setupSuccessful; i++ {
		hook := FrameworkHooks[i]
		hookName := hook.HookName

		log.Println("Setting up hook", hookName)
		err := hook.SetupHook()

		if err != nil {
			switch err.Tier() {
			case ErrorTierInconsequential:
				log.Println("WARNING: failed to run initialization hook \"" + hookName + "\":\n" + err.Error())
			case ErrorTierFatal:
				log.Println("failed to run initialization hook \"" + hookName + "\":\n" + err.Error())
				setupSuccessful = false // Skip running if we can't
				break                   // don't finish setting up either
			}
		} else {
			hook.Ran = true // There's maybe no teardown if it didn't successfully run
		}
	}

	if setupSuccessful {
		m.Run()
	}

	for i := len(FrameworkHooks) - 1; i >= 0; i-- {
		hook := FrameworkHooks[i]
		if hook.Ran && hook.TeardownHook != nil {
			err := hook.TeardownHook()
			hookName := hook.HookName

			if err != nil {
				switch err.Tier() {
				case ErrorTierInconsequential:
					log.Println("WARNING: failed to run teardown hook \"" + hookName + "\":\n" + err.Error())
				case ErrorTierFatal: // Continue running closure hooks, despite being fatal, because other hooks could still clean up properly.
					log.Println("FATAL: failed to run teardown hook \"" + hookName + "\":\n" + err.Error())
				}
			}
		}
	}

	//for hookName, hook := range InitHooks {
	//	err := hook()
	//}
	//
	//m.Run()
	//
	//for hookName, hook := range CleanupHooks {
	//	err := hook()
	//	if err != nil {
	//		switch err.Tier() {
	//		case ErrorTierInconsequential:
	//			log.Println("WARNING: failed to run initialization hook " + hookName + ": " + err.Error())
	//		case ErrorTierFatal:
	//			log.Fatalln("failed to run initialization hook " + hookName + ": " + err.Error())
	//		}
	//	}
	//}
}

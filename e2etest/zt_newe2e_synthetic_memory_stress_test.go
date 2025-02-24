package e2etest

type SyntheticMemoryStressTestSuite struct{}

/*
This test suite requires three things

1) The telemetry account is configured
2) Stress test data is generated and present in the expected containers (run the generators in stress_generators)
3) Stress testing is enabled-- this is not enabled for every run, and should be flipped on for release PRs.

Currently, this suite (and it's scenarios) are designed to target highly memory intensive operations,
(e.g. massive sync cases, many folder properties transfers, etc.)
as well as some common intensive operations (e.g. millions of files, several gigantic files, etc.) in S2S.


*/

func RegisterSyntheticStressTestHook(a Asserter) {
	// Why a hook instead of init? We want to check that conditions are set, which is reliant upon the config.
	if !GlobalConfig.TelemetryConfigured() {
		return // don't register if we don't have the account set up
	}

	if !GlobalConfig.TelemetryConfig.StressTestEnabled {
		return // don't register if we haven't enabled the stress tests.
	}

	// todo: validate stress test data is present

	suiteManager.RegisterSuite(&SyntheticMemoryStressTestSuite{})
}

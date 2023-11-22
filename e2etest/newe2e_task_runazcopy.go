package e2etest

import (
	"bytes"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"os/exec"
	"reflect"
)

// AzCopyJobPlan todo probably load the job plan directly?
type AzCopyJobPlan struct{}

type AzCopyVerb string

const ( // initially supporting a limited set of verbs
	AzCopyVerbCopy   AzCopyVerb = "copy"
	AzCopyVerbSync   AzCopyVerb = "sync"
	AzCopyVerbRemove AzCopyVerb = "remove"
)

type AzCopyCommand struct {
	Verb AzCopyVerb
	// Instead of directly taking resource managers or any extensions thereof, we'll take strings.
	// This is theoretically the most versatile option, allowing us to specify SAS and otherwise.
	// It is less convenient than magically wrangling auth type, but after some debate I (Adele)
	// came to the conclusion that there was no "good" solution to that problem.
	Targets     []string
	Flags       any // check SampleFlags
	Environment AzCopyEnvironment

	ShouldFail bool
}

type AzCopyEnvironment struct {
	// `env:"XYZ"` is re-used but does not inherit the traits of config's env trait. Merely used for low-code mapping.

	LogLocation                  *string `env:"AZCOPY_LOG_LOCATION"`
	JobPlanLocation              *string `env:"AZCOPY_JOB_PLAN_LOCATION"`
	ServicePrincipalAppID        *string `env:"AZCOPY_SPA_APPLICATION_ID"`
	ServicePrincipalClientSecret *string `env:"AZCOPY_SPA_CLIENT_SECRET"`

	InheritEnvironment bool
}

// RunAzCopy todo define more cleanly, implement
func RunAzCopy(a Asserter, commandSpec AzCopyCommand) *AzCopyJobPlan {
	if dryrunner, ok := a.(DryrunAsserter); ok && dryrunner.Dryrun() {
		return &AzCopyJobPlan{}
	}

	env := MapFromTags(reflect.ValueOf(commandSpec.Environment), "env")

	out := &bytes.Buffer{}
	command := exec.Cmd{
		Path: GlobalConfig.AzCopyExecutableConfig.ExecutablePath,
		Args: func() []string {
			out := []string{GlobalConfig.AzCopyExecutableConfig.ExecutablePath, string(commandSpec.Verb)}

			for _, v := range commandSpec.Targets {
				out = append(out, v) // todo
			}

			if commandSpec.Flags != nil {
				flags := MapFromTags(reflect.ValueOf(commandSpec.Flags), "flag")
				for k, v := range flags {
					out = append(out, fmt.Sprintf("--%s=%s", k, v))
				}
			}

			return out
		}(),
		Env: func() []string {
			out := make([]string, 0)

			for k, v := range env {
				out = append(out, fmt.Sprintf("%s=%s", k, v))
			}

			if commandSpec.Environment.InheritEnvironment {
				out = append(out, os.Environ()...)
			}

			return nil
		}(),

		Stdout: out, // todo
		Stdin:  nil, // todo
	}

	err := command.Run()
	a.Log(out.String())
	a.NoError("run command", err)
	a.AssertNow("expected exit code",
		common.Iff[Assertion](commandSpec.ShouldFail, Not{Equal{}}, Equal{}),
		0, command.ProcessState.ExitCode())

	return &AzCopyJobPlan{}
}

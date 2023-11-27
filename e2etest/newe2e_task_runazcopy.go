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

type AzCopyTarget struct {
	ResourceManager
	AuthType ExplicitCredentialTypes // Expects *one* credential type that the Resource supports. Assumes SAS (or GCP/S3) if not present.

	// todo: SAS permissions
	// todo: specific OAuth types (e.g. MSI, etc.)
}

type AzCopyCommand struct {
	Verb AzCopyVerb
	// Passing a ResourceManager assumes SAS (or GCP/S3) auth is intended.
	// Passing an AzCopyTarget will allow you to specify an exact credential type.
	// When OAuth, S3, GCP, AcctKey, etc. the appropriate env flags should auto-populate.
	Targets     []ResourceManager
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

func (c *AzCopyCommand) applyTargetAuth(a Asserter, target ResourceManager) string {
	intendedAuthType := EExplicitCredentialType.SASToken()
	if tgt, ok := target.(AzCopyTarget); ok {
		count := tgt.AuthType.Count()
		a.AssertNow("target auth type must be single", Equal{}, count <= 1, true)
		if count == 1 {
			intendedAuthType = tgt.AuthType
		}
	} else if target.Location() == common.ELocation.S3() {
		intendedAuthType = EExplicitCredentialType.S3()
	} else if target.Location() == common.ELocation.GCP() {
		intendedAuthType = EExplicitCredentialType.GCP()
	}

	switch intendedAuthType {
	case EExplicitCredentialType.PublicAuth(), EExplicitCredentialType.None():
		return target.URI(a, false)
	case EExplicitCredentialType.SASToken():
		return target.URI(a, true)
	case EExplicitCredentialType.OAuth():
		c.Environment.ServicePrincipalAppID = &GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.ApplicationID
		c.Environment.ServicePrincipalClientSecret = &GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.ClientSecret
		return target.URI(a, false)
	default:
		a.Error("unsupported credential type")
		return target.URI(a, true)
	}
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
				out = append(out, commandSpec.applyTargetAuth(a, v))
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

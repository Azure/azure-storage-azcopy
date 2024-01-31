package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"os"
	"os/exec"
	"reflect"
	"strings"
)

// AzCopyJobPlan todo probably load the job plan directly? WI#26418256
type AzCopyJobPlan struct{}

// AzCopyStdout shouldn't be used or relied upon right now! This will be fleshed out eventually. todo WI#26418258
type AzCopyStdout struct {
	RawOutput []string
}

func (a *AzCopyStdout) Write(p []byte) (n int, err error) {
	str := string(p)
	lines := strings.Split(str, "\n")

	a.RawOutput = append(a.RawOutput, lines...)

	return len(p), nil
}

func (a *AzCopyStdout) String() string {
	return strings.Join(a.RawOutput, "\n")
}

type AzCopyVerb string

const ( // initially supporting a limited set of verbs
	AzCopyVerbCopy   AzCopyVerb = "copy"
	AzCopyVerbSync   AzCopyVerb = "sync"
	AzCopyVerbRemove AzCopyVerb = "remove"
)

type AzCopyTarget struct {
	RemoteResourceManager
	AuthType ExplicitCredentialTypes // Expects *one* credential type that the Resource supports. Assumes SAS (or GCP/S3) if not present.

	// todo: SAS permissions
	// todo: specific OAuth types (e.g. MSI, etc.)
}

func CreateAzCopyTarget(rm RemoteResourceManager, authType ExplicitCredentialTypes, a Asserter) AzCopyTarget {
	validTypes := rm.ValidAuthTypes()

	a.AssertNow(fmt.Sprintf("expected only one auth type, got %s", authType), Equal{}, authType.Count(), 1)
	a.AssertNow(fmt.Sprintf("expected authType to be contained within valid types (got %s, needed %s)", authType, validTypes), Equal{}, validTypes.Includes(authType), true)

	return AzCopyTarget{rm, authType}
}

type AzCopyCommand struct {
	Verb AzCopyVerb
	// Passing a ResourceManager assumes SAS (or GCP/S3) auth is intended.
	// Passing an AzCopyTarget will allow you to specify an exact credential type.
	// When OAuth, S3, GCP, AcctKey, etc. the appropriate env flags should auto-populate.
	Targets     []ResourceManager
	Flags       any // check SampleFlags
	Environment *AzCopyEnvironment

	ShouldFail bool
}

type AzCopyEnvironment struct {
	// `env:"XYZ"` is re-used but does not inherit the traits of config's env trait. Merely used for low-code mapping.

	LogLocation                  *string `env:"AZCOPY_LOG_LOCATION,defaultfunc:DefaultLogLoc"`
	JobPlanLocation              *string `env:"AZCOPY_JOB_PLAN_LOCATION,defaultfunc:DefaultPlanLoc"`
	AutoLoginMode                *string `env:"AZCOPY_AUTO_LOGIN_TYPE"`
	AutoLoginTenantID            *string `env:"AZCOPY_TENANT_ID"`
	ServicePrincipalAppID        *string `env:"AZCOPY_SPA_APPLICATION_ID"`
	ServicePrincipalClientSecret *string `env:"AZCOPY_SPA_CLIENT_SECRET"`

	InheritEnvironment bool
}

func (env *AzCopyEnvironment) generateAzcopyDir(a ScenarioAsserter) {
	dir, err := os.MkdirTemp("", "azcopytests*")
	a.NoError("create tempdir", err)
	env.LogLocation = &dir
	env.JobPlanLocation = &dir
	a.Cleanup(func(a ScenarioAsserter) {
		err := os.RemoveAll(dir)
		a.NoError("remove tempdir", err)
	})
}

func (env *AzCopyEnvironment) DefaultLogLoc(a ScenarioAsserter) string {
	if env.JobPlanLocation != nil {
		env.LogLocation = env.JobPlanLocation
	} else if env.LogLocation == nil {
		env.generateAzcopyDir(a)
	}

	return *env.LogLocation
}

func (env *AzCopyEnvironment) DefaultPlanLoc(a ScenarioAsserter) string {
	if env.LogLocation != nil {
		env.JobPlanLocation = env.LogLocation
	} else if env.JobPlanLocation == nil {
		env.generateAzcopyDir(a)
	}

	return *env.JobPlanLocation
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
		// Only set it if it wasn't already configured. If it was manually configured,
		// special testing may be occurring, and this may be indicated to just get a SAS-less URI.
		// Alternatively, we may have already configured it here once before.
		if c.Environment.AutoLoginMode == nil && c.Environment.ServicePrincipalAppID == nil && c.Environment.ServicePrincipalClientSecret == nil && c.Environment.AutoLoginTenantID == nil {
			c.Environment.AutoLoginMode = pointerTo("SPN") // TODO! There are two other modes for this. These probably can't apply in automated scenarios, but it's worth having tests for that we run before every release! WI#26625161

			if GlobalConfig.StaticResources() {
				oAuthInfo := GlobalConfig.E2EAuthConfig.StaticStgAcctInfo.StaticOAuth
				a.AssertNow("At least NEW_E2E_STATIC_APPLICATION_ID and NEW_E2E_STATIC_CLIENT_SECRET must be specified to use OAuth.", Empty{true}, oAuthInfo.ApplicationID, oAuthInfo.ClientSecret)

				c.Environment.ServicePrincipalAppID = &oAuthInfo.ApplicationID
				c.Environment.ServicePrincipalClientSecret = &oAuthInfo.ClientSecret
				c.Environment.AutoLoginTenantID = common.Iff(oAuthInfo.TenantID != "", &oAuthInfo.TenantID, nil)
			} else {
				// oauth should reliably work
				oAuthInfo := GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo
				c.Environment.ServicePrincipalAppID = &oAuthInfo.ApplicationID
				c.Environment.ServicePrincipalClientSecret = &oAuthInfo.ClientSecret
				c.Environment.AutoLoginTenantID = common.Iff(oAuthInfo.TenantID != "", &oAuthInfo.TenantID, nil)
			}
		}

		return target.URI(a, false)
	default:
		a.Error("unsupported credential type")
		return target.URI(a, true)
	}
}

// RunAzCopy todo define more cleanly, implement
func RunAzCopy(a ScenarioAsserter, commandSpec AzCopyCommand) (*AzCopyStdout, *AzCopyJobPlan) {
	if a.Dryrun() {
		return nil, &AzCopyJobPlan{}
	}

	// separate these from the struct so their execution order is fixed
	args := func() []string {
		if commandSpec.Environment == nil {
			commandSpec.Environment = &AzCopyEnvironment{}
		}

		out := []string{GlobalConfig.AzCopyExecutableConfig.ExecutablePath, string(commandSpec.Verb)}

		for _, v := range commandSpec.Targets {
			out = append(out, commandSpec.applyTargetAuth(a, v))
		}

		if commandSpec.Flags != nil {
			flags := MapFromTags(reflect.ValueOf(commandSpec.Flags), "flag", a)
			for k, v := range flags {
				out = append(out, fmt.Sprintf("--%s=%s", k, v))
			}
		}

		return out
	}()
	env := func() []string {
		out := make([]string, 0)

		env := MapFromTags(reflect.ValueOf(commandSpec.Environment), "env", a)

		for k, v := range env {
			out = append(out, fmt.Sprintf("%s=%s", k, v))
		}

		if commandSpec.Environment.InheritEnvironment {
			out = append(out, os.Environ()...)
		}

		return out
	}()

	out := &AzCopyStdout{}
	command := exec.Cmd{
		Path: GlobalConfig.AzCopyExecutableConfig.ExecutablePath,
		Args: args,
		Env:  env,

		Stdout: out, // todo
		Stdin:  nil, // todo
	}

	err := command.Run()
	a.Assert("run command", IsNil{}, err)
	a.Assert("expected exit code",
		common.Iff[Assertion](commandSpec.ShouldFail, Not{Equal{}}, Equal{}),
		0, command.ProcessState.ExitCode())

	if err != nil {
		a.Log("AzCopy output:\n%s", out.String())
	}

	return out, &AzCopyJobPlan{}
}

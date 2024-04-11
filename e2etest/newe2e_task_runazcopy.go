package e2etest

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/cmd"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"os"
	"os/exec"
	"reflect"
	"strings"
)

// AzCopyJobPlan todo probably load the job plan directly? WI#26418256
type AzCopyJobPlan struct{}

type AzCopyStdout interface {
	RawStdout() []string

	io.Writer
	fmt.Stringer
}

// AzCopyRawStdout shouldn't be used or relied upon right now! This will be fleshed out eventually. todo WI#26418258
type AzCopyRawStdout struct {
	RawOutput []string
}

func (a *AzCopyRawStdout) RawStdout() []string {
	return a.RawOutput
}

func (a *AzCopyRawStdout) Write(p []byte) (n int, err error) {
	str := string(p)
	lines := strings.Split(str, "\n")

	a.RawOutput = append(a.RawOutput, lines...)

	return len(p), nil
}

func (a *AzCopyRawStdout) String() string {
	return strings.Join(a.RawOutput, "\n")
}

var _ AzCopyStdout = &AzCopyRawStdout{}
var _ AzCopyStdout = &AzCopyListStdout{}

type AzCopyListStdout struct {
	AzCopyRawStdout
}

func (a *AzCopyListStdout) RawStdout() []string {
	return a.AzCopyRawStdout.RawStdout()
}

func (a *AzCopyListStdout) Write(p []byte) (n int, err error) {
	return a.AzCopyRawStdout.Write(p)
}

func (a *AzCopyListStdout) String() string {
	return a.AzCopyRawStdout.String()
}

func (a *AzCopyListStdout) Unmarshal() ([]cmd.AzCopyListObject, *cmd.AzCopyListSummary, error) {
	var listOutput []cmd.AzCopyListObject
	var listSummary *cmd.AzCopyListSummary
	for _, line := range a.RawOutput {
		if line == "" {
			continue
		}
		var out common.JsonOutputTemplate
		err := json.Unmarshal([]byte(line), &out)
		if err != nil {
			return nil, nil, err
		}
		if out.MessageType == common.EOutputMessageType.ListObject().String() {
			var obj *cmd.AzCopyListObject
			objErr := json.Unmarshal([]byte(out.MessageContent), &obj)
			if objErr != nil {
				return nil, nil, fmt.Errorf("error unmarshaling list output; object error: %s", objErr)
			}
			listOutput = append(listOutput, *obj)

		} else if out.MessageType == common.EOutputMessageType.ListSummary().String() {
			var sum *cmd.AzCopyListSummary
			sumErr := json.Unmarshal([]byte(out.MessageContent), &sum)
			if sumErr != nil {
				return nil, nil, fmt.Errorf("error unmarshaling list output; summary error: %s", sumErr)
			}
			listSummary = sum
		}
	}
	return listOutput, listSummary, nil
}

type AzCopyVerb string

const ( // initially supporting a limited set of verbs
	AzCopyVerbCopy   AzCopyVerb = "copy"
	AzCopyVerbSync   AzCopyVerb = "sync"
	AzCopyVerbRemove AzCopyVerb = "remove"
	AzCopyVerbList   AzCopyVerb = "list"
)

type AzCopyTarget struct {
	ResourceManager
	AuthType ExplicitCredentialTypes // Expects *one* credential type that the Resource supports. Assumes SAS (or GCP/S3) if not present.
	Opts     CreateAzCopyTargetOptions

	// todo: SAS permissions
	// todo: specific OAuth types (e.g. MSI, etc.)
}

type CreateAzCopyTargetOptions struct {
	// SASTokenOptions expects a GenericSignatureValues, which can contain account signatures, or a service signature.
	SASTokenOptions GenericSignatureValues
	Scheme          string
}

func CreateAzCopyTarget(rm ResourceManager, authType ExplicitCredentialTypes, a Asserter, opts ...CreateAzCopyTargetOptions) AzCopyTarget {
	var validTypes ExplicitCredentialTypes
	if rrm, ok := rm.(RemoteResourceManager); ok {
		validTypes = rrm.ValidAuthTypes()
	}

	if validTypes != EExplicitCredentialType.None() {
		a.AssertNow(fmt.Sprintf("expected only one auth type, got %s", authType), Equal{}, authType.Count(), 1)
		a.AssertNow(fmt.Sprintf("expected authType to be contained within valid types (got %s, needed %s)", authType, validTypes), Equal{}, validTypes.Includes(authType), true)
	} else {
		a.AssertNow("Expected no auth types", Equal{}, authType, EExplicitCredentialType.None())
	}

	return AzCopyTarget{rm, authType, FirstOrZero(opts)}
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
	var opts GetURIOptions
	if tgt, ok := target.(AzCopyTarget); ok {
		count := tgt.AuthType.Count()
		a.AssertNow("target auth type must be single", Equal{}, count <= 1, true)
		if count == 1 {
			intendedAuthType = tgt.AuthType
		}

		opts.AzureOpts.SASValues = tgt.Opts.SASTokenOptions
		opts.RemoteOpts.Scheme = tgt.Opts.Scheme
	} else if target.Location() == common.ELocation.S3() {
		intendedAuthType = EExplicitCredentialType.S3()
	} else if target.Location() == common.ELocation.GCP() {
		intendedAuthType = EExplicitCredentialType.GCP()
	}

	switch intendedAuthType {
	case EExplicitCredentialType.PublicAuth(), EExplicitCredentialType.None():
		return target.URI() // no SAS, no nothing.
	case EExplicitCredentialType.SASToken():
		opts.AzureOpts.WithSAS = true
		return target.URI(opts)
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

		return target.URI() // Generate like public
	default:
		a.Error("unsupported credential type")
		return target.URI()
	}
}

// RunAzCopy todo define more cleanly, implement
func RunAzCopy(a ScenarioAsserter, commandSpec AzCopyCommand) (AzCopyStdout, *AzCopyJobPlan) {
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

	var out AzCopyStdout
	switch commandSpec.Verb {
	case AzCopyVerbList:
		out = &AzCopyListStdout{}
	default:
		out = &AzCopyRawStdout{}
	}
	command := exec.Cmd{
		Path: GlobalConfig.AzCopyExecutableConfig.ExecutablePath,
		Args: args,
		Env:  env,

		Stdout: out, // todo
	}
	in, err := command.StdinPipe()
	a.NoError("get stdin pipe", err)

	err = command.Start()
	a.Assert("run command", IsNil{}, err)

	if isLaunchedByDebugger {
		beginAzCopyDebugging(in)
	}

	err = command.Wait()
	a.Assert("wait for finalize", common.Iff[Assertion](commandSpec.ShouldFail, Not{IsNil{}}, IsNil{}), err)
	a.Assert("expected exit code",
		common.Iff[Assertion](commandSpec.ShouldFail, Not{Equal{}}, Equal{}),
		0, command.ProcessState.ExitCode())

	if err != nil {
		a.Log("AzCopy output:\n%s", out.String())
	}

	return out, &AzCopyJobPlan{}
}

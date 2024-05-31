package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
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

	// If Stdout is nil, a sensible default is picked in place.
	Stdout AzCopyStdout

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

	AzureFederatedTokenFile *string `env:"AZURE_FEDERATED_TOKEN_FILE"`
	AzureTenantId           *string `env:"AZURE_TENANT_ID"`
	AzureClientId           *string `env:"AZURE_CLIENT_ID"`

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
			if GlobalConfig.StaticResources() {
				c.Environment.AutoLoginMode = pointerTo("SPN")
				oAuthInfo := GlobalConfig.E2EAuthConfig.StaticStgAcctInfo.StaticOAuth
				a.AssertNow("At least NEW_E2E_STATIC_APPLICATION_ID and NEW_E2E_STATIC_CLIENT_SECRET must be specified to use OAuth.", Empty{true}, oAuthInfo.ApplicationID, oAuthInfo.ClientSecret)

				c.Environment.ServicePrincipalAppID = &oAuthInfo.ApplicationID
				c.Environment.ServicePrincipalClientSecret = &oAuthInfo.ClientSecret
				c.Environment.AutoLoginTenantID = common.Iff(oAuthInfo.TenantID != "", &oAuthInfo.TenantID, nil)
			} else {
				// oauth should reliably work
				oAuthInfo := GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo
				if oAuthInfo.Environment == AzurePipeline {
					c.Environment.InheritEnvironment = true
					c.Environment.AutoLoginTenantID = common.Iff(oAuthInfo.DynamicOAuth.Workload.TenantId != "", &oAuthInfo.DynamicOAuth.Workload.TenantId, nil)
					c.Environment.AutoLoginMode = pointerTo(common.EAutoLoginType.AzCLI().String())
				} else {
					c.Environment.AutoLoginMode = pointerTo(common.EAutoLoginType.SPN().String())
					c.Environment.ServicePrincipalAppID = &oAuthInfo.DynamicOAuth.SPNSecret.ApplicationID
					c.Environment.ServicePrincipalClientSecret = &oAuthInfo.DynamicOAuth.SPNSecret.ClientSecret
					c.Environment.AutoLoginTenantID = common.Iff(oAuthInfo.DynamicOAuth.SPNSecret.TenantID != "", &oAuthInfo.DynamicOAuth.SPNSecret.TenantID, nil)
				}
			}
		} else if c.Environment.AutoLoginMode != nil {
			oAuthInfo := GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo
			if strings.ToLower(*c.Environment.AutoLoginMode) == common.EAutoLoginType.Workload().String() {
				c.Environment.InheritEnvironment = true
				// Get the value of the AZURE_FEDERATED_TOKEN environment variable
				token := oAuthInfo.DynamicOAuth.Workload.FederatedToken
				a.AssertNow("idToken must be specified to authenticate with workload identity", Empty{Invert: true}, token)
				// Write the token to a temporary file
				// Create a temporary file to store the token
				file, err := os.CreateTemp("", "azure_federated_token.txt")
				a.AssertNow("Error creating temporary file", IsNil{}, err)
				defer file.Close()

				// Write the token to the temporary file
				_, err = file.WriteString(token)
				a.AssertNow("Error writing to temporary file", IsNil{}, err)

				// Set the AZURE_FEDERATED_TOKEN_FILE environment variable
				c.Environment.AzureFederatedTokenFile = pointerTo(file.Name())
				c.Environment.AzureTenantId = pointerTo(oAuthInfo.DynamicOAuth.Workload.TenantId)
				c.Environment.AzureClientId = pointerTo(oAuthInfo.DynamicOAuth.Workload.ClientId)
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
	var flagMap map[string]string
	var envMap map[string]string

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
			flagMap = MapFromTags(reflect.ValueOf(commandSpec.Flags), "flag", a)
			for k, v := range flagMap {
				out = append(out, fmt.Sprintf("--%s=%s", k, v))
			}
		}

		return out
	}()
	env := func() []string {
		out := make([]string, 0)

		envMap = MapFromTags(reflect.ValueOf(commandSpec.Environment), "env", a)

		for k, v := range envMap {
			out = append(out, fmt.Sprintf("%s=%s", k, v))
		}

		if commandSpec.Environment.InheritEnvironment {
			out = append(out, os.Environ()...)
		}

		return out
	}()

	var out = commandSpec.Stdout
	if out == nil {
		switch {
		case !strings.EqualFold(flagMap["output-type"], "json"): // Won't parse non-computer-readable outputs
			out = &AzCopyRawStdout{}
		case strings.EqualFold(flagMap["dryrun"], "true"): //  Dryrun has its own special sort of output
			out = &AzCopyParsedDryrunStdout{}
		case commandSpec.Verb == AzCopyVerbCopy || commandSpec.Verb == AzCopyVerbSync || commandSpec.Verb == AzCopyVerbRemove:
			out = &AzCopyParsedCopySyncRemoveStdout{}
		case commandSpec.Verb == AzCopyVerbList:
			out = &AzCopyParsedListStdout{}
		default: // We don't know how to parse this.
			out = &AzCopyRawStdout{}
		}
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

	a.Cleanup(func(a ScenarioAsserter) {
		if stdout, ok := out.(*AzCopyParsedCopySyncRemoveStdout); ok {
			UploadLogs(a, stdout, DerefOrZero(commandSpec.Environment.LogLocation))
			_ = os.RemoveAll(DerefOrZero(commandSpec.Environment.LogLocation))
		}
	})

	return out, &AzCopyJobPlan{}
}

func UploadLogs(a ScenarioAsserter, stdout *AzCopyParsedCopySyncRemoveStdout, logDir string) {
	logPath := GlobalConfig.AzCopyExecutableConfig.LogDropPath
	if logPath == "" || !a.Failed() {
		return
	}

	// sometimes, the log dir cannot be copied because the destination is on another drive. So, we'll copy the files instead by hand.
	files, err := os.ReadDir(logDir)
	a.NoError("Failed to read log dir", err)
	jobId := ""
	if stdout.InitMsg.JobID != "" {
		jobId = stdout.InitMsg.JobID
	} else {
		for _, file := range files { // first, find the job ID
			if strings.HasSuffix(file.Name(), ".log") {
				jobId = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(file.Name(), "-chunks"), "-scanning"), ".log")
				break
			}
		}
	}

	// Create the destination log directory
	destLogDir := filepath.Join(logPath, jobId)
	err = os.MkdirAll(destLogDir, os.ModePerm|os.ModeDir)
	a.NoError("Failed to create log dir", err)

	// Copy the files by hand
	err = filepath.WalkDir(logDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relPath := strings.TrimPrefix(path, logDir)
		if d.IsDir() {
			err = os.MkdirAll(filepath.Join(destLogDir, relPath), os.ModePerm|os.ModeDir)
			return err
		}

		// copy the file
		srcFile, err := os.Open(path)
		if err != nil {
			return err
		}

		destFile, err := os.Create(filepath.Join(destLogDir, relPath))
		if err != nil {
			return err
		}

		defer srcFile.Close()
		defer destFile.Close()

		_, err = io.Copy(destFile, srcFile)
		if err != nil {
			return err
		}

		return err
	})
	a.NoError("Failed to copy log files", err)

	a.Log("Uploaded failed run logs for job %s", jobId)
}

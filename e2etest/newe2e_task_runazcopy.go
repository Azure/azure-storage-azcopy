package e2etest

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/google/uuid"
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
	AzCopyVerbCopy        AzCopyVerb = "copy"
	AzCopyVerbSync        AzCopyVerb = "sync"
	AzCopyVerbRemove      AzCopyVerb = "remove"
	AzCopyVerbList        AzCopyVerb = "list"
	AzCopyVerbLogin       AzCopyVerb = "login"
	AzCopyVerbLoginStatus AzCopyVerb = "login status"
	AzCopyVerbLogout      AzCopyVerb = "logout"
	AzCopyVerbJobsList    AzCopyVerb = "jobs list"
	AzCopyVerbJobsResume  AzCopyVerb = "jobs resume"
	AzCopyVerbJobsClean   AzCopyVerb = "jobs clean"
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
	// The wildcard string to append to the end of a resource URI.
	Wildcard string
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
	Verb           AzCopyVerb
	PositionalArgs []string
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
	// `env:"XYZ"` is reused but does not inherit the traits of config's env trait. Merely used for low-code mapping.

	LogLocation                  *string `env:"AZCOPY_LOG_LOCATION,defaultfunc:DefaultLogLoc"`
	JobPlanLocation              *string `env:"AZCOPY_JOB_PLAN_LOCATION,defaultfunc:DefaultPlanLoc"`
	AutoLoginMode                *string `env:"AZCOPY_AUTO_LOGIN_TYPE"`
	AutoLoginTenantID            *string `env:"AZCOPY_TENANT_ID"`
	ServicePrincipalAppID        *string `env:"AZCOPY_SPA_APPLICATION_ID"`
	ServicePrincipalClientSecret *string `env:"AZCOPY_SPA_CLIENT_SECRET"`

	AzureFederatedTokenFile *string `env:"AZURE_FEDERATED_TOKEN_FILE"`
	AzureTenantId           *string `env:"AZURE_TENANT_ID"`
	AzureClientId           *string `env:"AZURE_CLIENT_ID"`

	LoginCacheName *string `env:"AZCOPY_LOGIN_CACHE_NAME"`

	// InheritEnvironment is a lowercase list of environment variables to always inherit.
	// Specifying "*" as an entry with the value "true" will act as a wildcard, and inherit all env vars.
	InheritEnvironment map[string]bool `env:",defaultfunc:DefaultInheritEnvironment"`

	ManualLogin bool

	// If this is set, the logs have already been persisted.
	// These should never be set by a test writer.
	SessionId    *string
	LogUploadDir *string
	RunCount     *uint
}

func (env *AzCopyEnvironment) InheritEnvVar(name string) {
	env.EnsureInheritEnvironment()
	env.InheritEnvironment[strings.ToLower(name)] = true
}

func (env *AzCopyEnvironment) EnsureInheritEnvironment() {
	if env.InheritEnvironment == nil {
		env.DefaultInheritEnvironment(nil)
	}
}

func (env *AzCopyEnvironment) DefaultInheritEnvironment(a ScenarioAsserter) map[string]bool {
	env.InheritEnvironment = map[string]bool{
		"path": true,
	}

	return env.InheritEnvironment
}

func (env *AzCopyEnvironment) generateAzcopyDir(a ScenarioAsserter) {
	dir, err := os.MkdirTemp("", "azcopytests*")
	a.NoError("create tempdir", err)
	env.LogLocation = &dir
	env.JobPlanLocation = &dir
	a.Cleanup(func(a Asserter) {
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
		opts.Wildcard = tgt.Opts.Wildcard
	} else if target.Location() == common.ELocation.S3() {
		intendedAuthType = EExplicitCredentialType.S3()
	} else if target.Location() == common.ELocation.GCP() {
		intendedAuthType = EExplicitCredentialType.GCP()
	}

	switch intendedAuthType {
	case EExplicitCredentialType.PublicAuth(), EExplicitCredentialType.None():
		return target.URI(opts) // no SAS, no nothing.
	case EExplicitCredentialType.SASToken():
		opts.AzureOpts.WithSAS = true
		return target.URI(opts)
	case EExplicitCredentialType.OAuth():
		// Only set it if it wasn't already configured. If it was manually configured,
		// special testing may be occurring, and this may be indicated to just get a SAS-less URI.
		// Alternatively, we may have already configured it here once before.
		if !c.Environment.ManualLogin {

			if c.Environment.AutoLoginMode == nil && c.Environment.ServicePrincipalAppID == nil && c.Environment.ServicePrincipalClientSecret == nil && c.Environment.AutoLoginTenantID == nil {
				if GlobalConfig.StaticResources() {
					staticOauth := GlobalConfig.E2EAuthConfig.StaticStgAcctInfo.StaticOAuth
					tenant := staticOauth.TenantID
					if useSPN, _, appId, secret := GlobalConfig.GetSPNOptions(); useSPN {
						c.Environment.AutoLoginMode = pointerTo("SPN")
						a.AssertNow("At least NEW_E2E_STATIC_APPLICATION_ID and NEW_E2E_STATIC_CLIENT_SECRET must be specified to use OAuth.", Empty{true}, appId, secret)

						c.Environment.ServicePrincipalAppID = &appId
						c.Environment.ServicePrincipalClientSecret = &secret
						c.Environment.AutoLoginTenantID = common.Iff(tenant != "", &tenant, nil)
					} else if staticOauth.OAuthSource.PSInherit {
						c.Environment.AutoLoginMode = pointerTo("pscred")
						c.Environment.AutoLoginTenantID = common.Iff(tenant != "", &tenant, nil)
						c.Environment.InheritEnvVar("home")
						c.Environment.InheritEnvVar("USERPROFILE")
						c.Environment.InheritEnvVar("HOMEPATH")
						c.Environment.InheritEnvVar("HOMEDRIVE")
						c.Environment.InheritEnvVar("AZURE_CONFIG_DIR")
					} else if staticOauth.OAuthSource.CLIInherit {
						c.Environment.AutoLoginMode = pointerTo("azcli")
						c.Environment.AutoLoginTenantID = common.Iff(tenant != "", &tenant, nil)
						c.Environment.InheritEnvVar("home")
						c.Environment.InheritEnvVar("USERPROFILE")
						c.Environment.InheritEnvVar("HOMEPATH")
						c.Environment.InheritEnvVar("HOMEDRIVE")
						c.Environment.InheritEnvVar("AZURE_CONFIG_DIR")
					}
				} else {
					// oauth should reliably work
					oAuthInfo := GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo
					if oAuthInfo.Environment == AzurePipeline {
						// No need to force keep path, we already inherit that.
						c.Environment.InheritEnvVar("home")
						c.Environment.InheritEnvVar("USERPROFILE")
						c.Environment.InheritEnvVar("HOMEPATH")
						c.Environment.InheritEnvVar("HOMEDRIVE")
						c.Environment.InheritEnvVar("AZURE_CONFIG_DIR")
						c.Environment.InheritEnvVar(WorkloadIdentityToken)
						c.Environment.InheritEnvVar(WorkloadIdentityServicePrincipalID)
						c.Environment.InheritEnvVar(WorkloadIdentityTenantID)

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
				} else if mode == common.EAutoLoginType.SPN().String() || mode == common.EAutoLoginType.MSI().String() {
					c.Environment.InheritEnvironment = true
				}

			}
		}
		return target.URI(opts) // Generate like public
	default:
		a.Error("unsupported credential type")
		return target.URI(opts)
	}
}

// RunAzCopy todo define more cleanly, implement
func RunAzCopy(a ScenarioAsserter, commandSpec AzCopyCommand) (AzCopyStdout, *AzCopyJobPlan) {
	if a.Dryrun() {
		return nil, &AzCopyJobPlan{}
	}
	a.HelperMarker().Helper()
	var flagMap map[string]string
	var envMap map[string]string

	// separate these from the struct so their execution order is fixed
	args := func() []string {
		if commandSpec.Environment == nil {
			commandSpec.Environment = &AzCopyEnvironment{}
		}

		out := []string{GlobalConfig.AzCopyExecutableConfig.ExecutablePath}
		out = append(out, strings.Split(string(commandSpec.Verb), " ")...)

		for _, v := range commandSpec.PositionalArgs {
			out = append(out, v)
		}

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

		//if commandSpec.Environment.InheritEnvironment {
		//	out = append(out, os.Environ()...)
		//}
		if commandSpec.Environment.InheritEnvironment != nil {
			ieMap := commandSpec.Environment.InheritEnvironment
			if ieMap["*"] {
				out = append(out, os.Environ()...)
			} else {
				for _, v := range os.Environ() {
					key := v[:strings.Index(v, "=")]

					if ieMap[strings.ToLower(key)] {
						out = append(out, v)
					}
				}
			}
		}

		return out
	}()

	var out = commandSpec.Stdout
	if out == nil {
		switch {
		case strings.EqualFold(flagMap["dry-run"], "true") && (strings.EqualFold(flagMap["output-type"], "json") || strings.EqualFold(flagMap["output-type"], "text") || flagMap["output-type"] == ""): //  Dryrun has its own special sort of output, that supports non-json output.
			jsonMode := strings.EqualFold(flagMap["output-type"], "json")
			var fromTo common.FromTo
			if !jsonMode && len(commandSpec.Targets) >= 2 {
				fromTo = common.FromTo(commandSpec.Targets[0].Location())<<8 | common.FromTo(commandSpec.Targets[1].Location())
			}
			out = &AzCopyParsedDryrunStdout{
				JsonMode: jsonMode,
				fromTo:   fromTo,
				Raw:      make(map[string]bool),
			}
		case !strings.EqualFold(flagMap["output-type"], "json"): // Won't parse non-computer-readable outputs
			out = &AzCopyRawStdout{}
		case commandSpec.Verb == AzCopyVerbCopy || commandSpec.Verb == AzCopyVerbSync || commandSpec.Verb == AzCopyVerbRemove:

			out = &AzCopyParsedCopySyncRemoveStdout{
				JobPlanFolder: *commandSpec.Environment.JobPlanLocation,
				LogFolder:     *commandSpec.Environment.LogLocation,
			}
		case commandSpec.Verb == AzCopyVerbList:
			out = &AzCopyParsedListStdout{}
		case commandSpec.Verb == AzCopyVerbJobsList:
			out = &AzCopyParsedJobsListStdout{}
		case commandSpec.Verb == AzCopyVerbJobsResume:
			out = &AzCopyParsedCopySyncRemoveStdout{ // Resume command treated the same as copy/sync/remove
				JobPlanFolder: *commandSpec.Environment.JobPlanLocation,
				LogFolder:     *commandSpec.Environment.LogLocation,
			}
		case commandSpec.Verb == AzCopyVerbLoginStatus:
			out = &AzCopyParsedLoginStatusStdout{}

		default: // We don't know how to parse this.
			out = &AzCopyRawStdout{}
		}
	}

	stderr := &bytes.Buffer{}
	command := exec.Cmd{
		Path: GlobalConfig.AzCopyExecutableConfig.ExecutablePath,
		Args: args,
		Env:  env,

		Stdout: out, // todo
		Stderr: stderr,
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

	// validate log file retention for jobs clean command before the job logs are cleaned up and uploaded
	if !a.Failed() && len(commandSpec.PositionalArgs) != 0 && commandSpec.PositionalArgs[0] == "clean" {
		ValidateLogFileRetention(a, *commandSpec.Environment.LogLocation, 1)
	}

	a.Cleanup(func(a Asserter) {
		UploadLogs(a, out, stderr, commandSpec.Environment)
		_ = os.RemoveAll(DerefOrZero(commandSpec.Environment.LogLocation))
	})

	return out, &AzCopyJobPlan{}
}

func UploadLogs(a Asserter, stdout AzCopyStdout, stderr *bytes.Buffer, env *AzCopyEnvironment) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("Log cleanup failed", err, "\n", string(debug.Stack()))
		}
	}()

	logDropPath := GlobalConfig.AzCopyExecutableConfig.LogDropPath
	if logDropPath == "" || !a.Failed() {
		return
	}

	if env.LogUploadDir == nil {
		logDir := DerefOrZero(env.LogLocation)

		var err error
		// Previously, we searched for a job ID to upload. Instead, we now treat shared environments as "sessions", and upload the logs all together.
		sessionId := uuid.NewString()
		env.SessionId = &sessionId

		// Create the destination log directory
		destLogDir := filepath.Join(logDropPath, sessionId)
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
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			a.NoError("Failed to copy log files", err)
		}

		env.LogUploadDir = &destLogDir
	}

	sessionId := *env.SessionId
	destLogDir := *env.LogUploadDir
	runCount := DerefOrZero(env.RunCount)
	runCount++
	env.RunCount = &runCount

	// Write stdout to the folder instead of the job log
	f, err := os.OpenFile(filepath.Join(destLogDir, fmt.Sprintf("stdout-%03d.txt", runCount)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0744)
	a.NoError("Failed to create stdout file", err)
	_, err = f.WriteString(stdout.String())
	a.NoError("Failed to write stdout file", err)
	err = f.Close()
	a.NoError("Failed to close stdout file", err)

	// If stderr is non-zero, output that too!
	if stderr != nil && stderr.Len() > 0 {
		f, err := os.OpenFile(filepath.Join(destLogDir, fmt.Sprintf("stderr-%03d.txt", runCount)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0744)
		a.NoError("Failed to create stdout file", err)
		_, err = stderr.WriteTo(f)
		a.NoError("Failed to write stdout file", err)
		err = f.Close()
		a.NoError("Failed to close stdout file", err)
	}

	if runCount == 1 {
		a.Log("Uploaded failed run logs for session %s", sessionId)
	}
}

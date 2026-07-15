package e2etest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// AzCopyJobPlan todo probably load the job plan directly? WI#26418256
type AzCopyJobPlan struct{}

type AzCopyStdout interface {
	RawStdout() []string

	io.Writer
	fmt.Stringer
}

type AzCopyDiscardStdout struct{}

func (a *AzCopyDiscardStdout) RawStdout() []string {
	return []string{}
}

func (a *AzCopyDiscardStdout) Write(p []byte) (n int, err error) {
	fmt.Print(string(p))

	// no-op
	return len(p), nil
}

func (a *AzCopyDiscardStdout) String() string {
	return ""
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
	AzCopyVerbJobsRemove  AzCopyVerb = "jobs remove"
	AzCopyVerbJobsShow    AzCopyVerb = "jobs show"
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

	// These fields should almost never be intentionally set by a test writer unless the author really knows what they're doing,
	// as the fields are automatically controlled.
	ParentContext          *AzCopyEnvironmentContext
	EnvironmentId          *uint
	RunCount               *uint
	AzcopyConcurrencyValue *string `env:"AZCOPY_CONCURRENCY_VALUE"`
}

func (env *AzCopyEnvironment) InheritEnvVar(name string) {
	env.EnsureInheritEnvironment()
	env.InheritEnvironment[strings.ToLower(name)] = true
}

func (env *AzCopyEnvironment) EnsureInheritEnvironment() {
	if env.InheritEnvironment == nil {
		env.DefaultInheritEnvironment(nil, context.TODO()) // context isn't important in this default yet
	}
}

var RunAzCopyDefaultInheritEnvironment = map[string]bool{
	"path":             true,
	"home":             true,
	"userprofile":      true,
	"homepath":         true,
	"homedrive":        true,
	"azure_config_dir": true,
}

func (env *AzCopyEnvironment) DefaultInheritEnvironment(a ScenarioAsserter, ctx context.Context) map[string]bool {
	env.InheritEnvironment = RunAzCopyDefaultInheritEnvironment

	return env.InheritEnvironment
}

func (env *AzCopyEnvironment) generateAzcopyDir(a ScenarioAsserter, ctx context.Context) {
	envCtx := ctx.Value(AzCopyEnvironmentManagerKey{}).(*AzCopyEnvironmentContext)
	envTmpPath := envCtx.GetEnvTempPath(env)

	err := os.MkdirAll(envTmpPath, 0777)
	a.NoError("failed to create env dir ("+envTmpPath+")", err, true)
	env.LogLocation = pointerTo(filepath.Join(envTmpPath, LogSubdir))
	env.JobPlanLocation = pointerTo(filepath.Join(envTmpPath, PlanSubdir))
}

func (env *AzCopyEnvironment) DefaultLogLoc(a ScenarioAsserter, ctx context.Context) string {
	if env.JobPlanLocation != nil {
		env.LogLocation = env.JobPlanLocation
	} else if env.LogLocation == nil {
		env.generateAzcopyDir(a, ctx)
	}

	return *env.LogLocation
}

func (env *AzCopyEnvironment) DefaultPlanLoc(a ScenarioAsserter, ctx context.Context) string {
	if env.LogLocation != nil {
		env.JobPlanLocation = env.LogLocation
	} else if env.JobPlanLocation == nil {
		env.generateAzcopyDir(a, ctx)
	}

	return *env.JobPlanLocation
}

func (c *AzCopyCommand) applyTargetAuth(a Asserter, target ResourceManager, id int) string {
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

	if target.Account() != nil {
		availableTypes := target.Account().AvailableAuthTypes()
		if !availableTypes.Includes(intendedAuthType) {
			log := fmt.Sprintf("requested auth type %s is not supported by account %s (target #%d); falling back to ",
				intendedAuthType.String(), target.Account().AccountName(), id)
			switch {
			case availableTypes.Includes(EExplicitCredentialType.OAuth()):
				intendedAuthType = EExplicitCredentialType.OAuth()
			case availableTypes.Includes(EExplicitCredentialType.SASToken()):
				intendedAuthType = EExplicitCredentialType.SASToken()
			default:
				intendedAuthType = EExplicitCredentialType.None()
			}
			log += intendedAuthType.String()
			a.Log(log)
		}
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
					} else if staticOauth.OAuthSource.CLIInherit {
						c.Environment.AutoLoginMode = pointerTo("azcli")
						c.Environment.AutoLoginTenantID = common.Iff(tenant != "", &tenant, nil)
					}
				} else {
					// oauth should reliably work
					oAuthInfo := GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo
					if oAuthInfo.Environment == AzurePipeline {
						// No need to force keep path, we already inherit that.
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
				var mode common.AutoLoginType
				a.NoError("failed to parse auto login mode `"+*c.Environment.AutoLoginMode+"`", mode.Parse(*c.Environment.AutoLoginMode))
				if mode == common.EAutoLoginType.Workload() {
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

	// we have no need to update our context manager, Fetch should do it for us.
	envCtx := FetchAzCopyEnvironmentContext(a)
	envCtx.SetupCleanup(a) // Make sure we add the cleanup hook; the sync.Once ensures idempotency.

	// register our environment, or create a new one if needed.
	var runNum uint
	if env := commandSpec.Environment; env == nil {
		commandSpec.Environment = envCtx.CreateEnvironment()
	} else {
		runNum = envCtx.RegisterEnvironment(env)
	}

	ctx := context.WithValue(envCtx, AzCopyRunNumKey{}, runNum)
	ctx = context.WithValue(ctx, AzCopyEnvironmentKey{}, commandSpec.Environment)

	// separate these from the struct so their execution order is fixed
	// Setup the positional args
	args := func() []string {
		if commandSpec.Environment == nil {
			commandSpec.Environment = &AzCopyEnvironment{}
		}

		out := []string{GlobalConfig.AzCopyExecutableConfig.ExecutablePath}
		out = append(out, strings.Split(string(commandSpec.Verb), " ")...)

		for _, v := range commandSpec.PositionalArgs {
			out = append(out, v)
		}

		for k, v := range commandSpec.Targets {
			out = append(out, commandSpec.applyTargetAuth(a, v, k))
		}

		if commandSpec.Flags == nil {
			switch commandSpec.Verb {
			case AzCopyVerbCopy:
				commandSpec.Flags = CopyFlags{}
			case AzCopyVerbSync:
				commandSpec.Flags = SyncFlags{}
			case AzCopyVerbList:
				commandSpec.Flags = ListFlags{}
			case AzCopyVerbLogin:
				commandSpec.Flags = LoginFlags{}
			case AzCopyVerbLoginStatus:
				commandSpec.Flags = LoginStatusFlags{}
			case AzCopyVerbRemove:
				commandSpec.Flags = RemoveFlags{}
			case AzCopyVerbJobsClean:
				commandSpec.Flags = JobsCleanFlags{}
			case AzCopyVerbJobsRemove:
				commandSpec.Flags = JobsRemoveFlags{}
			case AzCopyVerbJobsList:
				commandSpec.Flags = JobsListFlags{}
			case AzCopyVerbJobsShow:
				commandSpec.Flags = JobsShowFlags{}
			default:
				commandSpec.Flags = GlobalFlags{}
			}
		}

		flagMap = MapFromTags(reflect.ValueOf(commandSpec.Flags), "flag", a, ctx)
		for k, v := range flagMap {
			out = append(out, fmt.Sprintf("--%s=%s", k, v))
		}

		return out
	}()
	// Setup the env vars
	env := func() []string {
		out := make([]string, 0)

		envMap = MapFromTags(reflect.ValueOf(commandSpec.Environment), "env", a, ctx)

		for k, v := range envMap {
			out = append(out, fmt.Sprintf("%s=%s", k, v))
		}

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
	if out == nil { // Select the correct stdoutput parser
		switch {
		// Dry-run parser
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

		// Text formats don't get parsed usually
		case !strings.EqualFold(flagMap["output-type"], "json"):
			out = &AzCopyRawStdout{}

		// Copy/sync/remove share the same output format
		case commandSpec.Verb == AzCopyVerbCopy || commandSpec.Verb == AzCopyVerbSync || commandSpec.Verb == AzCopyVerbRemove:
			out = &AzCopyParsedCopySyncRemoveStdout{
				JobPlanFolder: *commandSpec.Environment.JobPlanLocation,
				LogFolder:     *commandSpec.Environment.LogLocation,
			}

		// List
		case commandSpec.Verb == AzCopyVerbList:
			out = &AzCopyParsedListStdout{}

		// Jobs list
		case commandSpec.Verb == AzCopyVerbJobsList:
			out = &AzCopyParsedJobsListStdout{}

		// Jobs resume
		case commandSpec.Verb == AzCopyVerbJobsResume:
			out = &AzCopyParsedCopySyncRemoveStdout{ // Resume command treated the same as copy/sync/remove
				JobPlanFolder: *commandSpec.Environment.JobPlanLocation,
				LogFolder:     *commandSpec.Environment.LogLocation,
			}

		case commandSpec.Verb == AzCopyVerbJobsShow:
			if !commandSpec.ShouldFail {
				out = &AzCopyParsedJobsShowStdout{}
			} else {
				out = &AzCopyRawStdout{}
			}

		// Login status
		case commandSpec.Verb == AzCopyVerbLoginStatus:
			out = &AzCopyParsedLoginStatusStdout{}

		// Login (interactive)
		case commandSpec.Verb == AzCopyVerbLogin:
			var lType common.AutoLoginType
			if ltStr := flagMap["login-type"]; ltStr != "" {
				_ = lType.Parse(ltStr)
			}

			if lType.IsInteractive() {
				out = NewAzCopyInteractiveStdout(a)
				break
			}

			fallthrough

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

	// The environment manager will handle cleanup for us-- All we need to do at this point is register our stdout.
	envCtx.RegisterLogUpload(LogUpload{
		EnvironmentID: *commandSpec.Environment.EnvironmentId,
		RunID:         runNum,

		Stdout: out.String(),
		Stderr: stderr.String(),
	})

	return out, &AzCopyJobPlan{}
}

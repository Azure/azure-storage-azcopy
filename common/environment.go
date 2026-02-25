// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"

	"github.com/JeffreyRichter/enum/enum"
)

type EnvironmentVariable struct {
	Name         string
	DefaultValue string
	Description  string
	Hidden       bool

	// Replaces specifies prior names for this environment variable. These will be looked up when getting the environment variable.
	Replaces []string
	// ReplacedBy specifies the name of the environment variable replacing this one.
	ReplacedBy string
}

const (
	deprecatedEnvironmentVariableWarningFormat = "Deprecated environment variable %s is set. This has been read in place of %s. Please migrate to %s (check `azcopy env` for details.)"
)

func doDeprecatedEnvWarn(deprecatedName, newName string) {
	DeprecatedEnvVarsWarnOnce[deprecatedName].Do(func() {
		GetLifecycleMgr().Warn(fmt.Sprintf(deprecatedEnvironmentVariableWarningFormat,
			deprecatedName, newName, newName))
	})
}

func (env EnvironmentVariable) Lookup(intentionDeprecatedLookup ...bool) (val, name string, present bool) {
	// If we have deprecated this env variable, and we're looking it up without intending to, something's wrong. Let the developer know by panicking.
	if env.ReplacedBy != "" && !FirstOrZero(intentionDeprecatedLookup) {
		panic(fmt.Sprintf("Unintentional lookup of deprecated environment variable %s (replaced by %s). If this was intentional, add true to parameters of the called function.", env.Name, env.ReplacedBy))
	}

	// Prefer the "latest" name instead of any older ones, falling back to the older ones, then the default if it is not present.
	val, ok := os.LookupEnv(env.Name)
	if ok {
		if env.ReplacedBy != "" {
			doDeprecatedEnvWarn(env.Name, env.ReplacedBy)
		}

		return val, env.Name, true
	}

	// Check for any of the fallback deprecated names...
	for _, deprecatedName := range env.Replaces {
		val, ok = os.LookupEnv(deprecatedName)
		if ok {
			doDeprecatedEnvWarn(deprecatedName, env.Name)

			return val, deprecatedName, true
		}
	}

	// then, if we haven't found anything viable, fall back to the default. If unset, it will be "".
	return env.DefaultValue, env.Name, false
}

// IsSet is a breakout for Lookup's present result, useful in-line.
func (env EnvironmentVariable) IsSet(intentionDeprecatedLookup ...bool) bool {
	_, _, ok := env.Lookup(intentionDeprecatedLookup...)
	return ok
}

// LookupName is a breakout for Lookup's name result, useful in-line.
func (env EnvironmentVariable) LookupName(intentionDeprecatedLookup ...bool) string {
	_, name, _ := env.Lookup(intentionDeprecatedLookup...)
	return name
}

// Value is a breakout for Lookup's val result, useful in-line.
func (env EnvironmentVariable) Value(intentionDeprecatedLookup ...bool) string {
	val, _, _ := env.Lookup(intentionDeprecatedLookup...)
	return val
}

// Clear clears the environment variable, and any historical variants.
// Notably, does not intentionally climb further up in "newness".
func (env EnvironmentVariable) Clear(intentionDeprecatedClear ...bool) {
	if !FirstOrZero(intentionDeprecatedClear) && env.ReplacedBy != "" {
		panic(fmt.Sprintf("Unintentional clear of deprecated environment variable %s (replaced by %s). If this was intentional, add true to parameters of the called function. Note that it will not climb in \"newness\".", env.Name, env.ReplacedBy))
	}

	_ = os.Unsetenv(env.Name)

	// If we are intentionally clearing a deprecated variable, we care about *probably*, just that variable's name.
	if !FirstOrZero(intentionDeprecatedClear) {
		for _, v := range env.Replaces {
			_ = os.Unsetenv(v)
		}
	}
}

// This array needs to be updated when a new public environment variable is added
// Things are here, rather than in command line parameters for one of two reasons:
// 1. They are optional and obscure (e.g. performance tuning parameters) or
// 2. They are authentication secrets, which we do not accept on the command line
var VisibleEnvironmentVariables = []EnvironmentVariable{
	EEnvironmentVariable.LogLocation(),
	EEnvironmentVariable.JobPlanLocation(),
	EEnvironmentVariable.ConcurrencyValue(),
	EEnvironmentVariable.TransferInitiationPoolSize(),
	EEnvironmentVariable.EnumerationPoolSize(),
	EEnvironmentVariable.DisableHierarchicalScanning(),
	EEnvironmentVariable.ParallelStatFiles(),
	EEnvironmentVariable.BufferGB(),
	EEnvironmentVariable.AWSAccessKeyID(),
	EEnvironmentVariable.AWSSecretAccessKey(),
	EEnvironmentVariable.GoogleAppCredentials(),
	EEnvironmentVariable.ShowPerfStates(),
	EEnvironmentVariable.PacePageBlobs(),
	EEnvironmentVariable.AutoTuneToCpu(),
	EEnvironmentVariable.CacheProxyLookup(),
	EEnvironmentVariable.DefaultServiceApiVersion(),
	EEnvironmentVariable.UserAgentPrefix(),
	EEnvironmentVariable.AWSAccessKeyID(),
	EEnvironmentVariable.AWSSecretAccessKey(),
	EEnvironmentVariable.ClientSecret(),
	EEnvironmentVariable.CertificatePassword(),
	EEnvironmentVariable.AutoLoginType(),
	EEnvironmentVariable.TenantID(),
	EEnvironmentVariable.AADEndpoint(),
	EEnvironmentVariable.ApplicationID(),
	EEnvironmentVariable.CertificatePath(),
	EEnvironmentVariable.ManagedIdentityClientID(),
	EEnvironmentVariable.ManagedIdentityObjectID(),
	EEnvironmentVariable.ManagedIdentityResourceString(),
	EEnvironmentVariable.RequestTryTimeout(),
	EEnvironmentVariable.CPKEncryptionKey(),
	EEnvironmentVariable.CPKEncryptionKeySHA256(),
	EEnvironmentVariable.DisableSyslog(),
	EEnvironmentVariable.MimeMapping(),
	EEnvironmentVariable.DownloadToTempPath(),
}

// Oncer should pretty much always be *sync.Once.
// But... the interface is replicated, so we can inject *not* a sync.once for testing.
type Oncer interface {
	Do(func())
}

var DeprecatedEnvVarsWarnOnce = func() map[string]Oncer {
	enumVal := reflect.ValueOf(EEnvironmentVariable)
	methodCount := enumVal.NumMethod()
	varType := reflect.TypeOf(EnvironmentVariable{})

	out := make(map[string]Oncer)

	for idx := range methodCount {
		methodVal := enumVal.Method(idx)
		methodType := methodVal.Type()

		// we're looking for our typical environment variable specification. Should be just the enum type in, and just the EnvironmentVariable type out.
		if !(methodType.NumIn() == 1 && methodType.In(0) == enumVal.Type()) {
			continue
		}

		if !(methodType.NumOut() == 1 && methodType.Out(0) == varType) {
			continue
		}

		// Call it, pull the resulting type, and earmark it if it is deprecated.
		results := methodVal.Call([]reflect.Value{enumVal})
		envVar := results[0].Interface().(EnvironmentVariable)

		if envVar.ReplacedBy != "" {
			out[envVar.Name] = &sync.Once{}
		}
		for _, v := range envVar.Replaces {
			out[v] = &sync.Once{}
		}
	}

	return out
}()

type eEnvironmentVariable struct{}

var EEnvironmentVariable = eEnvironmentVariable{}

func (eEnvironmentVariable) UserDir() EnvironmentVariable {
	// Only used internally, not listed in the environment variables.
	return EnvironmentVariable{
		Name: Iff(runtime.GOOS == "windows", "USERPROFILE", "HOME"),
	}
}

var EAutoLoginType = AutoLoginType(0)

type AutoLoginType uint8

func (AutoLoginType) Device() AutoLoginType     { return AutoLoginType(0) }
func (AutoLoginType) SPN() AutoLoginType        { return AutoLoginType(1) }
func (AutoLoginType) MSI() AutoLoginType        { return AutoLoginType(2) }
func (AutoLoginType) AzCLI() AutoLoginType      { return AutoLoginType(3) }
func (AutoLoginType) PsCred() AutoLoginType     { return AutoLoginType(4) }
func (AutoLoginType) Workload() AutoLoginType   { return AutoLoginType(5) }
func (AutoLoginType) TokenStore() AutoLoginType { return AutoLoginType(255) } // Storage Explorer internal integration only. Do not add this to ValidAutoLoginTypes.

func (d AutoLoginType) IsInteractive() bool {
	return d == d.Device()
}

func (d AutoLoginType) String() string {
	return strings.ToLower(enum.StringInt(d, reflect.TypeOf(d)))
}

func (d *AutoLoginType) Parse(s string) error {
	// allow empty to mean "Device"
	if s == "" {
		*d = EAutoLoginType.Device()
		return nil
	}

	val, err := enum.ParseInt(reflect.TypeOf(d), s, true, true)
	if err == nil {
		*d = val.(AutoLoginType)
	}
	return err
}

// MarshalJSON customizes the JSON encoding for AutoLoginType
func (d AutoLoginType) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

// UnmarshalJSON customizes the JSON decoding for AutoLoginType
func (d *AutoLoginType) UnmarshalJSON(data []byte) error {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if strValue, ok := v.(string); ok {
		return d.Parse(strValue)
	}
	// Handle numeric values
	if numValue, ok := v.(float64); ok {
		if numValue < 0 || numValue > 255 {
			return fmt.Errorf("value out of range for _token_source_refresh: %v", numValue)
		}
		*d = AutoLoginType(uint8(numValue))
		return nil
	}

	return fmt.Errorf("unsupported type for AutoLoginType: %T", v)
}

func ValidAutoLoginTypes() []string {
	return []string{
		EAutoLoginType.Device().String() + " (Device code workflow)",
		EAutoLoginType.SPN().String() + " (Service Principal)",
		EAutoLoginType.MSI().String() + " (Managed Service Identity)",
		EAutoLoginType.AzCLI().String() + " (Azure CLI)",
		EAutoLoginType.PsCred().String() + " (Azure PowerShell)",
		EAutoLoginType.Workload().String() + " (Workload Identity)",
	}
}

func (eEnvironmentVariable) AutoLoginType() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_AUTO_LOGIN_TYPE",
		Description: "Specify the credential type to access Azure Resource without invoking the login command and using the OS secret store, available values are " + strings.Join(ValidAutoLoginTypes(), ", ") + ".",
	}
}

func (eEnvironmentVariable) TenantID() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_TENANT_ID",
		Description: "The Azure Active Directory tenant ID to use for OAuth device interactive login. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (eEnvironmentVariable) AADEndpoint() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_ACTIVE_DIRECTORY_ENDPOINT",
		Description: "The Azure Active Directory endpoint to use. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (eEnvironmentVariable) ApplicationID() EnvironmentVariable {
	// Used for auto-login.
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_APPLICATION_ID",
		Description: "The Azure Active Directory application ID used for Service Principal authentication. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (eEnvironmentVariable) ClientSecret() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_CLIENT_SECRET",
		Description: "The Azure Active Directory client secret used for Service Principal authentication",
		Hidden:      true,
	}
}

func (eEnvironmentVariable) CertificatePath() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_CERT_PATH",
		Description: "The path of the certificate used for Service Principal authentication. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (eEnvironmentVariable) CertificatePassword() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_CERT_PASSWORD",
		Description: "The password used to decrypt the certificate used for Service Principal authentication.",
		Hidden:      true,
	}
}

// For MSI login
func (eEnvironmentVariable) ManagedIdentityClientID() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_MSI_CLIENT_ID",
		Description: "Client ID for User-assigned identity. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (eEnvironmentVariable) ManagedIdentityObjectID() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_MSI_OBJECT_ID",
		Description: "Object ID for user-assigned identity. This parameter is deprecated. Please use client id or resource id.",
	}
}

func (eEnvironmentVariable) ManagedIdentityResourceString() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_MSI_RESOURCE_STRING",
		Description: "Resource String for user-assigned identity. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (eEnvironmentVariable) ConcurrencyValue() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_CONCURRENCY_VALUE",
		Description: "Overrides how many HTTP connections work on transfers. By default, this number is determined based on the number of logical cores on the machine.",
	}
}

// added in so that CPU usage detection can be disabled if advanced users feel it is causing tuning to be too conservative (i.e. not enough concurrency, due to detected CPU usage)
func (eEnvironmentVariable) AutoTuneToCpu() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_TUNE_TO_CPU",
		Description: "Set to false to prevent AzCopy from taking CPU usage into account when auto-tuning its concurrency level (e.g. in the benchmark command).",
	}
}

func (eEnvironmentVariable) TransferInitiationPoolSize() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_CONCURRENT_FILES",
		Description: "Overrides the (approximate) number of files that are in progress at any one time, by controlling how many files we concurrently initiate transfers for.",
	}
}

const azCopyConcurrentScan = "AZCOPY_CONCURRENT_SCAN"

func (eEnvironmentVariable) EnumerationPoolSize() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        azCopyConcurrentScan,
		Description: "Controls the (max) degree of parallelism used during scanning. Only affects parallelized enumerators, which include Azure Files/Blobs, and local file systems.",
	}
}

func (eEnvironmentVariable) DisableHierarchicalScanning() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_DISABLE_HIERARCHICAL_SCAN",
		Description: "Applies only when Azure Blobs is the source. Concurrent scanning is faster but employs the hierarchical listing API, which can result in more IOs/cost. Specify 'true' to sacrifice performance but save on cost.",
	}
}

func (eEnvironmentVariable) ParallelStatFiles() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_PARALLEL_STAT_FILES",
		Description:  "Causes AzCopy to look up file properties on parallel 'threads' when scanning the local file system.  The threads are drawn from the pool defined by " + azCopyConcurrentScan + ".  Setting this to true may improve scanning performance on Linux.  Not needed or recommended on Windows.",
		DefaultValue: "false", // we are defaulting to false even on Linux, because it does create more load, in terms of file system IOPS, and we don't yet have a large enough variety of real-world test cases to justify the default being true
	}
}

func (eEnvironmentVariable) OptimizeSparsePageBlobTransfers() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_OPTIMIZE_SPARSE_PAGE_BLOB",
		Description:  "Provide a knob to disable the optimizations in case they cause customers any unforeseen issue. Set to any other value than 'true' to disable.",
		DefaultValue: "true",
	}
}

func (eEnvironmentVariable) CacheProxyLookup() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_CACHE_PROXY_LOOKUP",
		Description:  "By default AzCopy on Windows will cache proxy server lookups at hostname level (not taking URL path into account). Set to any other value than 'true' to disable the cache.",
		DefaultValue: "true",
	}
}

func (eEnvironmentVariable) LoginCacheName() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_LOGIN_CACHE_NAME",
		Description: "Do not use in production. Overrides the file name or key name used to cache azcopy's token. Do not use in production. This feature is not documented, intended for testing, and may break. Do not use in production.",
	}
}

func (eEnvironmentVariable) LogLocation() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_LOG_LOCATION",
		Description: "Overrides where the log files are stored, to avoid filling up a disk.",
	}
}

func (eEnvironmentVariable) JobPlanLocation() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_JOB_PLAN_LOCATION",
		Description: "Overrides where the job plan files (used for progress tracking and resuming) are stored, to avoid filling up a disk.",
	}
}

func (eEnvironmentVariable) BufferGB() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_BUFFER_GB",
		Description: "Max number of GB that AzCopy should use for buffering data between network and disk. May include decimal point, e.g. 0.5. The default is based on machine size.",
	}
}

func (eEnvironmentVariable) AccountName() EnvironmentVariable {
	return EnvironmentVariable{Name: "ACCOUNT_NAME"}
}

func (eEnvironmentVariable) AccountKey() EnvironmentVariable {
	return EnvironmentVariable{
		Name:   "ACCOUNT_KEY",
		Hidden: true,
	}
}

func (eEnvironmentVariable) ProfileCPU() EnvironmentVariable {
	return EnvironmentVariable{Name: "AZCOPY_PROFILE_CPU"}
}

func (eEnvironmentVariable) ProfileMemory() EnvironmentVariable {
	return EnvironmentVariable{Name: "AZCOPY_PROFILE_MEM"}
}

func (eEnvironmentVariable) PacePageBlobs() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_PACE_PAGE_BLOBS",
		Description: "Should throughput for page blobs automatically be adjusted to match Service limits? Default is true. Set to 'false' to disable",
	}
}

func (eEnvironmentVariable) ShowPerfStates() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SHOW_PERF_STATES",
		Description: "If set, to anything, on-screen output will include counts of chunks by state",
	}
}

func (eEnvironmentVariable) AWSAccessKeyID() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AWS_ACCESS_KEY_ID",
		Description: "The AWS access key ID for S3 source used in service to service copy.",
	}
}

func (eEnvironmentVariable) AWSSecretAccessKey() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AWS_SECRET_ACCESS_KEY",
		Description: "The AWS secret access key for S3 source used in service to service copy.",
		Hidden:      true,
	}
}

// AwsSessionToken is temporarily internally reserved, and not exposed to users.
func (eEnvironmentVariable) AwsSessionToken() EnvironmentVariable {
	return EnvironmentVariable{Name: "AWS_SESSION_TOKEN"}
}

func (eEnvironmentVariable) GoogleAppCredentials() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "GOOGLE_APPLICATION_CREDENTIALS",
		Description: "The application credentials required to access GCP resources for service to service copy.",
	}
}

func (eEnvironmentVariable) GoogleCloudProject() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "GOOGLE_CLOUD_PROJECT",
		Description: "Project ID required for service level traversals in Google Cloud Storage",
	}
}

// OAuthTokenInfo is only used for internal integration.
func (eEnvironmentVariable) OAuthTokenInfo() EnvironmentVariable {
	return EnvironmentVariable{Name: "AZCOPY_OAUTH_TOKEN_INFO"}
}

// CredentialType is only used for internal integration.
func (eEnvironmentVariable) CredentialType() EnvironmentVariable {
	return EnvironmentVariable{Name: "AZCOPY_CRED_TYPE"}
}

func (eEnvironmentVariable) DefaultServiceApiVersion() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_DEFAULT_SERVICE_API_VERSION",
		DefaultValue: "2025-07-05",
		Description:  "Overrides the service API version so that AzCopy could accommodate custom environments such as Azure Stack.",
	}
}

func (eEnvironmentVariable) UserAgentPrefix() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_USER_AGENT_PREFIX",
		Description: "Add a prefix to the default AzCopy User Agent, which is used for telemetry purposes. A space is automatically inserted.",
	}
}

func (eEnvironmentVariable) RequestTryTimeout() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_REQUEST_TRY_TIMEOUT",
		Description: "Set time (in minutes) for how long AzCopy should try to upload files for each request before AzCopy times out.",
	}
}

func (eEnvironmentVariable) CPKEncryptionKey() EnvironmentVariable {
	return EnvironmentVariable{Name: "CPK_ENCRYPTION_KEY", Hidden: true}
}

func (eEnvironmentVariable) CPKEncryptionKeySHA256() EnvironmentVariable {
	return EnvironmentVariable{Name: "CPK_ENCRYPTION_KEY_SHA256", Hidden: false}
}

func (eEnvironmentVariable) DisableSyslog() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_DISABLE_SYSLOG",
		DefaultValue: "false",
		Description: "Disables logging in Syslog or Windows Event Logger. By default we log to these channels. " +
			"However, to reduce the noise in Syslog/Windows Event Log, consider setting this environment variable to true.",
	}
}

func (eEnvironmentVariable) MimeMapping() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_CONTENT_TYPE_MAP",
		DefaultValue: "",
		Description:  "Location of the file to override default OS mime mapping",
	}
}

func (eEnvironmentVariable) DownloadToTempPath() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_DOWNLOAD_TO_TEMP_PATH",
		DefaultValue: "true",
		Description:  "Configures azcopy to download to a temp path before actual download. Allowed values are true/false",
	}
}

func (eEnvironmentVariable) DisableBlobTransferResume() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_DISABLE_INCOMPLETE_BLOB_TRANSFER",
		DefaultValue: "false",
		Description:  "An incomplete transfer to blob endpoint will be resumed from start if set to true",
	}
}

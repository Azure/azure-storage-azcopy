package enum

import (
	"iter"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common/enum/enum_def"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
)

type EnvironmentVariable struct {
	Name         string
	DefaultValue string
	Description  string
	// ContainsSecret - When running `azcopy env`, should we hide the stored value by default?
	ContainsSecret bool
	// DeveloperOption - do we hide it from the public? i.e. is this an internal/integration thing?
	DeveloperOption bool
}

func (e EnvironmentVariable) String() string {
	return EEnvironmentVariable.String(e)
}

var EEnvironmentVariable = eEnvironmentVariable{}

type eEnvironmentVariable struct {
	enum_def.EnumImpl[EnvironmentVariable, eEnvironmentVariable]
}

// VisibleEnvironmentVariables wraps and filters EEnvironmentVariable.Values() to show all values not marked with Hide
var VisibleEnvironmentVariables = func() iter.Seq[EnvironmentVariable] {
	return func(yield func(EnvironmentVariable) bool) {
		for val := range EEnvironmentVariable.Values() {
			if val.DeveloperOption {
				continue
			}

			if !yield(val) {
				return
			}
		}
	}
}

// ========= Values ==========

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
		Name:           "AZCOPY_SPA_CLIENT_SECRET",
		Description:    "The Azure Active Directory client secret used for Service Principal authentication",
		ContainsSecret: true,
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
		Name:           "AZCOPY_SPA_CERT_PASSWORD",
		Description:    "The password used to decrypt the certificate used for Service Principal authentication.",
		ContainsSecret: true,
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
		Name:            "AZCOPY_OPTIMIZE_SPARSE_PAGE_BLOB",
		Description:     "Provide a knob to disable the optimizations in case they cause customers any unforeseen issue. Set to any other value than 'true' to disable.",
		DefaultValue:    "true",
		DeveloperOption: true,
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
		Name:            "AZCOPY_LOGIN_CACHE_NAME",
		Description:     "Do not use in production. Overrides the file name or key name used to cache azcopy's token. Do not use in production. This feature is not documented, intended for testing, and may break. Do not use in production.",
		DeveloperOption: true,
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
	return EnvironmentVariable{
		Name:            "ACCOUNT_NAME",
		DeveloperOption: true,
	}
}

func (eEnvironmentVariable) AccountKey() EnvironmentVariable {
	return EnvironmentVariable{
		Name:            "ACCOUNT_KEY",
		ContainsSecret:  true,
		DeveloperOption: true,
	}
}

func (eEnvironmentVariable) ProfileCPU() EnvironmentVariable {
	return EnvironmentVariable{
		Name:            "AZCOPY_PROFILE_CPU",
		DeveloperOption: true,
	}
}

func (eEnvironmentVariable) ProfileMemory() EnvironmentVariable {
	return EnvironmentVariable{
		Name:            "AZCOPY_PROFILE_MEM",
		DeveloperOption: true,
	}
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
		Name:           "AWS_SECRET_ACCESS_KEY",
		Description:    "The AWS secret access key for S3 source used in service to service copy.",
		ContainsSecret: true,
	}
}

func (eEnvironmentVariable) S3CompatibleEndpoint() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "S3_COMPATIBLE_ENDPOINT",
		Description: "The S3 compatible endpoint used for S3 source in service to service copy.",
	}
}

// AwsSessionToken is temporarily internally reserved, and not exposed to users.
func (eEnvironmentVariable) AwsSessionToken() EnvironmentVariable {
	return EnvironmentVariable{
		Name:            "AWS_SESSION_TOKEN",
		DeveloperOption: true,
	}
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

func (eEnvironmentVariable) DefaultServiceApiVersion() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_DEFAULT_SERVICE_API_VERSION",
		DefaultValue: "2025-05-05",
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
	return EnvironmentVariable{Name: "CPK_ENCRYPTION_KEY", ContainsSecret: true}
}

func (eEnvironmentVariable) CPKEncryptionKeySHA256() EnvironmentVariable {
	return EnvironmentVariable{Name: "CPK_ENCRYPTION_KEY_SHA256", ContainsSecret: false}
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
		Name:            "AZCOPY_DISABLE_INCOMPLETE_BLOB_TRANSFER",
		DefaultValue:    "false",
		Description:     "An incomplete transfer to blob endpoint will be resumed from start if set to true",
		DeveloperOption: true,
	}
}

func (eEnvironmentVariable) DisableReauth() EnvironmentVariable {
	return EnvironmentVariable{
		Name:            "AZCOPY_DISABLE_REAUTH",
		DefaultValue:    "false",
		Description:     "Disables automatic reauthentication prompt when a token expires. Set to true to prevent hangs in non-interactive contexts.",
		DeveloperOption: true,
	}
}

// KeyringConfig is only used for internal integration.
func (eEnvironmentVariable) KeyringConfig() EnvironmentVariable {
	return EnvironmentVariable{
		Name:            "AZCOPY_KEYRING",
		DefaultValue:    "false",
		Description:     "A json struct matching the schema of cred.KeyringEnvConf",
		DeveloperOption: true,
	}
}

// OAuthTokenInfo is only used for internal integration.
func (eEnvironmentVariable) OAuthTokenInfo() EnvironmentVariable {
	return EnvironmentVariable{
		Name:            "AZCOPY_OAUTH_TOKEN_INFO",
		DeveloperOption: true,
	}
}

// CredentialType is only used for internal integration.
func (eEnvironmentVariable) CredentialType() EnvironmentVariable {
	return EnvironmentVariable{
		Name:            "AZCOPY_CRED_TYPE",
		DeveloperOption: true,
	}
}

func (eEnvironmentVariable) UserDir() EnvironmentVariable {
	// Only used internally, not listed in the environment variables.
	return EnvironmentVariable{
		Name:            ternary.Iff(runtime.GOOS == "windows", "USERPROFILE", "HOME"),
		DeveloperOption: true,
	}
}

func (eEnvironmentVariable) AppDir() EnvironmentVariable {
	homeDir, _ := os.UserHomeDir()

	return EnvironmentVariable{
		Name:            "AZCOPY_APP_DIR",
		DefaultValue:    filepath.Join(homeDir, ".azcopy"),
		DeveloperOption: true,
	}
}

// Lookup returns the value of the environment variable and whether it was set.
// If not set, the default value and false are returned.
func (e EnvironmentVariable) Lookup() (string, bool) {
	v, ok := os.LookupEnv(e.Name)
	if !ok {
		return e.DefaultValue, false
	}
	return v, true
}

// Get returns the value of the environment variable, or the default value if not set.
func (e EnvironmentVariable) Get() string {
	v, _ := e.Lookup()
	return v
}

func (e EnvironmentVariable) IsSet() bool {
	_, ok := e.Lookup()
	return ok
}

// Clear unsets the environment variable.
func (e EnvironmentVariable) Clear() error {
	return os.Unsetenv(e.Name)
}

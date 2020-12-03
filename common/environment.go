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
	"runtime"
)

type EnvironmentVariable struct {
	Name         string
	DefaultValue string
	Description  string
	Hidden       bool
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
}

var EEnvironmentVariable = EnvironmentVariable{}

func (EnvironmentVariable) UserDir() EnvironmentVariable {
	// Only used internally, not listed in the environment variables.
	return EnvironmentVariable{
		Name: IffString(runtime.GOOS == "windows", "USERPROFILE", "HOME"),
	}
}

func (EnvironmentVariable) AutoLoginType() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_AUTO_LOGIN_TYPE",
		Description: "Specify the credential type to access Azure Resource without invoking the login command and using the OS secret store, available values SPN, MSI and DEVICE - sequentially for Service Principal, Managed Service Identity and Device workflow.",
	}
}

func (EnvironmentVariable) TenantID() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_TENANT_ID",
		Description: "The Azure Active Directory tenant ID to use for OAuth device interactive login. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (EnvironmentVariable) AADEndpoint() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_ACTIVE_DIRECTORY_ENDPOINT",
		Description: "The Azure Active Directory endpoint to use. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (EnvironmentVariable) ApplicationID() EnvironmentVariable {
	// Used for auto-login.
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_APPLICATION_ID",
		Description: "The Azure Active Directory application ID used for Service Principal authentication. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (EnvironmentVariable) ClientSecret() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_CLIENT_SECRET",
		Description: "The Azure Active Directory client secret used for Service Principal authentication",
		Hidden:      true,
	}
}

func (EnvironmentVariable) CertificatePath() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_CERT_PATH",
		Description: "The path of the certificate used for Service Principal authentication. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (EnvironmentVariable) CertificatePassword() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_CERT_PASSWORD",
		Description: "The password used to decrypt the certificate used for Service Principal authentication.",
		Hidden:      true,
	}
}

// For MSI login
func (EnvironmentVariable) ManagedIdentityClientID() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_MSI_CLIENT_ID",
		Description: "Client ID for User-assigned identity. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (EnvironmentVariable) ManagedIdentityObjectID() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_MSI_OBJECT_ID",
		Description: "Object ID for user-assigned identity. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (EnvironmentVariable) ManagedIdentityResourceString() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_MSI_RESOURCE_STRING",
		Description: "Resource String for user-assigned identity. This variable is only used for auto login, please use the command line flag instead when invoking the login command.",
	}
}

func (EnvironmentVariable) ConcurrencyValue() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_CONCURRENCY_VALUE",
		Description: "Overrides how many HTTP connections work on transfers. By default, this number is determined based on the number of logical cores on the machine.",
	}
}

// added in so that CPU usage detection can be disabled if advanced users feel it is causing tuning to be too conservative (i.e. not enough concurrency, due to detected CPU usage)
func (EnvironmentVariable) AutoTuneToCpu() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_TUNE_TO_CPU",
		Description: "Set to false to prevent AzCopy from taking CPU usage into account when auto-tuning its concurrency level (e.g. in the benchmark command).",
	}
}

func (EnvironmentVariable) TransferInitiationPoolSize() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_CONCURRENT_FILES",
		Description: "Overrides the (approximate) number of files that are in progress at any one time, by controlling how many files we concurrently initiate transfers for.",
	}
}

const azCopyConcurrentScan = "AZCOPY_CONCURRENT_SCAN"

func (EnvironmentVariable) EnumerationPoolSize() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        azCopyConcurrentScan,
		Description: "Controls the (max) degree of parallelism used during scanning. Only affects parallelized enumerators, which include Azure Files/Blobs, and local file systems.",
	}
}

func (EnvironmentVariable) DisableHierarchicalScanning() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_DISABLE_HIERARCHICAL_SCAN",
		Description: "Applies only when Azure Blobs is the source. Concurrent scanning is faster but employs the hierarchical listing API, which can result in more IOs/cost. Specify 'true' to sacrifice performance but save on cost.",
	}
}

func (EnvironmentVariable) ParallelStatFiles() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_PARALLEL_STAT_FILES",
		Description:  "Causes AzCopy to look up file properties on parallel 'threads' when scanning the local file system.  The threads are drawn from the pool defined by " + azCopyConcurrentScan + ".  Setting this to true may improve scanning performance on Linux.  Not needed or recommended on Windows.",
		DefaultValue: "false", // we are defaulting to false even on Linux, because it does create more load, in terms of file system IOPS, and we don't yet have a large enough variety of real-world test cases to justify the default being true
	}
}

func (EnvironmentVariable) OptimizeSparsePageBlobTransfers() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_OPTIMIZE_SPARSE_PAGE_BLOB",
		Description:  "Provide a knob to disable the optimizations in case they cause customers any unforeseen issue. Set to any other value than 'true' to disable.",
		DefaultValue: "true",
	}
}

func (EnvironmentVariable) CacheProxyLookup() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_CACHE_PROXY_LOOKUP",
		Description:  "By default AzCopy on Windows will cache proxy server lookups at hostname level (not taking URL path into account). Set to any other value than 'true' to disable the cache.",
		DefaultValue: "true",
	}
}

func (EnvironmentVariable) LogLocation() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_LOG_LOCATION",
		Description: "Overrides where the log files are stored, to avoid filling up a disk.",
	}
}

func (EnvironmentVariable) JobPlanLocation() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_JOB_PLAN_LOCATION",
		Description: "Overrides where the job plan files (used for progress tracking and resuming) are stored, to avoid filling up a disk.",
	}
}

func (EnvironmentVariable) BufferGB() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_BUFFER_GB",
		Description: "Max number of GB that AzCopy should use for buffering data between network and disk. May include decimal point, e.g. 0.5. The default is based on machine size.",
	}
}

func (EnvironmentVariable) AccountName() EnvironmentVariable {
	return EnvironmentVariable{Name: "ACCOUNT_NAME"}
}

func (EnvironmentVariable) AccountKey() EnvironmentVariable {
	return EnvironmentVariable{
		Name:   "ACCOUNT_KEY",
		Hidden: true,
	}
}

func (EnvironmentVariable) ProfileCPU() EnvironmentVariable {
	return EnvironmentVariable{Name: "AZCOPY_PROFILE_CPU"}
}

func (EnvironmentVariable) ProfileMemory() EnvironmentVariable {
	return EnvironmentVariable{Name: "AZCOPY_PROFILE_MEM"}
}

func (EnvironmentVariable) PacePageBlobs() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_PACE_PAGE_BLOBS",
		Description: "Should throughput for page blobs automatically be adjusted to match Service limits? Default is true. Set to 'false' to disable",
	}
}

func (EnvironmentVariable) ShowPerfStates() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SHOW_PERF_STATES",
		Description: "If set, to anything, on-screen output will include counts of chunks by state",
	}
}

func (EnvironmentVariable) AWSAccessKeyID() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AWS_ACCESS_KEY_ID",
		Description: "The AWS access key ID for S3 source used in service to service copy.",
	}
}

func (EnvironmentVariable) AWSSecretAccessKey() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AWS_SECRET_ACCESS_KEY",
		Description: "The AWS secret access key for S3 source used in service to service copy.",
		Hidden:      true,
	}
}

// AwsSessionToken is temporaily internally reserved, and not exposed to users.
func (EnvironmentVariable) AwsSessionToken() EnvironmentVariable {
	return EnvironmentVariable{Name: "AWS_SESSION_TOKEN"}
}

// OAuthTokenInfo is only used for internal integration.
func (EnvironmentVariable) OAuthTokenInfo() EnvironmentVariable {
	return EnvironmentVariable{Name: "AZCOPY_OAUTH_TOKEN_INFO"}
}

// CredentialType is only used for internal integration.
func (EnvironmentVariable) CredentialType() EnvironmentVariable {
	return EnvironmentVariable{Name: "AZCOPY_CRED_TYPE"}
}

func (EnvironmentVariable) DefaultServiceApiVersion() EnvironmentVariable {
	return EnvironmentVariable{
		Name:         "AZCOPY_DEFAULT_SERVICE_API_VERSION",
		DefaultValue: "2019-12-12",
		Description:  "Overrides the service API version so that AzCopy could accommodate custom environments such as Azure Stack.",
	}
}

func (EnvironmentVariable) UserAgentPrefix() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_USER_AGENT_PREFIX",
		Description: "Add a prefix to the default AzCopy User Agent, which is used for telemetry purposes. A space is automatically inserted.",
	}
}

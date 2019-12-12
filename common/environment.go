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
var VisibleEnvironmentVariables = []EnvironmentVariable{
	EEnvironmentVariable.ConcurrencyValue(),
	EEnvironmentVariable.TransferInitiationPoolSize(),
	EEnvironmentVariable.LogLocation(),
	EEnvironmentVariable.JobPlanLocation(),
	EEnvironmentVariable.BufferGB(),
	EEnvironmentVariable.AWSAccessKeyID(),
	EEnvironmentVariable.AWSSecretAccessKey(),
	EEnvironmentVariable.ShowPerfStates(),
	EEnvironmentVariable.PacePageBlobs(),
	EEnvironmentVariable.DefaultServiceApiVersion(),
	EEnvironmentVariable.ClientSecret(),
	EEnvironmentVariable.CertificatePassword(),
	EEnvironmentVariable.AutoTuneToCpu(),
	EEnvironmentVariable.CacheProxyLookup(),
	EEnvironmentVariable.UserAgentPrefix(),
}

var EEnvironmentVariable = EnvironmentVariable{}

func (EnvironmentVariable) UserDir() EnvironmentVariable {
	// Only used internally, not listed in the environment variables.
	return EnvironmentVariable{
		Name: IffString(runtime.GOOS == "windows", "USERPROFILE", "HOME"),
	}
}

func (EnvironmentVariable) ClientSecret() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_CLIENT_SECRET",
		Description: "The Azure Active Directory client secret used for Service Principal authentication",
		Hidden:      true,
	}
}

func (EnvironmentVariable) CertificatePassword() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_SPA_CERT_PASSWORD",
		Description: "The password used to decrypt the certificate used for Service Principal authentication.",
		Hidden:      true,
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
		DefaultValue: "2018-03-28",
		Description:  "Overrides the service API version so that AzCopy could accommodate custom environments such as Azure Stack.",
	}
}

func (EnvironmentVariable) UserAgentPrefix() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_USER_AGENT_PREFIX",
		Description: "Add a prefix to the default AzCopy User Agent, which is used for telemetry purposes. A space is automatically inserted.",
	}
}

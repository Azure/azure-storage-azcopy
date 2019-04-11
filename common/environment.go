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

type EnvironmentVariable struct {
	Name         string
	DefaultValue string
	Description  string
}

// This array needs to be updated when a new public environment variable is added
var VisibleEnvironmentVariables = []EnvironmentVariable{
	EEnvironmentVariable.ConcurrencyValue(),
	EEnvironmentVariable.LogLocation(),
	EEnvironmentVariable.AWSAccessKeyID(),
	EEnvironmentVariable.AWSSecretAccessKey(),
	EEnvironmentVariable.ShowPerfStates(),
	EEnvironmentVariable.PacePageBlobs(),
	EEnvironmentVariable.DefaultServiceApiVersion(),
}

var EEnvironmentVariable = EnvironmentVariable{}

func (EnvironmentVariable) ConcurrencyValue() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_CONCURRENCY_VALUE",
		Description: "Overrides how many Go Routines work on transfers. By default, this number is determined based on the number of logical cores on the machine.",
	}
}

func (EnvironmentVariable) LogLocation() EnvironmentVariable {
	return EnvironmentVariable{
		Name:        "AZCOPY_LOG_LOCATION",
		Description: "Overrides where the log files are stored, to avoid filling up a disk.",
	}
}

func (EnvironmentVariable) AccountName() EnvironmentVariable {
	return EnvironmentVariable{Name: "ACCOUNT_NAME"}
}

func (EnvironmentVariable) AccountKey() EnvironmentVariable {
	return EnvironmentVariable{Name: "ACCOUNT_KEY"}
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

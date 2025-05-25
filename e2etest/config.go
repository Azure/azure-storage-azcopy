// Copyright © Microsoft <wastore@microsoft.com>
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

package e2etest

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"

	"github.com/JeffreyRichter/enum/enum"
)

// clearly define all the inputs to the end-to-end tests
// it's ok to panic if the inputs are absolutely required
// the general guidance is to take in as few parameters as possible
type GlobalInputManager struct{}

func (GlobalInputManager) GetWorkloadIdentity() (tenantID string, clientID string, token string) {
	tenantID = os.Getenv("tenantId")
	if tenantID == "" {
		panic("tenantId must be specified to authenticate with workload identity")
	}
	clientID = os.Getenv("servicePrincipalId")
	if clientID == "" {
		panic("servicePrincipalId must be specified to authenticate with workload identity")
	}
	token = os.Getenv("idToken")
	if token == "" {
		panic("idToken must be specified to authenticate with workload identity")
	}
	return
}

func (GlobalInputManager) GetServicePrincipalAuth() (tenantID string, applicationID string, clientSecret string) {
	tenantID = os.Getenv("AZCOPY_E2E_TENANT_ID")
	applicationID = os.Getenv("AZCOPY_E2E_APPLICATION_ID")
	clientSecret = os.Getenv("AZCOPY_E2E_CLIENT_SECRET")

	if applicationID == "" || clientSecret == "" {
		panic("Insufficient information was supplied for service principal authentication")
	}

	return
}

func (GlobalInputManager) GetAccountAndKey(accountType AccountType) (string, string) {
	var name, key string

	switch accountType {
	case EAccountType.Standard():
		name = os.Getenv("AZCOPY_E2E_ACCOUNT_NAME")
		key = os.Getenv("AZCOPY_E2E_ACCOUNT_KEY")
	case EAccountType.HierarchicalNamespaceEnabled():
		name = os.Getenv("AZCOPY_E2E_ACCOUNT_NAME_HNS")
		key = os.Getenv("AZCOPY_E2E_ACCOUNT_KEY_HNS")
	case EAccountType.Classic():
		name = os.Getenv("AZCOPY_E2E_CLASSIC_ACCOUNT_NAME")
		key = os.Getenv("AZCOPY_E2E_CLASSIC_ACCOUNT_KEY")
	case EAccountType.Azurite():
		// Note: the key below is not a secret, this is the publicly documented Azurite key
		name = "devstoreaccount1"
		key = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
	default:
		panic("Only the standard account type is supported for the moment.")
	}

	if name == "" || key == "" {
		panic(fmt.Sprintf("Name and key for %s account must be set before running tests", accountType))
	}

	return name, key
}

func (GlobalInputManager) GetExecutablePath() string {
	path := os.Getenv("AZCOPY_E2E_EXECUTABLE_PATH")
	if path == "" {
		panic("Cannot test AzCopy if AZCOPY_E2E_EXECUTABLE_PATH is not provided")
	}

	return path
}

func (GlobalInputManager) KeepFailedData() bool {
	raw := os.Getenv("AZCOPY_E2E_KEEP_FAILED_DATA")
	if raw == "" {
		return false
	}

	result, err := strconv.ParseBool(raw)
	if err != nil {
		panic("If AZCOPY_E2E_KEEP_FAILED_DATA is set, it must be a boolean")
	}

	return result
}

func (GlobalInputManager) TestSummaryLogPath() string {
	return os.Getenv("AZCOPY_E2E_TEST_SUMMARY_LOG")
}

var EAccountType = AccountType(0)

type AccountType uint8

func (AccountType) Standard() AccountType                     { return AccountType(0) }
func (AccountType) PremiumBlockBlobs() AccountType            { return AccountType(1) }
func (AccountType) PremiumPageBlobs() AccountType             { return AccountType(8) }
func (AccountType) PremiumFileShares() AccountType            { return AccountType(9) }
func (AccountType) PremiumHNSEnabled() AccountType            { return AccountType(10) }
func (AccountType) HierarchicalNamespaceEnabled() AccountType { return AccountType(2) }
func (AccountType) Classic() AccountType                      { return AccountType(3) }
func (AccountType) StdManagedDisk() AccountType               { return AccountType(4) }
func (AccountType) OAuthManagedDisk() AccountType             { return AccountType(5) }
func (AccountType) S3() AccountType                           { return AccountType(6) } // Stub, for future testing use
func (AccountType) GCP() AccountType                          { return AccountType(7) } // Stub, for future testing use
func (AccountType) Azurite() AccountType                      { return AccountType(8) }
func (AccountType) ManagedDiskSnapshot() AccountType          { return AccountType(9) }
func (AccountType) ManagedDiskSnapshotOAuth() AccountType     { return AccountType(10) }
func (AccountType) LargeManagedDiskSnapshot() AccountType     { return AccountType(11) }
func (AccountType) LargeManagedDisk() AccountType             { return AccountType(12) }

func (o AccountType) String() string {
	return enum.StringInt(o, reflect.TypeOf(o))
}

func (o AccountType) IsManagedDisk() bool {
	return o == o.StdManagedDisk() || o == o.OAuthManagedDisk() || o == o.ManagedDiskSnapshot() || o == o.ManagedDiskSnapshotOAuth() ||
		o == o.LargeManagedDiskSnapshot() || o == o.LargeManagedDisk()
}

func (o AccountType) IsBlobOnly() bool {
	return o.IsManagedDisk() || o == o.HierarchicalNamespaceEnabled()
}

/*
{"SubscriptionID":"","ResourceGroupName":"","DiskName":""}
*/
type ManagedDiskConfig struct {
	SubscriptionID    string
	ResourceGroupName string
	DiskName          string

	oauth      AccessToken
	isSnapshot bool
}

var ClassicE2EOAuthCache *OAuthCache

func (gim GlobalInputManager) GetMDConfig(accountType AccountType) (*ManagedDiskConfig, error) {
	var mdConfigVar string
	var isSnapshot bool

	switch accountType {
	case EAccountType.StdManagedDisk():
		mdConfigVar = "AZCOPY_E2E_STD_MANAGED_DISK_CONFIG"
	case EAccountType.OAuthManagedDisk():
		mdConfigVar = "AZCOPY_E2E_OAUTH_MANAGED_DISK_CONFIG"
	case EAccountType.LargeManagedDisk():
		mdConfigVar = "AZCOPY_E2E_LARGE_MANAGED_DISK_CONFIG"
	case EAccountType.ManagedDiskSnapshot():
		mdConfigVar = "AZCOPY_E2E_STD_MANAGED_DISK_SNAPSHOT_CONFIG"
		isSnapshot = true
	case EAccountType.ManagedDiskSnapshotOAuth():
		mdConfigVar = "AZCOPY_E2E_OAUTH_MANAGED_DISK_SNAPSHOT_CONFIG"
		isSnapshot = true
	case EAccountType.LargeManagedDiskSnapshot():
		mdConfigVar = "AZCOPY_E2E_LARGE_MANAGED_DISK_SNAPSHOT_CONFIG"
		isSnapshot = true
	default:
		return nil, fmt.Errorf("account type %s is invalid for GetMDConfig", accountType.String())
	}

	conf := os.Getenv(mdConfigVar)
	if conf == "" {
		return nil, fmt.Errorf("config for env var %s was empty; empty config: {\"SubscriptionID\":\"\",\"ResourceGroupName\":\"\",\"DiskName\":\"\"}", mdConfigVar)
	}

	var out ManagedDiskConfig
	err := json.Unmarshal([]byte(conf), &out)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config") // Outputting the error may reveal semi-sensitive info like subscription ID
	}

	// Attach additional details to the config
	out.isSnapshot = isSnapshot
	err = gim.SetupClassicOAuthCache()
	if err != nil {
		return nil, fmt.Errorf("failed to setup OAuth cache: %w", err)
	}

	out.oauth, err = ClassicE2EOAuthCache.GetAccessToken(AzureManagementResource)
	if err != nil {
		return nil, fmt.Errorf("failed to get access token: %w", err)
	}

	return &out, nil
}

func (gim GlobalInputManager) SetupClassicOAuthCache() error {
	tenantId := os.Getenv("tenantId")

	cred, err := azidentity.NewDefaultAzureCredential(&azidentity.DefaultAzureCredentialOptions{TenantID: tenantId})
	if err != nil {
		return fmt.Errorf("failed to create credential: %w", err)
	}

	ClassicE2EOAuthCache = NewOAuthCache(cred, tenantId)

	return nil
}

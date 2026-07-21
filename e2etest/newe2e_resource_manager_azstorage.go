package e2etest

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	blobfscommon "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	datalakesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"
	blobfsservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"

	"strings"
)

func addWildCard(uri string, optList ...GetURIOptions) string {
	wildcard := FirstOrZero(optList).Wildcard
	if wildcard == "" {
		return uri
	}
	if strings.Contains(uri, "?") {
		uri = strings.Replace(uri, "?", wildcard+"?", 1)
	} else {
		uri += wildcard
	}

	return uri
}

type AzureAccountResourceManager struct {
	InternalAccountName string
	InternalAccountKey  string
	InternalAccountType AccountType

	TokenCredential azcore.TokenCredential
	ArmClient       *ARMStorageAccount
}

func (acct *AzureAccountResourceManager) ApplySAS(URI string, loc common.Location, optList ...GetURIOptions) string {
	if acct == nil {
		panic("Account must not be nil to generate a SAS token.")
	}
	opts := FirstOrZero(optList)

	if !opts.AzureOpts.WithSAS || acct.InternalAccountKey == "" {
		return URI
	}

	var sasVals GenericSignatureValues
	if opts.AzureOpts.SASValues == nil {
		// Default to account level SAS to cover all our bases
		sasVals = GenericAccountSignatureValues{}
	} else {
		sasVals = opts.AzureOpts.SASValues

	}

	switch loc {
	case common.ELocation.Blob():
		skc, err := blobservice.NewSharedKeyCredential(acct.InternalAccountName, acct.InternalAccountKey)
		common.PanicIfErr(err)

		p, err := sasVals.AsBlob().SignWithSharedKey(skc)
		common.PanicIfErr(err)

		parts, err := blobsas.ParseURL(URI)
		common.PanicIfErr(err)

		parts.SAS = p
		parts.Scheme = ternary.Iff(opts.RemoteOpts.Scheme != "", opts.RemoteOpts.Scheme, "https")
		return parts.String()
	case common.ELocation.File():
		skc, err := fileservice.NewSharedKeyCredential(acct.InternalAccountName, acct.InternalAccountKey)
		common.PanicIfErr(err)

		p, err := sasVals.AsFile().SignWithSharedKey(skc)
		common.PanicIfErr(err)

		parts, err := filesas.ParseURL(URI)
		common.PanicIfErr(err)

		parts.SAS = p
		parts.Scheme = ternary.Iff(opts.RemoteOpts.Scheme != "", opts.RemoteOpts.Scheme, "https")
		return parts.String()
	case common.ELocation.BlobFS():
		skc, err := blobfscommon.NewSharedKeyCredential(acct.InternalAccountName, acct.InternalAccountKey)
		common.PanicIfErr(err)

		p, err := sasVals.AsDatalake().SignWithSharedKey(skc)
		common.PanicIfErr(err)

		parts, err := datalakesas.ParseURL(URI)
		common.PanicIfErr(err)

		parts.SAS = p
		parts.Scheme = ternary.Iff(opts.RemoteOpts.Scheme != "", opts.RemoteOpts.Scheme, "https")
		return parts.String()
	default:
		panic("Unsupported location " + loc.String())
		return URI
	}
}

// ManagementClient returns the parent management client for this storage account.
// If this was created raw from key+name, this will return nil.
// If the account is a "modern" ARM storage account, ARMStorageAccount will be returned.
// If the account is a "classic" storage account, ARMClassicStorageAccount (not yet implemented) will be returned.
func (acct *AzureAccountResourceManager) ManagementClient() *ARMStorageAccount {
	return acct.ArmClient
}

func (acct *AzureAccountResourceManager) AccountName() string {
	return acct.InternalAccountName
}

func (acct *AzureAccountResourceManager) AccountType() AccountType {
	return acct.InternalAccountType
}

func (acct *AzureAccountResourceManager) AvailableServices() []common.Location {
	return []common.Location{
		common.ELocation.Blob(),
		common.ELocation.BlobFS(),
		common.ELocation.File(),
	}
}

func (acct *AzureAccountResourceManager) getServiceURL(a Asserter, service common.Location) string {
	switch service {
	case common.ELocation.Blob():
		return fmt.Sprintf("https://%s.blob.core.windows.net/", acct.InternalAccountName)
	case common.ELocation.File():
		return fmt.Sprintf("https://%s.file.core.windows.net/", acct.InternalAccountName)
	case common.ELocation.BlobFS():
		return fmt.Sprintf("https://%s.dfs.core.windows.net/", acct.InternalAccountName)
	default:
		a.Error(fmt.Sprintf("Service %s is not supported by this resource manager.", service))
		return ""
	}
}

func (acct *AzureAccountResourceManager) GetService(a Asserter, location common.Location) ServiceResourceManager {
	uri := acct.getServiceURL(a, location)

	switch location {
	case common.ELocation.Blob():
		var client *blobservice.Client
		var err error
		if acct.TokenCredential != nil {
			client, err = blobservice.NewClient(uri, acct.TokenCredential, nil)
		} else {
			sharedKey, skErr := blobservice.NewSharedKeyCredential(acct.InternalAccountName, acct.InternalAccountKey)
			a.NoError("Create shared key", skErr)
			client, err = blobservice.NewClientWithSharedKeyCredential(uri, sharedKey, nil)
		}
		a.NoError("Create Blob client", err)

		return &BlobServiceResourceManager{
			InternalAccount: acct,
			InternalClient:  client,
		}
	case common.ELocation.File():
		var client *fileservice.Client
		var err error
		if acct.TokenCredential != nil {
			client, err = fileservice.NewClient(uri, acct.TokenCredential, &fileservice.ClientOptions{AllowTrailingDot: to.Ptr(true), FileRequestIntent: to.Ptr(fileservice.ShareTokenIntentBackup)})
		} else {
			sharedKey, skErr := fileservice.NewSharedKeyCredential(acct.InternalAccountName, acct.InternalAccountKey)
			a.NoError("Create shared key", skErr)
			client, err = fileservice.NewClientWithSharedKeyCredential(uri, sharedKey, &fileservice.ClientOptions{AllowTrailingDot: to.Ptr(true), FileRequestIntent: to.Ptr(fileservice.ShareTokenIntentBackup)})
		}
		a.NoError("Create File client", err)

		return &FileServiceResourceManager{
			InternalAccount: acct,
			InternalClient:  client,
		}
	case common.ELocation.BlobFS():
		var client *blobfsservice.Client
		var err error
		if acct.TokenCredential != nil {
			client, err = blobfsservice.NewClient(uri, acct.TokenCredential, nil)
		} else {
			sharedKey, skErr := blobfscommon.NewSharedKeyCredential(acct.InternalAccountName, acct.InternalAccountKey)
			a.NoError("Create shared key", skErr)
			client, err = blobfsservice.NewClientWithSharedKeyCredential(uri, sharedKey, nil)
		}
		a.NoError("Create BlobFS client", err)

		return &BlobFSServiceResourceManager{
			InternalAccount: acct,
			InternalClient:  client,
		}
	default:
		return nil // GetServiceURL already covered the error
	}
}

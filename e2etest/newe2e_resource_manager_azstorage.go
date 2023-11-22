package e2etest

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	blobfscommon "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake"
	datalakeSAS "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"
	blobfsservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"time"
)

type AzureAccountResourceManager struct {
	accountName string
	accountKey  string
	accountType AccountType

	armClient *ARMStorageAccount
}

func (acct *AzureAccountResourceManager) ApplySAS(a Asserter, URI string, loc common.Location) string {
	a.AssertNow("account must not be nil", Not{IsNil{}}, acct)
	switch loc {
	case common.ELocation.Blob():
		skc, err := blob.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		a.NoError("Create shared key", err)

		p, err := blobsas.AccountSignatureValues{ // todo: currently this generates a "god SAS", granting all permissions. This is bad! Let's not do this in prod.
			Protocol:   blobsas.ProtocolHTTPS,
			StartTime:  time.Now().Add(-time.Hour),
			ExpiryTime: time.Now().Add(time.Hour * 24),
			Permissions: (&blobsas.AccountPermissions{
				Read:                  true,
				Write:                 true,
				Delete:                true,
				DeletePreviousVersion: true,
				PermanentDelete:       true,
				List:                  true,
				Add:                   true,
				Create:                true,
				Update:                true,
				Process:               true,
				FilterByTags:          true,
				Tag:                   true,
				SetImmutabilityPolicy: true,
			}).String(),
			ResourceTypes: (&blobsas.AccountResourceTypes{
				Service:   true,
				Container: true,
				Object:    true,
			}).String(),
		}.SignWithSharedKey(skc)
		a.NoError("Generate SAS token", err)

		parts, err := blobsas.ParseURL(URI)
		a.NoError("Parse URI", err)
		parts.SAS = p
		return parts.String()
	case common.ELocation.File():
		skc, err := fileservice.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		a.NoError("Create shared key", err)

		p, err := filesas.AccountSignatureValues{ // todo: currently this generates a "god SAS", granting all permissions. This is bad! Let's not do this in prod.
			Protocol:   filesas.ProtocolHTTPS,
			StartTime:  time.Now().Add(-time.Hour),
			ExpiryTime: time.Now().Add(time.Hour * 24),
			Permissions: (&filesas.AccountPermissions{
				Read:   true,
				Write:  true,
				Delete: true,
				List:   true,
				Create: true,
			}).String(),
			ResourceTypes: (&filesas.AccountResourceTypes{
				Service:   true,
				Container: true,
				Object:    true,
			}).String(),
		}.SignWithSharedKey(skc)
		a.NoError("Generate SAS token", err)

		parts, err := filesas.ParseURL(URI)
		a.NoError("Parse URI", err)
		parts.SAS = p
		return parts.String()
	case common.ELocation.BlobFS():
		skc, err := blobfscommon.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		a.NoError("Create shared key", err)

		p, err := datalakeSAS.AccountSignatureValues{ // todo: currently this generates a "god SAS", granting all permissions. This is bad! Let's not do this in prod.
			Protocol:   datalakeSAS.ProtocolHTTPS,
			StartTime:  time.Now().Add(-time.Hour),
			ExpiryTime: time.Now().Add(time.Hour * 24),
			Permissions: (&datalakeSAS.AccountPermissions{
				Read:                  true,
				Write:                 true,
				Delete:                true,
				DeletePreviousVersion: true,
				PermanentDelete:       true,
				List:                  true,
				Add:                   true,
				Create:                true,
				Update:                true,
				Process:               true,
				FilterByTags:          true,
				Tag:                   true,
				SetImmutabilityPolicy: true,
			}).String(),
			ResourceTypes: (&datalakeSAS.AccountResourceTypes{
				Service:   true,
				Container: true,
				Object:    true,
			}).String(),
		}.SignWithSharedKey(skc)
		a.NoError("Generate SAS token", err)

		parts, err := datalakeSAS.ParseURL(URI)
		a.NoError("Parse URI", err)
		parts.SAS = p
		return parts.String()
	default:
		a.Error(fmt.Sprintf("location %s unsupported", loc))
		return "" // won't reach
	}
}

// ManagementClient returns the parent management client for this storage account.
// If this was created raw from key+name, this will return nil.
// If the account is a "modern" ARM storage account, ARMStorageAccount will be returned.
// If the account is a "classic" storage account, ARMClassicStorageAccount (not yet implemented) will be returned.
func (acct *AzureAccountResourceManager) ManagementClient() *ARMStorageAccount {
	return acct.armClient
}

func (acct *AzureAccountResourceManager) AccountName() string {
	return acct.accountName
}

func (acct *AzureAccountResourceManager) AccountType() AccountType {
	return acct.accountType
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
		return fmt.Sprintf("https://%s.blob.core.windows.net/", acct.accountName)
	case common.ELocation.File():
		return fmt.Sprintf("https://%s.file.core.windows.net/", acct.accountName)
	case common.ELocation.BlobFS():
		return fmt.Sprintf("https://%s.dfs.core.windows.net/", acct.accountName)
	default:
		a.Error(fmt.Sprintf("Service %s is not supported by this resource manager.", service))
		return ""
	}
}

func (acct *AzureAccountResourceManager) GetService(a Asserter, location common.Location) ServiceResourceManager {
	uri := acct.getServiceURL(a, location)

	switch location {
	case common.ELocation.Blob():
		sharedKey, err := blobservice.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		a.NoError("Create shared key", err)
		client, err := blobservice.NewClientWithSharedKeyCredential(uri, sharedKey, nil)
		a.NoError("Create Blob client", err)

		return &BlobServiceResourceManager{
			internalAccount: acct,
			internalClient:  client,
		}
	case common.ELocation.File():
		sharedKey, err := fileservice.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		a.NoError("Create shared key", err)
		client, err := fileservice.NewClientWithSharedKeyCredential(uri, sharedKey, nil)
		a.NoError("Create File client", err)

		return &FileServiceResourceManager{
			internalAccount: acct,
			internalClient:  client,
		}
	case common.ELocation.BlobFS():
		sharedKey, err := blobfscommon.NewSharedKeyCredential(acct.accountName, acct.accountKey)
		client, err := blobfsservice.NewClientWithSharedKeyCredential(uri, sharedKey, nil)
		a.NoError("Create BlobFS client", err)

		return &BlobFSServiceResourceManager{
			internalAccount: acct,
			internalClient:  client,
		}
	default:
		return nil // GetServiceURL already covered the error
	}
}

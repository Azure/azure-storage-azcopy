package main

import (
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	blobfsservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
)

// OAuthAccountResourceManager is jank incarnate, this isn't meant to be here long. if you're feeling the growing pains of it,
// maybe looking at merging it into the new e2e tests, it's time to burn the building down.
type OAuthAccountResourceManager struct {
	serviceClientOptions azcore.ClientOptions
	cred                 azcore.TokenCredential
	accountName          string
}

func (a *OAuthAccountResourceManager) AccountName() string {
	return a.accountName
}

func (a *OAuthAccountResourceManager) AccountType() e2etest.AccountType {
	return e2etest.EAccountType.Standard() // this gets unused for the most part, it's ok if we return a possibly false value. it isn't used for most logic.
}

func (a *OAuthAccountResourceManager) AvailableServices() []common.Location {
	return []common.Location{
		common.ELocation.Blob(),
		common.ELocation.BlobFS(),
		common.ELocation.File(),
	}
}

func (a *OAuthAccountResourceManager) GetService(asserter e2etest.Asserter, location common.Location) e2etest.ServiceResourceManager {
	var serviceSuffix = map[common.Location]string{
		common.ELocation.Blob():   ".blob.core.windows.net",
		common.ELocation.File():   ".file.core.windows.net",
		common.ELocation.BlobFS(): ".dfs.core.windows.net",
	}

	uri := fmt.Sprint("https://%s%s/", a.accountName, serviceSuffix)

	var out e2etest.ServiceResourceManager
	switch location {
	case common.ELocation.Blob():
		c, err := blobservice.NewClient(uri, a.cred, &blobservice.ClientOptions{
			ClientOptions: a.serviceClientOptions,
		})
		if err != nil {
			asserter.NoError("create blob service", err)
			return nil
		}

		out = &e2etest.BlobServiceResourceManager{
			InternalAccount: nil,
			InternalClient:  c,
		}
	case common.ELocation.File():
		c, err := fileservice.NewClient(uri, a.cred, &fileservice.ClientOptions{
			ClientOptions: a.serviceClientOptions,
		})
		if err != nil {
			asserter.NoError("create file service", err)
			return nil
		}

		out = &e2etest.FileServiceResourceManager{
			InternalAccount: nil,
			InternalClient:  c,
		}
	case common.ELocation.BlobFS():
		c, err := blobfsservice.NewClient(uri, a.cred, &blobfsservice.ClientOptions{
			ClientOptions: a.serviceClientOptions,
		})
		if err != nil {
			asserter.NoError("create blob service", err)
			return nil
		}

		out = &e2etest.BlobFSServiceResourceManager{
			InternalAccount: nil,
			InternalClient:  c,
		}
	case common.ELocation.FileNFS():
		c, err := fileservice.NewClient(uri, a.cred, &fileservice.ClientOptions{
			ClientOptions: a.serviceClientOptions,
		})
		if err != nil {
			asserter.NoError("create file service", err)
			return nil
		}

		out = &e2etest.FileServiceResourceManager{
			InternalAccount: nil,
			InternalClient:  c,
			Llocation:       common.ELocation.FileNFS(),
		}
	default:
		asserter.Error("Invalid service specified: " + location.String())
	}
	return out
}

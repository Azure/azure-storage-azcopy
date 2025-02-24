package main

import (
	"errors"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	blobservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/service"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	fileservice "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/e2etest"
	"reflect"
	"strings"
)

type GeneratorConfig struct {
	AuthInfo struct {
		AuthSource struct {
			PSInherit  bool   `env:"PS_INHERIT,required"`
			CLIInherit bool   `env:"CLI_INHERIT,required"`
			AcctKey    string `env:"ACCT_KEY,required"`
		} `env:",required,mutually_exclusive"`
		TenantID string `env:"TENANT_ID,required"`
	}
}

func GetResourceManagerForURI(uri string) (e2etest.ServiceResourceManager, error) {
	switch {
	case strings.Contains(uri, "dfs.core.windows.net"):
		strings.ReplaceAll(uri, "dfs.core.windows.net", "blob.core.windows.net")
		fallthrough
	case strings.Contains(uri, "blob.core.windows.net"):
		blobUriParts, err := blobsas.ParseURL(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse uri: %w", err)
		} else if blobUriParts.ContainerName != "" || blobUriParts.BlobName != "" {

		}

		var client *blobservice.Client
		if acctKey := genConfig.AuthInfo.AuthSource.AcctKey; acctKey != "" {
			acctName := strings.TrimSuffix(blobUriParts.Host, ".blob.core.windows.net")
			var skey *blobservice.SharedKeyCredential
			skey, err = blobservice.NewSharedKeyCredential(acctName, acctKey)
			if err != nil {
				return nil, fmt.Errorf("failed to parse shared key: %w", err)
			}

			client, err = blobservice.NewClientWithSharedKeyCredential(uri, skey, nil)
		} else {
			var tCred azcore.TokenCredential
			tCred, err = genConfig.GetTokenCredential()
			if err != nil {
				return nil, err
			}

			client, err = blobservice.NewClient(uri, tCred, nil)
		}

		if err != nil {
			return nil, fmt.Errorf("failed to create client: %w", err)
		}

		return &e2etest.BlobServiceResourceManager{
			InternalClient: client,
		}, nil
	case strings.Contains(uri, "file.core.windows.net"):
		fUrlParts, err := filesas.ParseURL(uri)
		if err != nil {
			return nil, fmt.Errorf("failed to parse uri: %w", err)
		} else if fUrlParts.ShareName != "" || fUrlParts.DirectoryOrFilePath != "" {
			return nil, fmt.Errorf("target must be a service")
		}

		var client *fileservice.Client
		if acctKey := genConfig.AuthInfo.AuthSource.AcctKey; acctKey != "" {
			acctName := strings.TrimSuffix(fUrlParts.Host, ".file.core.windows.net")
			var sKey *fileservice.SharedKeyCredential
			sKey, err = fileservice.NewSharedKeyCredential(acctName, acctKey)
			if err != nil {
				return nil, fmt.Errorf("failed to parse shared key: %w", err)
			}

			client, err = fileservice.NewClientWithSharedKeyCredential(uri, sKey, nil)
		} else {
			var tCred azcore.TokenCredential
			tCred, err = genConfig.GetTokenCredential()
			if err != nil {
				return nil, fmt.Errorf("failed to get token cred: %w", err)
			}

			client, err = fileservice.NewClient(uri, tCred, nil)
		}
		if err != nil {
			return nil, fmt.Errorf("failed to create share client: %w", err)
		}

		return &e2etest.FileServiceResourceManager{
			InternalClient: client,
		}, nil
	default:
		return nil, errors.New("target URI did not match file or blob scheme")
	}
}

func (g GeneratorConfig) GetTokenCredential() (azcore.TokenCredential, error) {
	ai := g.AuthInfo
	switch {
	case ai.AuthSource.CLIInherit:
		ret, err := azidentity.NewAzureCLICredential(&azidentity.AzureCLICredentialOptions{
			TenantID: ai.TenantID,
		})

		return ret, err
	case ai.AuthSource.PSInherit:
		ret, err := common.NewPowershellContextCredential(&common.PowershellContextCredentialOptions{
			TenantID: ai.TenantID,
		})

		return ret, err
	default:
		return nil, errors.New("invalid authSource")
	}
}

var genConfig GeneratorConfig

func init() {
	err := e2etest.ReadConfig(reflect.ValueOf(&genConfig).Elem(), "GeneratorConfig", e2etest.EnvTag{Required: true})
	if err != nil {
		panic("failed reading config: " + err.Error())
	}
}

package cmd

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/buildmode"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// GetCredentialManager is a variable for two reasons:
// 1) Mover may want to replace it to inject credentials of their own
// 2) To create a closure to hide "global" variables
var GetCredentialManager = func() func() cred.Manager {
	// Contain the "globals" within a closure so that we don't have access to them in the outside world.
	var (
		credManagerInstance cred.Manager
		credManagerOnce     sync.Once
	)

	return func() cred.Manager {
		credManagerOnce.Do(func() {
			if common.AzcopyJobPlanFolder == "" {
				panic("invalid state, AzcopyJobPlanFolder should not be an empty string")
			}

			keyrings := make([]cred.Keyring, 1, 4)
			keyrings[0] = cred.NewAutoLoginKeyring()

			if integration, err := cred.GetIntegrationKeyring(); err != nil {
				glcm.Warn(fmt.Sprintf("could not get integration keyring: %s", err))
			} else {
				keyrings = append(keyrings, integration)
			}
			if env, err := cred.GetEnvironmentKeyring(); err != nil {
				glcm.Warn(fmt.Sprintf("could not get environment keyring: %s", err))
			} else {
				keyrings = append(keyrings, env)
			}
			if osKeyring, err := cred.GetOSKeyring(cred.GetOSKeyringOptions{}); err != nil {
				glcm.Warn(fmt.Sprintf("could not get OS keyring: %s", err))
			} else {
				keyrings = append(keyrings, osKeyring)
			}

			credManagerInstance = cred.NewManager(keyrings...)
		})
		return credManagerInstance
	}
}()

type getTargetCredInfoOptions struct {
	ctx context.Context

	canBePublic      bool
	sharedKeyAllowed bool

	preferredTokenName string

	cpkOptions   common.CpkOptions
	tokenManager cred.Manager
}

type credInfoOptions struct {
	tokenCredential  azcore.TokenCredential
	s3CredentialInfo cred.S3CredentialInfo
}

func credInfo(credType enum.CredentialType, opts ...credInfoOptions) cred.CredentialInfo {
	info := cred.CredentialInfo{CredentialType: credType}
	if len(opts) > 0 {
		info.TokenCredential = opts[0].tokenCredential
		info.S3CredentialInfo = opts[0].s3CredentialInfo
	}
	return info
}

func getTargetCredInfo(resourceString common.ResourceString, location common.Location, opts getTargetCredInfoOptions) (cred.CredentialInfo, error) {
	if forced := GetCredTypeFromEnvVar(); forced != enum.ECredentialType.Unknown() &&
		location != common.ELocation.S3() && location != common.ELocation.GCP() {
		return credInfo(forced), nil
	}

	if opts.ctx == nil {
		opts.ctx = context.TODO()
	}

	switch location {
	case common.ELocation.Blob():
		return getBlobCredInfo(resourceString, opts)
	case common.ELocation.BlobFS():
		return getBlobFSCredInfo(resourceString, opts)
	case common.ELocation.File():
		return getFileCredInfo(resourceString, opts)
	case common.ELocation.S3():
		return getS3CredInfo()
	case common.ELocation.GCP():
		return getGCPCredInfo()
	case common.ELocation.Local(), common.ELocation.Benchmark(), common.ELocation.None(), common.ELocation.Pipe():
		return getLocalCredInfo()
	}

	return cred.CredentialInfo{}, errors.New("unknown location: " + location.String())
}

func getBlobCredInfo(resourceString common.ResourceString, opts getTargetCredInfoOptions) (cred.CredentialInfo, error) {
	return getBlobBasedCredInfo(resourceString, common.ELocation.Blob(), opts)
}

func getBlobFSCredInfo(resourceString common.ResourceString, opts getTargetCredInfoOptions) (cred.CredentialInfo, error) {
	return getBlobBasedCredInfo(resourceString, common.ELocation.BlobFS(), opts)
}

func getBlobBasedCredInfo(resourceString common.ResourceString, location common.Location, opts getTargetCredInfoOptions) (cred.CredentialInfo, error) {
	uri, _ := resourceString.FullURL()
	// normal accounts can't be prefixed like this (at least under normal blob endpoints!)
	// and it isn't allowed for storage accounts to have this naming scheme typically, anywho.
	// if someone is developing a service against an emulator or whatnot, and naming storage accounts this way, they are footgunning.
	isMdAccount := strings.HasPrefix(uri.Host, "md-")

	// Managed disk requires SAS bare minimum. No SAS, no managed disk.
	if isMdAccount && resourceString.SAS == "" {
		return credInfo(enum.ECredentialType.Unknown()), nil
	}

	// Handle all managed disk cases, to become DRY.
	if isMdAccount && mdAccountNeedsOAuth(opts.ctx, uri.String(), opts.cpkOptions) {
		if opts.tokenManager == nil {
			return cred.CredentialInfo{}, common.NewAzError(common.EAzError.LoginCredMissing(), "No SAS token or OAuth token is present and the resource is not public")
		}
		if _, err := opts.tokenManager.GetCredentials(opts.preferredTokenName); err != nil {
			return cred.CredentialInfo{}, common.NewAzError(common.EAzError.LoginCredMissing(), "No SAS token or OAuth token is present and the resource is not public")
		}
		return credInfo(enum.ECredentialType.MDOAuthToken()), nil
	} else if isMdAccount {
		//
		return credInfo(enum.ECredentialType.Anonymous()), nil
	}

	// Managed disk, if it has a SAS, isn't always *just* SAS. it could need OAuth too.
	if resourceString.SAS != "" {
		return credInfo(enum.ECredentialType.Anonymous()), nil
	}

	// Test public access, if it's an option...
	if opts.canBePublic {
		if isPublic(opts.ctx, uri.String(), opts.cpkOptions) {
			return credInfo(enum.ECredentialType.Anonymous()), nil
		}
	}

	// If we have a token manager, see if we can fetch the token. If we can, we know what we're using!
	if opts.tokenManager != nil {
		if tc, err := opts.tokenManager.GetCredentials(opts.preferredTokenName); err == nil {
			return credInfo(enum.ECredentialType.OAuthToken(), credInfoOptions{tokenCredential: tc}), nil
		}
	}

	// BlobFS currently supports Shared key. Remove this piece of code once we deprecate that.
	if opts.sharedKeyAllowed && location == common.ELocation.BlobFS() {
		name := enum.EEnvironmentVariable.AccountName().Get()
		key := enum.EEnvironmentVariable.AccountKey().Get()
		if name != "" && key != "" {
			warnIfSharedKeyAuthForDatalake()
			return credInfo(enum.ECredentialType.SharedKey()), nil
		}
	}

	return credInfo(enum.ECredentialType.Unknown()), nil
}

func getFileCredInfo(resourceString common.ResourceString, opts getTargetCredInfoOptions) (cred.CredentialInfo, error) {
	// Short-circuit for SAS
	if resourceString.SAS != "" {
		return credInfo(enum.ECredentialType.Anonymous()), nil
	}

	// Try to fetch OAuth if we can.
	if opts.tokenManager != nil {
		if _, err := opts.tokenManager.GetCredentials(opts.preferredTokenName); err == nil {
			return credInfo(enum.ECredentialType.OAuthToken()), nil
		}
	}

	return credInfo(enum.ECredentialType.Unknown()), nil
}

func getS3CredInfo() (cred.CredentialInfo, error) {
	if !buildmode.IsMover {
		accessKeyID := enum.EEnvironmentVariable.AWSAccessKeyID().Get()
		secretAccessKey := enum.EEnvironmentVariable.AWSSecretAccessKey().Get()
		if accessKeyID == "" || secretAccessKey == "" {
			return credInfo(enum.ECredentialType.S3PublicBucket()), nil
		}
	}

	return credInfo(enum.ECredentialType.S3AccessKey()), nil
}

func getGCPCredInfo() (cred.CredentialInfo, error) {
	googleAppCredentials := enum.EEnvironmentVariable.GoogleAppCredentials().Get()
	if googleAppCredentials == "" {
		return cred.CredentialInfo{}, errors.New("GOOGLE_APPLICATION_CREDENTIALS environment variable must be set before using GCP transfer feature")
	}
	return credInfo(enum.ECredentialType.GoogleAppCredentials()), nil
}

func getLocalCredInfo() (cred.CredentialInfo, error) {
	return credInfo(enum.ECredentialType.Anonymous()), nil
}

package azcopy

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

type remoteProvider struct {
	srcServiceClient *common.ServiceClient
	srcCredType      common.CredentialType
	dstServiceClient *common.ServiceClient
	dstCredType      common.CredentialType
}

func NewSyncRemoteProvider(ctx context.Context, uotm *common.UserOAuthTokenManager, src, dst common.ResourceString, fromTo common.FromTo, cpkOptions common.CpkOptions, trailingDot common.TrailingDotOption) (rp *remoteProvider, err error) {
	rp = &remoteProvider{}

	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	rp.srcServiceClient, rp.srcCredType, err = GetSourceServiceClient(ctx, src, fromTo.From(), trailingDot, cpkOptions, uotm)
	if err != nil {
		return rp, err
	}
	if fromTo.IsS2S() && rp.srcCredType != common.ECredentialType.Anonymous() {
		if rp.srcCredType.IsAzureOAuth() && fromTo.To().CanForwardOAuthTokens() {
			// no-op, this is OK
		} else if rp.srcCredType == common.ECredentialType.GoogleAppCredentials() || rp.srcCredType == common.ECredentialType.S3AccessKey() || rp.srcCredType == common.ECredentialType.S3PublicBucket() {
			// this too, is OK
		} else if rp.srcCredType == common.ECredentialType.Anonymous() {
			// this is OK
		} else {
			return rp, fmt.Errorf("the source of a %s->%s sync must either be public, or authorized with a SAS token; blob destinations can forward OAuth", fromTo.From(), fromTo.To())
		}
	}
	rp.dstServiceClient, rp.dstCredType, err = GetDestinationServiceClient(ctx, dst, fromTo, rp.srcCredType, trailingDot, cpkOptions, uotm)
	if err != nil {
		return rp, err
	}

	// Check protocol compatibility for File Shares
	if err := ValidateProtocolCompatibility(ctx, fromTo, src, dst, rp.srcServiceClient, rp.dstServiceClient); err != nil {
		return nil, err
	}

	return rp, nil
}

func NewCopyRemoteProvider(ctx context.Context, uotm *common.UserOAuthTokenManager, src, dst common.ResourceString, fromTo common.FromTo, cpkOptions common.CpkOptions, trailingDot common.TrailingDotOption) (rp *remoteProvider, err error) {
	rp = &remoteProvider{}

	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	rp.srcServiceClient, rp.srcCredType, err = GetSourceServiceClient(ctx, src, fromTo.From(), trailingDot, cpkOptions, uotm)
	if err != nil {
		return rp, err
	}

	rp.dstServiceClient, rp.dstCredType, err = GetDestinationServiceClient(ctx, dst, fromTo, rp.srcCredType, trailingDot, cpkOptions, uotm)
	if err != nil {
		return rp, err
	}

	// Check protocol compatibility for File Shares
	if err := ValidateProtocolCompatibility(ctx, fromTo, src, dst, rp.srcServiceClient, rp.dstServiceClient); err != nil {
		return nil, err
	}

	return rp, nil
}

func GetSourceServiceClient(ctx context.Context,
	source common.ResourceString,
	loc common.Location,
	trailingDot common.TrailingDotOption,
	cpk common.CpkOptions,
	uotm *common.UserOAuthTokenManager) (*common.ServiceClient, common.CredentialType, error) {
	srcCredType, _, err := GetCredentialTypeForLocation(ctx,
		loc,
		source,
		true,
		uotm,
		cpk)
	if err != nil {
		return nil, srcCredType, err
	}
	var tc azcore.TokenCredential
	if srcCredType.IsAzureOAuth() {
		// Get token from env var or cache.
		tokenInfo, err := uotm.GetTokenInfo(ctx)
		if err != nil {
			return nil, srcCredType, err
		}

		tc, err = tokenInfo.GetTokenCredential()
		if err != nil {
			return nil, srcCredType, err
		}
	}

	var srcReauthTok *common.ScopedAuthenticator
	if at, ok := tc.(common.AuthenticateToken); ok { // We don't need two different tokens here since it gets passed in just the same either way.
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		srcReauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	options := traverser.CreateClientOptions(common.AzcopyCurrentJobLogger, nil, srcReauthTok)

	// Create Source Client.
	var azureFileSpecificOptions any
	if loc == common.ELocation.File() || loc == common.ELocation.FileNFS() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot: trailingDot == common.ETrailingDotOption.Enable(),
		}
	}

	srcServiceClient, err := common.GetServiceClientForLocation(
		loc,
		source,
		srcCredType,
		tc,
		&options,
		azureFileSpecificOptions,
	)
	if err != nil {
		return nil, srcCredType, err
	}
	return srcServiceClient, srcCredType, nil
}

func GetDestinationServiceClient(ctx context.Context,
	destination common.ResourceString,
	fromTo common.FromTo,
	srcCredType common.CredentialType,
	trailingDot common.TrailingDotOption,
	cpk common.CpkOptions,
	uotm *common.UserOAuthTokenManager) (*common.ServiceClient, common.CredentialType, error) {
	dstCredType, _, err := GetCredentialTypeForLocation(ctx,
		fromTo.To(),
		destination,
		false,
		uotm,
		cpk)
	if err != nil {
		return nil, dstCredType, err
	}
	var tc azcore.TokenCredential
	if dstCredType.IsAzureOAuth() {
		// Get token from env var or cache.
		tokenInfo, err := uotm.GetTokenInfo(ctx)
		if err != nil {
			return nil, dstCredType, err
		}

		tc, err = tokenInfo.GetTokenCredential()
		if err != nil {
			return nil, dstCredType, err
		}
	}

	var dstReauthTok *common.ScopedAuthenticator
	if at, ok := tc.(common.AuthenticateToken); ok { // We don't need two different tokens here since it gets passed in just the same either way.
		// This will cause a reauth with StorageScope, which is fine, that's the original Authenticate call as it stands.
		dstReauthTok = (*common.ScopedAuthenticator)(common.NewScopedCredential(at, common.ECredentialType.OAuthToken()))
	}

	var srcTokenCred *common.ScopedToken
	if fromTo.IsS2S() && srcCredType.IsAzureOAuth() {
		// Get token from env var or cache.
		srcTokenInfo, err := uotm.GetTokenInfo(ctx)
		if err != nil {
			return nil, dstCredType, err
		}

		sourceTc, err := srcTokenInfo.GetTokenCredential()
		if err != nil {
			return nil, dstCredType, err
		}
		srcTokenCred = common.NewScopedCredential(sourceTc, srcCredType)
	}

	options := traverser.CreateClientOptions(common.AzcopyCurrentJobLogger, srcTokenCred, dstReauthTok)

	// Create Destination Client.
	var azureFileSpecificOptions any
	if fromTo.To() == common.ELocation.File() || fromTo.To() == common.ELocation.FileNFS() {
		azureFileSpecificOptions = &common.FileClientOptions{
			AllowTrailingDot:       trailingDot == common.ETrailingDotOption.Enable(),
			AllowSourceTrailingDot: trailingDot == common.ETrailingDotOption.Enable() && fromTo.To() == common.ELocation.File(),
		}
	}

	dstServiceClient, err := common.GetServiceClientForLocation(
		fromTo.To(),
		destination,
		dstCredType,
		tc,
		&options,
		azureFileSpecificOptions,
	)
	if err != nil {
		return nil, dstCredType, err
	}
	return dstServiceClient, dstCredType, nil
}

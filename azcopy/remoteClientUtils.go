package azcopy

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

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

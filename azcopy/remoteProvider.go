package azcopy

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/common/cred"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/ste"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type remoteProvider struct {
	srcServiceClient     *common.ServiceClient
	srcCredInfo          cred.CredentialInfo
	dstCredInfo          cred.CredentialInfo
	srcCredType          enum.CredentialType
	dstServiceClient     *common.ServiceClient
	dstCredType          enum.CredentialType
	srcTokenCredential   azcore.TokenCredential
	dstTokenCredential   azcore.TokenCredential
	s3CredentialProvider credentials.Provider
}

func newCopyRemoteProvider(ctx context.Context, manager cred.Manager, src, dst common.ResourceString, fromTo common.FromTo, cpkOptions common.CpkOptions, trailingDot common.TrailingDotOption, credentialNames ...string) (*remoteProvider, error) {
	return newRemoteProvider(ctx, manager, src, dst, fromTo, cpkOptions, trailingDot, credentialNames...)
}

func newSyncRemoteProvider(ctx context.Context, manager cred.Manager, src, dst common.ResourceString, fromTo common.FromTo, cpkOptions common.CpkOptions, trailingDot common.TrailingDotOption, credentialNames ...string) (*remoteProvider, error) {
	return newRemoteProvider(ctx, manager, src, dst, fromTo, cpkOptions, trailingDot, credentialNames...)
}

func newRemoteProvider(ctx context.Context, manager cred.Manager, src, dst common.ResourceString, fromTo common.FromTo, cpkOptions common.CpkOptions, trailingDot common.TrailingDotOption, credentialNames ...string) (*remoteProvider, error) {
	ctx = context.WithValue(ctx, ste.ServiceAPIVersionOverride, ste.DefaultServiceApiVersion)
	var sourceName, destinationName string
	if len(credentialNames) > 0 {
		sourceName = credentialNames[0]
	}
	if len(credentialNames) > 1 {
		destinationName = credentialNames[1]
	}
	sourceCred, err := GetTargetCredInfo(src, fromTo.From(), GetTargetCredInfoOptions{
		Context: ctx, CanBePublic: true, SharedKeyAllowed: !fromTo.IsS2S(),
		PreferredTokenName: sourceName, CpkOptions: cpkOptions, TokenManager: manager,
	})
	if err != nil {
		return nil, fmt.Errorf("source authorization failed: %w", err)
	}
	if provider, ok := ctx.Value("customS3Creds").(credentials.Provider); ok && fromTo.From() == common.ELocation.S3() {
		sourceCred.CredentialType = enum.ECredentialType.S3AccessKey()
		sourceCred.S3CredentialInfo.Provider = provider
	}
	destinationCred, err := GetTargetCredInfo(dst, fromTo.To(), GetTargetCredInfoOptions{
		Context: ctx, SharedKeyAllowed: !fromTo.IsS2S(),
		PreferredTokenName: destinationName, TokenManager: manager,
	})
	if err != nil {
		return nil, fmt.Errorf("destination authorization failed: %w", err)
	}
	if fromTo.IsS2S() && sourceCred.CredentialType.IsAzureOAuth() && !fromTo.To().CanForwardOAuthTokens() {
		return nil, errors.New("the destination cannot forward source OAuth tokens; authorize the source with a SAS token")
	}
	if fromTo.IsS2S() && (sourceCred.CredentialType == enum.ECredentialType.SharedKey() || destinationCred.CredentialType == enum.ECredentialType.SharedKey()) {
		return nil, errors.New("shared key auth is not supported for S2S operations")
	}
	for _, target := range []struct {
		resource common.ResourceString
		location common.Location
		info     cred.CredentialInfo
	}{{src, fromTo.From(), sourceCred}, {dst, fromTo.To(), destinationCred}} {
		if target.info.CredentialType == enum.ECredentialType.Unknown() && target.location.IsRemote() {
			return nil, fmt.Errorf("no credential is available for %s", target.location)
		}
		if err := CheckAuthSafeForTarget(target.info.CredentialType, target.resource.Value, TrustedSuffixes, target.location); err != nil {
			return nil, err
		}
	}
	rp := &remoteProvider{
		srcCredType: sourceCred.CredentialType, dstCredType: destinationCred.CredentialType,
		srcCredInfo: sourceCred, dstCredInfo: destinationCred,
		srcTokenCredential: sourceCred.TokenCredential, dstTokenCredential: destinationCred.TokenCredential,
		s3CredentialProvider: sourceCred.S3CredentialInfo.Provider,
	}
	var sourceOptions any
	if fromTo.From().IsFile() {
		sourceOptions = &common.FileClientOptions{AllowTrailingDot: trailingDot.IsEnabled()}
	}
	sourceClientOptions := CreateClientOptions(common.AzcopyCurrentJobLogger, nil, sourceCred.TokenCredential)
	if fromTo.To() == common.ELocation.Pipe() {
		sourceClientOptions = CreateClientOptions(common.AzcopyCurrentJobLogger, nil, nil)
	}
	if fromTo.From().IsRemote() {
		rp.srcServiceClient, err = common.GetServiceClientForLocation(fromTo.From(), src, sourceCred.CredentialType, sourceCred.TokenCredential, &sourceClientOptions, sourceOptions)
		if err != nil {
			return nil, err
		}
	}
	var destinationOptions any
	if fromTo.To().IsFile() {
		destinationOptions = &common.FileClientOptions{
			AllowTrailingDot:       trailingDot.IsEnabled(),
			AllowSourceTrailingDot: trailingDot.IsEnabled() && fromTo.From().IsFile(),
		}
	}
	var forwardedSource azcore.TokenCredential
	if fromTo.IsS2S() && sourceCred.CredentialType.IsAzureOAuth() {
		forwardedSource = sourceCred.TokenCredential
	}
	destinationClientOptions := CreateClientOptions(common.AzcopyCurrentJobLogger, forwardedSource, destinationCred.TokenCredential)
	if fromTo.To().IsRemote() {
		rp.dstServiceClient, err = common.GetServiceClientForLocation(fromTo.To(), dst, destinationCred.CredentialType, destinationCred.TokenCredential, &destinationClientOptions, destinationOptions)
		if err != nil {
			return nil, err
		}
	}
	if err := ValidateProtocolCompatibility(ctx, fromTo, src, dst, rp.srcServiceClient, rp.dstServiceClient); err != nil {
		return nil, err
	}
	return rp, nil
}

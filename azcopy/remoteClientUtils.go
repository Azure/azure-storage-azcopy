// Copyright © 2025 Microsoft <wastore@microsoft.com>
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
		cpk,
		uotm)
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
		cpk,
		uotm)
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

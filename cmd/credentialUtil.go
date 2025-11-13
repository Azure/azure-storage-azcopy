// Copyright © 2017 Microsoft <wastore@microsoft.com>
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

// This file contains credential utils used only in cmd module.

package cmd

import (
	"context"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/azcopy"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type rawFromToInfo struct {
	fromTo              common.FromTo
	source, destination common.ResourceString
}

func GetCredentialInfoForLocation(ctx context.Context, location common.Location, resource common.ResourceString, isSource bool, cpkOptions common.CpkOptions) (credInfo common.CredentialInfo, isPublic bool, err error) {

	// get the type
	credInfo.CredentialType, isPublic, err = azcopy.GetCredentialTypeForLocation(ctx, location, resource, isSource, cpkOptions, Client.GetUserOAuthTokenManagerInstance())

	if err != nil {
		glcm.Error(err.Error())
	}

	// flesh out the rest of the fields, for those types that require it
	if credInfo.CredentialType.IsAzureOAuth() {
		uotm := Client.GetUserOAuthTokenManagerInstance()

		if tokenInfo, err := uotm.GetTokenInfo(ctx); err != nil {
			return credInfo, false, err
		} else {
			credInfo.OAuthTokenInfo = *tokenInfo
		}
	}

	return
}

// getCredentialType checks user provided info, and gets the proper credential type
// for current command.
// TODO: consider replace with calls to getCredentialInfoForLocation
// (right now, we have tweaked this to be a wrapper for that function, but really should remove this one totally)
func getCredentialType(ctx context.Context, raw rawFromToInfo, cpkOptions common.CpkOptions) (credType common.CredentialType, err error) {

	uotm := Client.GetUserOAuthTokenManagerInstance()
	switch {
	case raw.fromTo.To().IsRemote():
		// we authenticate to the destination. Source is assumed to be SAS, or public, or a local resource
		credType, _, err = azcopy.GetCredentialTypeForLocation(ctx, raw.fromTo.To(), raw.destination, false, common.CpkOptions{}, uotm)
	case raw.fromTo == common.EFromTo.BlobTrash() ||
		raw.fromTo == common.EFromTo.BlobFSTrash() ||
		raw.fromTo == common.EFromTo.FileTrash():
		// For to Trash direction, use source as resource URL
		// Also, by setting isSource=false we inform getCredentialTypeForLocation() that resource
		// being deleted cannot be public.
		credType, _, err = azcopy.GetCredentialTypeForLocation(ctx, raw.fromTo.From(), raw.source, false, cpkOptions, uotm)
	case raw.fromTo.From().IsRemote() && raw.fromTo.To().IsLocal():
		// we authenticate to the source.
		credType, _, err = azcopy.GetCredentialTypeForLocation(ctx, raw.fromTo.From(), raw.source, true, cpkOptions, uotm)
	default:
		credType = common.ECredentialType.Anonymous()
		// Log the FromTo types which getCredentialType hasn't solved, in case of miss-use.
		glcm.Info(fmt.Sprintf("Use anonymous credential by default for from-to '%v'", raw.fromTo))
	}
	if err != nil {
		glcm.Error(err.Error())
		return
	}

	return
}

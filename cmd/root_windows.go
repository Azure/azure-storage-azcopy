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

package cmd

import (
	"math"
	"net/http"
	"path"
	"strings"

	"github.com/minio/minio-go/v7"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// processOSSpecificInitialization changes the soft limit for filedescriptor for process
// return the filedescriptor limit for process. If the function fails with some, it returns
// the error
// TODO: this api is implemented for windows as well but not required because Windows
// does not default to a precise low limit like Linux does
func processOSSpecificInitialization() (int, error) {

	// this exaggerates what's possible, but is accurate enough for our purposes, in which our goal is simply to apply no specific limit on Windows
	const effectivelyUnlimited = math.MaxInt32

	return effectivelyUnlimited, nil
}

// getAzCopyAppPath returns the path of Azcopy in local appdata.
func getAzCopyAppPath() string {
	userProfile := common.GetEnvironmentVariable(common.EEnvironmentVariable.UserDir())
	azcopyAppDataFolder := strings.ReplaceAll(path.Join(userProfile, ".azcopy"), "/", `\`)

	return azcopyAppDataFolder
}

func init() {
	//Catch everything that uses http.DefaultTransport with ieproxy.GetProxyFunc()
	http.DefaultTransport.(*http.Transport).Proxy = common.GlobalProxyLookup
	transport, err := minio.DefaultTransport(true)
	if err != nil {
		transport.Proxy = common.GlobalProxyLookup
	}
}

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
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestVersionEquality(t *testing.T) {
	a := assert.New(t)
	// simple equal
	v1, _ := NewVersion("10.0.0")
	v2, _ := NewVersion("10.0.0")
	a.Zero(v1.compare(*v2))

	// preview version equal
	v1, _ = NewVersion("10.0.0-preview")
	v2, _ = NewVersion("10.0.0-preview")
	a.Zero(v1.compare(*v2))

	// future version equal
	v1, _ = NewVersion("10.0.0-preview")
	v2, _ = NewVersion("10.0.0-beta5")
	a.Zero(v1.compare(*v2))
}

func TestVersionSuperiority(t *testing.T) {
	a := assert.New(t)
	// major version bigger
	v1, _ := NewVersion("11.3.0")
	v2, _ := NewVersion("10.8.3")
	a.Equal(1, v1.compare(*v2))
	a.False(v1.OlderThan(*v2))
	a.True(v1.NewerThan(*v2))

	// minor version bigger
	v1, _ = NewVersion("15.5.6")
	v2, _ = NewVersion("15.3.5")
	a.Equal(1, v1.compare(*v2))
	a.False(v1.OlderThan(*v2))
	a.True(v1.NewerThan(*v2))

	// patch version bigger
	v1, _ = NewVersion("15.5.6")
	v2, _ = NewVersion("15.5.5")
	a.Equal(1, v1.compare(*v2))
	a.False(v1.OlderThan(*v2))
	a.True(v1.NewerThan(*v2))

	// preview bigger
	v1, _ = NewVersion("15.5.5")
	v2, _ = NewVersion("15.5.5-preview")
	a.Equal(1, v1.compare(*v2))
	a.False(v1.OlderThan(*v2))
	a.True(v1.NewerThan(*v2))
}

func TestVersionInferiority(t *testing.T) {
	a := assert.New(t)
	// major version smaller
	v1, _ := NewVersion("10.5.6")
	v2, _ := NewVersion("11.8.3")
	a.Equal(-1, v1.compare(*v2))
	a.True(v1.OlderThan(*v2))
	a.False(v1.NewerThan(*v2))

	// minor version smaller
	v1, _ = NewVersion("15.3.6")
	v2, _ = NewVersion("15.5.5")
	a.Equal(-1, v1.compare(*v2))
	a.True(v1.OlderThan(*v2))
	a.False(v1.NewerThan(*v2))

	// patch version smaller
	v1, _ = NewVersion("15.5.5")
	v2, _ = NewVersion("15.5.6")
	a.Equal(-1, v1.compare(*v2))
	a.True(v1.OlderThan(*v2))
	a.False(v1.NewerThan(*v2))

	// preview smaller
	v1, _ = NewVersion("15.5.5-preview")
	v2, _ = NewVersion("15.5.5")
	a.Equal(-1, v1.compare(*v2))
	a.True(v1.OlderThan(*v2))
	a.False(v1.NewerThan(*v2))
}

func TestCacheNewerVersion1(t *testing.T) {
	a := assert.New(t)
	testFilePath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(testFilePath)

	// 1. local version is older than remote version, remote ver is cached
	fileName := "\\test_file.txt"
	remoteVer, err := NewVersion("10.20.0")
	a.NoError(err)
	localVer, err := NewVersion("10.19.0")
	a.NoError(err)

	err = localVer.CacheRemoteVersion(*remoteVer, testFilePath+fileName)
	a.NoError(err)

	cacheVer, err := ValidateCachedVersion(testFilePath + fileName)
	a.NoError(err)
	a.Equal(cacheVer, remoteVer)

}

func TestCacheNewerVersion2(t *testing.T) {
	a := assert.New(t)
	testFilePath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(testFilePath)

	// 2. local version is same as remote version, remote ver is cached
	fileName := "\\test_file.txt"
	remoteVer, err := NewVersion("10.20.0")
	a.NoError(err)
	localVer, err := NewVersion("10.20.0")
	a.NoError(err)

	err = localVer.CacheRemoteVersion(*remoteVer, testFilePath+fileName)
	a.NoError(err)

	// remote version is cached
	cacheVer, err := ValidateCachedVersion(testFilePath + fileName)
	a.NoError(err)
	a.Equal(cacheVer, remoteVer)
}

func TestCacheNewerVersion3(t *testing.T) {
	a := assert.New(t)
	testFilePath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(testFilePath)

	// 3. local version is newer than remote version
	// sanity test bc this should never happen and will never happen
	fileName := "\\test_file.txt"
	remoteVer, err := NewVersion("10.19.0")
	a.NoError(err)
	localVer, err := NewVersion("10.20.0")
	a.NoError(err)

	err = localVer.CacheRemoteVersion(*remoteVer, testFilePath+fileName)
	a.NoError(err)

	// remote version is not cached
	cacheVer, err := ValidateCachedVersion(testFilePath + fileName)
	a.Error(err)
	a.Nil(cacheVer)
}

func TestValidateCachedVersion(t *testing.T) {
	a := assert.New(t)
	testFilePath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(testFilePath)

	// 1. cache file with later expiry and test validate cached version
	fileName := "\\test_file.txt"
	remoteVer, err := NewVersion("10.20.0")
	a.NoError(err)

	expiry := time.Now().Add(24 * time.Hour).Format(versionFileTimeFormat)
	os.WriteFile(testFilePath+fileName, []byte(remoteVer.original+","+expiry), 0666)

	// remote version is cached
	cacheVer, err := ValidateCachedVersion(testFilePath + fileName)
	a.NoError(err)
	a.Equal(cacheVer, remoteVer)
}

func TestValidateCachedVersion2(t *testing.T) {
	a := assert.New(t)
	testFilePath := scenarioHelper{}.generateLocalDirectory(a)
	defer os.RemoveAll(testFilePath)

	// 2. cache file with past expiry and test validate cached version
	fileName := "\\test_file.txt"
	remoteVer, err := NewVersion("10.20.0")
	a.NoError(err)

	expiry := time.Now().Add(-(24 * time.Hour)).Format(versionFileTimeFormat)
	os.WriteFile(testFilePath+fileName, []byte(remoteVer.original+","+expiry), 0666)

	// because the cache has expired, the cached version cannot be validated
	_, err = ValidateCachedVersion(testFilePath + fileName)
	a.Error(err)
}

func TestGetGitHubLatestVersion(t *testing.T) {
	a := assert.New(t)
	latestVersion, err := getGitHubLatestRemoteVersion()
	a.NoError(err)
	a.NotNil(latestVersion)
	a.NotEmpty(latestVersion.original)
	versionVar, err := NewVersion(common.AzcopyVersion)
	a.NoError(err)
	a.NotNil(versionVar)
	fmt.Println(latestVersion.original, versionVar.original)
	sameOrLaterVersion := latestVersion.OlderThan(common.DerefOrZero(versionVar)) ||
		latestVersion.EqualTo(common.DerefOrZero(versionVar))
	a.True(sameOrLaterVersion)
}

// Mocked test of getGitHubLatestRemoteVersionWithURL
func TestGetGitHubLatestVersionWithMocking(t *testing.T) {
	a := assert.New(t)

	// Mocked API Response
	mockResp :=
		`{
			"tag_name": "v10.29.1",
			"name": "AzCopy v10.29.1"
		}`

	// HTTP test server
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(mockResp))
		a.NoError(err)
	}))
	defer testServer.Close()

	version, err := getGitHubLatestRemoteVersionWithURL(testServer.URL)
	versionSegments := []int64{10, 29, 1} // Cast to match Version struct type
	expected := version.segments
	a.NoError(err)
	a.Equal(expected, versionSegments)
}

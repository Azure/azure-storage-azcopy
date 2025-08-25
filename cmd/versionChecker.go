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
	"bytes"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

type Version struct {
	segments []int64 // {10, 29, 1}
	preview  bool
	original string
}

const versionFileTimeFormat = "2006-01-02T15:04:05Z"

// To keep the code simple, we assume we only use a simple subset of semantic versions.
// Namely, the version is either a normal stable version, or a pre-release version with '-preview' attached.
// Examples: 10.1.0, 11.2.0-preview
func NewVersion(raw string) (*Version, error) {
	const standardError = "invalid version string"

	raw = strings.Trim(raw, "\n")
	rawSegments := strings.Split(raw, ".")
	if len(rawSegments) != 3 {
		return nil, errors.New(standardError)
	}

	v := &Version{segments: make([]int64, 3), original: raw}
	for i, str := range rawSegments {
		if strings.Contains(str, "-") {
			if i != 2 {
				return nil, errors.New(standardError)
			}
			v.preview = true
			str = strings.Split(str, "-")[0]
		}

		val, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return nil, errors.New("cannot version string")
		}
		v.segments[i] = val
	}

	return v, nil
}

// compare this version (v) to another version (v2)
// return -1 if v is smaller/older than v2
// return 0 if v is equal to v2
// return 1 if v is bigger/newer than v2
func (v Version) compare(v2 Version) int {
	// short-circuit if the two version have the exact same raw string, no need to compare
	if v.original == v2.original {
		return 0
	}

	// compare the major/minor/patch version
	// if v has a bigger number, it is newer
	for i, num := range v.segments {
		if num > v2.segments[i] {
			return 1
		} else if num < v2.segments[i] {
			return -1
		}
	}

	// if both or neither versions are previews, then they are equal
	// usually this shouldn't happen since we already checked whether the two versions have equal raw string
	// however, it is entirely possible that we have new kinds of pre-release versions that this code is not parsing correctly
	// in this case we consider both pre-release version equal anyways
	if (v.preview && v2.preview) || (!v.preview && !v2.preview) {
		return 0
	} else if v.preview && !v2.preview {
		return -1
	}

	return 1
}

// OlderThan detects if version v is older than v2
func (v Version) OlderThan(v2 Version) bool {
	return v.compare(v2) == -1
}

// NewerThan detects if version v is newer than v2
func (v Version) NewerThan(v2 Version) bool {
	return v.compare(v2) == 1
}

func (v Version) EqualTo(v2 Version) bool {
	return v.compare(v2) == 0
}

// CacheRemoteVersion caches the remote version in file located on filePath for a day
// if version v (local version) is older than or equal to the remote version
func (v Version) CacheRemoteVersion(remoteVer Version, filePath string) error {
	if v.OlderThan(remoteVer) || v.EqualTo(remoteVer) {
		expiry := time.Now().Add(24 * time.Hour).Format(versionFileTimeFormat)
		// make sure filepath is absolute filepath so WriteFile is not written to customers current directory
		if err := os.WriteFile(filePath, []byte(remoteVer.original+","+expiry), 0666); err != nil {
			return err
		}
	}
	return nil
}

// ValidateCachedVersion checks if the given file on filepath contains cached version and expiry.
// Reads the cache and checks if the cache is still fresh.
// Returns Version object from cache.
func ValidateCachedVersion(filePath string) (*Version, error) {
	// Check the locally cached file to get the version.
	data, err := os.ReadFile(filePath)
	if err == nil {
		// If the data is fresh, don't make the call and return right away
		versionAndExpiry := bytes.Split(data, []byte(","))
		if len(versionAndExpiry) == 2 {
			version, err := NewVersion(string(versionAndExpiry[0]))
			if err == nil {
				expiry, err := time.Parse(versionFileTimeFormat, string(versionAndExpiry[1]))
				currentTime := time.Now()
				if err == nil && expiry.After(currentTime) {
					return version, nil
				}
			}
		}
	}
	return nil, errors.New("failed to fetch or validate the cached version")
}

// PrintOlderVersion prints out info messages that the newest version is available to download.
func PrintOlderVersion(newest Version, local Version) {
	if local.OlderThan(newest) {
		executablePathSegments := strings.Split(strings.Replace(os.Args[0], "\\", "/", -1), "/")
		executableName := executablePathSegments[len(executablePathSegments)-1]

		// output in info mode instead of stderr, as it was crashing CI jobs of some people
		glcm.Info(executableName + " " + local.original + ": A newer version " + newest.original + " is available to download\n")
	}
}

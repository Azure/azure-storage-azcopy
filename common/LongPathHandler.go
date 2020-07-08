// Copyright © Microsoft <wastore@microsoft.com>
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

package common

import (
	"path/filepath"
	"runtime"
	"strings"
)

// https://docs.microsoft.com/en-us/windows/desktop/FileIO/naming-a-file#maximum-path-length-limitation
// Because windows doesn't (by default) support strings above 260 characters,
// we need to provide a special prefix (\\?\) to tell it to support paths up to 32,767 characters.

// Furthermore, we don't use the built-in long path support in the Go SDK commit 231aa9d6d7
// because it fails to support long UNC paths. As a result, we opt to wrap things such as filepath.Glob()
// to safely use them with long UNC paths.

// ToExtendedPath converts short paths to an extended path.
func ToExtendedPath(short string) string {
	// filepath.Abs has an issue where if the path is just the drive indicator of your CWD, it just returns the CWD. So, we append the / to show that yes, we really mean C: or whatever.
	if runtime.GOOS == "windows" && len(short) == 2 && RootDriveRegex.MatchString(short) {
		short += "/"
	}

	short, err := filepath.Abs(short)
	PanicIfErr(err) //TODO: Handle errors better?

	// ex. C:/dir/file.txt -> \\?\C:\dir\file.txt
	// ex. \\share\dir\file.txt -> \\?\UNC\share\dir\file.txt
	if runtime.GOOS == "windows" { // Only do this on Windows
		if strings.HasPrefix(short, EXTENDED_PATH_PREFIX) { // already an extended path \\?\C:\folder\file.txt or \\?\UNC\sharename\folder\file.txt
			return strings.Replace(short, `/`, `\`, -1) // Just ensure it has all backslashes-- Windows can't handle forward-slash anymore in this format.
		} else if strings.HasPrefix(short, `\\`) { // this is a file share (//sharename/folder/file.txt)
			// Steal the first backslash, and then append the prefix. Enforce \.
			return strings.Replace(EXTENDED_UNC_PATH_PREFIX+short[1:], `/`, `\`, -1) // convert to extended UNC path
		} else { // this is coming from a drive-- capitalize the drive prefix. (C:/folder/file.txt)
			if len(short) >= 2 && RootDriveRegex.MatchString(short[:2]) {
				short = strings.Replace(short, short[:2], strings.ToUpper(short[:2]), 1)
			}
			// Then append the prefix. Enforce \.
			return strings.Replace(EXTENDED_PATH_PREFIX+short, `/`, `\`, -1) // Just append the prefix
		}
	}

	return short
}

// ToShortPath converts an extended path to a short path.
func ToShortPath(long string) string {
	if runtime.GOOS == "windows" { // Only do this on Windows
		// ex. \\?\UNC\share\dir\file.txt -> \\share\dir\file.txt
		// ex. \\?\C:\dir\file.txt -> C:\dir\file.txt
		if strings.HasPrefix(long, EXTENDED_UNC_PATH_PREFIX) { // UNC extended path: Cut the prefix off, add an extra \ to the start
			return `\` + long[len(EXTENDED_UNC_PATH_PREFIX):] // Return what we stole from it
		} else if strings.HasPrefix(long, EXTENDED_PATH_PREFIX) { // Standard extended path: Cut the prefix off.
			return long[len(EXTENDED_PATH_PREFIX):]
		}
	}

	return long
}

func IsShortPath(s string) bool {
	return s == ToShortPath(s)
}

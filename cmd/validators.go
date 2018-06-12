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
	"errors"
	"fmt"
	"github.com/Azure/azure-storage-azcopy/common"
	"net/url"
	"reflect"
	"strings"
)

func validateFromTo(src, dst string, userSpecifiedFromTo string) (common.FromTo, error) {
	inferredFromTo := inferFromTo(src, dst)
	if userSpecifiedFromTo == "" {
		// If user didn't explicitly specify FromTo, use what was inferred (if possible)
		if inferredFromTo == common.EFromTo.Unknown() {
			return common.EFromTo.Unknown(), errors.New("Invalid source/destination combination. Pleasee use the --FromTo switch")
		}
		return inferredFromTo, nil
	}

	// User explicitly specified FromTo, make sure it matches what we infer or accept it if we can't infer
	var userFromTo common.FromTo
	err := userFromTo.Parse(userSpecifiedFromTo)
	if err != nil {
		return common.EFromTo.Unknown(), fmt.Errorf("Invalid --FromTo value specified: %q", userSpecifiedFromTo)
	}
	if inferredFromTo == common.EFromTo.Unknown() || inferredFromTo == userFromTo ||
		userFromTo == common.EFromTo.BlobTrash() || userFromTo == common.EFromTo.FileTrash() {
		// We couldn't infer the FromTo or what we inferred matches what the user specified
		// We'll accept what the user specified
		return userFromTo, nil
	}
	// inferredFromTo != raw.fromTo: What we inferred doesn't match what the user specified
	return common.EFromTo.Unknown(), errors.New("The specified --FromTo swith is inconsistent with the specified source/destination combination.")
}

func inferFromTo(src, dst string) common.FromTo {
	// Try to infer the 1st argument
	srcLocation := inferArgumentLocation(src)
	if srcLocation == srcLocation.Unknown() {
		fmt.Printf("Can't infer source location of %q. Please specify the --FromTo switch", src)
		return common.EFromTo.Unknown()
	}

	dstLocation := inferArgumentLocation(dst)
	if dstLocation == dstLocation.Unknown() {
		fmt.Printf("Can't infer destination location of %q. Please specify the --FromTo switch", dst)
		return common.EFromTo.Unknown()
	}

	switch {
	case srcLocation == ELocation.Local() && dstLocation == ELocation.Blob():
		return common.EFromTo.LocalBlob()
	case srcLocation == ELocation.Blob() && dstLocation == ELocation.Local():
		return common.EFromTo.BlobLocal()
	case srcLocation == ELocation.Local() && dstLocation == ELocation.File():
		return common.EFromTo.LocalFile()
	case srcLocation == ELocation.File() && dstLocation == ELocation.Local():
		return common.EFromTo.FileLocal()
	case srcLocation == ELocation.Pipe() && dstLocation == ELocation.Blob():
		return common.EFromTo.PipeBlob()
	case srcLocation == ELocation.Blob() && dstLocation == ELocation.Pipe():
		return common.EFromTo.BlobPipe()
	case srcLocation == ELocation.Pipe() && dstLocation == ELocation.File():
		return common.EFromTo.PipeFile()
	case srcLocation == ELocation.File() && dstLocation == ELocation.Pipe():
		return common.EFromTo.FilePipe()
	case srcLocation == ELocation.Local() && dstLocation == ELocation.BlobFS():
		return common.EFromTo.LocalBlobFS()
	case srcLocation == ELocation.BlobFS() && dstLocation == ELocation.Local():
		return common.EFromTo.BlobFSLocal()
	}
	return common.EFromTo.Unknown()
}

var ELocation = Location(0)

// JobStatus indicates the status of a Job; the default is InProgress.
type Location uint32 // Must be 32-bit for atomic operations

func (Location) Unknown() Location { return Location(0) }
func (Location) Local() Location   { return Location(1) }
func (Location) Pipe() Location    { return Location(2) }
func (Location) Blob() Location    { return Location(3) }
func (Location) File() Location    { return Location(4) }
func (Location) BlobFS() Location  { return Location(5) }
func (l Location) String() string {
	return common.EnumHelper{}.StringInteger(uint32(l), reflect.TypeOf(l))
}

func inferArgumentLocation(arg string) Location {
	if arg == pipeLocation {
		return ELocation.Pipe()
	}
	if startsWith(arg, "https") {
		// Let's try to parse the argument as a URL
		u, err := url.Parse(arg)
		// NOTE: sometimes, a local path can also be parsed as a url. To avoid thinking it's a URL, check Scheme, Host, and Path
		if err == nil && u.Scheme != "" || u.Host != "" || u.Path != "" {
			// Is the argument a URL to blob storage?
			switch host := strings.ToLower(u.Host); true {
			// Azure Stack does not have the core.windows.net
			case strings.Contains(host, ".blob"):
				return ELocation.Blob()
			case strings.Contains(host, ".file.core.windows.net"):
				return ELocation.File()
			case strings.Contains(host, ".dfs.core.windows.net"):
				return ELocation.BlobFS()
			}
		}
	} else {
		// If we successfully get the argument's file stats, then we'll infer that this argument is a local file
		//_, err := os.Stat(arg)
		//if err != nil && !os.IsNotExist(err){
		//	return ELocation.Unknown()
		//}
		return ELocation.Local()
	}

	return ELocation.Unknown()
}

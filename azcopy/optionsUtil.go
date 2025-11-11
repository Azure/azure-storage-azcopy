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
	"errors"
	"fmt"
	"math"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/Azure/azure-storage-azcopy/v10/traverser"
)

// BlockSizeInBytes converts a FLOATING POINT number of MiB, to a number of bytes
// A non-nil error is returned if the conversion is not possible to do accurately (e.g. it comes out of a fractional number of bytes)
// The purpose of using floating point is to allow specialist users (e.g. those who want small block sizes to tune their read IOPS)
// to use fractions of a MiB. E.g.
// 0.25 = 256 KiB
// 0.015625 = 16 KiB
func BlockSizeInBytes(rawBlockSizeInMiB float64) (int64, error) {
	if rawBlockSizeInMiB < 0 {
		return 0, errors.New("negative block size not allowed")
	}
	rawSizeInBytes := rawBlockSizeInMiB * 1024 * 1024 // internally we use bytes, but users' convenience the command line uses MiB
	if rawSizeInBytes > math.MaxInt64 {
		return 0, errors.New("block size too big for int64")
	}
	const epsilon = 0.001 // arbitrarily using a tolerance of 1000th of a byte
	_, frac := math.Modf(rawSizeInBytes)
	isWholeNumber := frac < epsilon || frac > 1.0-epsilon // frac is very close to 0 or 1, so rawSizeInBytes is (very close to) an integer
	if !isWholeNumber {
		return 0, fmt.Errorf("while fractional numbers of MiB are allowed as the block size, the fraction must result to a whole number of bytes. %.12f MiB resolves to %.3f bytes", rawBlockSizeInMiB, rawSizeInBytes)
	}
	return int64(math.Round(rawSizeInBytes)), nil
}

// we assume that preserveSmbPermissions and preserveSmbInfo have already been validated, such that they are only true if both resource types support them
func NewFolderPropertyOption(fromTo common.FromTo, recursive, stripTopDir bool, filters []traverser.ObjectFilter,
	preserveSmbInfo, preservePermissions, preservePosixProperties, isDstNull, includeDirectoryStubs bool) (common.FolderPropertyOption, string) {

	getSuffix := func(willProcess bool) string {
		willProcessString := common.Iff(willProcess, "will be processed", "will not be processed")

		template := ". For the same reason, %s defined on folders %s"
		switch {
		case preservePermissions && preserveSmbInfo:
			return fmt.Sprintf(template, "properties and permissions", willProcessString)
		case preserveSmbInfo:
			return fmt.Sprintf(template, "properties", willProcessString)
		case preservePermissions:
			return fmt.Sprintf(template, "permissions", willProcessString)
		default:
			return "" // no preserve flags set, so we have nothing to say about them
		}
	}

	bothFolderAware := (fromTo.AreBothFolderAware() || preservePosixProperties || preservePermissions || includeDirectoryStubs) && !isDstNull
	isRemoveFromFolderAware := fromTo == common.EFromTo.FileTrash()
	if bothFolderAware || isRemoveFromFolderAware {
		if !recursive {
			return common.EFolderPropertiesOption.NoFolders(), // doesn't make sense to move folders when not recursive. E.g. if invoked with /* and WITHOUT recursive
				"Any empty folders will not be processed, because --recursive was not specified" +
					getSuffix(false)
		}

		// check filters. Otherwise, if filter was say --include-pattern *.txt, we would transfer properties
		// (but not contents) for every directory that contained NO text files.  Could make heaps of empty directories
		// at the destination.
		filtersOK := true
		for _, f := range filters {
			if f.AppliesOnlyToFiles() {
				filtersOK = false // we have a least one filter that doesn't apply to folders
			}
		}
		if !filtersOK {
			return common.EFolderPropertiesOption.NoFolders(),
				"Any empty folders will not be processed, because a file-focused filter is applied" +
					getSuffix(false)
		}

		message := "Any empty folders will be processed, because source and destination both support folders"
		if isRemoveFromFolderAware {
			message = "Any empty folders will be processed, because deletion is from a folder-aware location"
		}
		message += getSuffix(true)
		if stripTopDir {
			return common.EFolderPropertiesOption.AllFoldersExceptRoot(), message
		}
		return common.EFolderPropertiesOption.AllFolders(), message
	}

	return common.EFolderPropertiesOption.NoFolders(),
		"Any empty folders will not be processed, because source and/or destination doesn't have full folder support" +
			getSuffix(false)

}

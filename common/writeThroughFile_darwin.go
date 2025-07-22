//go:build linux || darwin
// +build linux darwin

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

package common

import (
	"context"
	"os"
)

func CreateFileOfSizeWithWriteThroughOption(ctx context.Context, destinationPath string, fileSize int64, writeThrough bool, t FolderCreationTracker, forceIfReadOnly bool) (*os.File, error) {
	// forceIfReadOnly is not used on this OS

	err := CreateParentDirectoryIfNotExist(ctx, destinationPath, t)
	if err != nil {
		return nil, err
	}

	// TODO: currently writeThrough is ignored here on darwin. Review, and use it if we have an implementation we are comfortable with
	// A quick internet search returned conflicting opinions on whether MacOS suppose O_SYNC or uses a different flag with the same meaning.
	// If different with same meaning, can we just use O_SYNC here?  That's what we need to find out before implementing.

	f, err := os.OpenFile(destinationPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, DEFAULT_FILE_PERM)
	if err != nil {
		return nil, err
	}
	//err = syscall.Fallocate(int(f.Fd()), 0, 0, fileSize)
	//if err != nil {
	//	return nil, err
	//}
	// TODO: Need to appropriate fallocate api for darwin
	if truncateError := f.Truncate(fileSize); truncateError != nil {
		return nil, truncateError
	}
	return f, nil
}

func SetBackupMode(enable bool, fromTo FromTo) error {
	// n/a on this platform
	return nil
}

func GetExtendedProperties(path string, entityType EntityType) (ExtendedProperties, error) {
	return DefaultExtendedProperties{}, nil
}

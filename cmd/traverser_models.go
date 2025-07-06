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
	"time"

	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// TraverserErrorItemInfo provides an interface for error information related to files and folders that failed enumeration.
// It includes methods to retrieve the file path, file size, last modified time,
// whether the file is a directory, the error message, and whether the file is a source.
//
// Methods:
// - FullFilePath() string: Returns the path of the file.
// - FileName() string: Returns the name of the file.
// - FileSize() int64: Returns the size of the file in bytes.
// - FileLastModifiedTime() time.Time: Returns the last modified time of the file.
// - IsDir() bool: Returns true if the file is a directory, false otherwise.
// - ErrorMsg() error: Returns the error message associated with the file.
// - Location() common.Location: Returns the source of the error (common.Location).
type TraverserErrorItemInfo interface {
	FullPath() string
	Name() string
	Size() int64
	LastModifiedTime() time.Time
	IsDir() bool
	ErrorMessage() error
	Location() common.Location
}

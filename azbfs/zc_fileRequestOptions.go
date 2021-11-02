// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azbfs

// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create.
type CreateFileOptions struct {
	// Custom headers to apply to the file.
	Headers BlobFSHTTPHeaders
	// User defined properties to be stored with the file.
	Metadata map[string]string
}

// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create.
type RenameFileOptions struct {
	// The optional destination file system for the file.
	DestinationFileSystem *string
	// The destination path for the file.
	DestinationPath string
	// The new SAS for a destination file
	DestinationSas *string
}

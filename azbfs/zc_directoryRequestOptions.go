// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azbfs

// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create.
type CreateDirectoryOptions struct {
	// Whether or not to recreate the directory if it exists.
	RecreateIfExists bool
	// User defined properties to be stored with the directory.
	Metadata map[string]string
}

// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/create.
type RenameDirectoryOptions struct {
	// The optional destination file system for the directory.
	DestinationFileSystem *string
	// The destination path for the directory.
	DestinationPath string
	// The new SAS for a destination Directory
	DestinationSas *string
}

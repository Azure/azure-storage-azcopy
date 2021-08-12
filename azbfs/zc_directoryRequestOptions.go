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
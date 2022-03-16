// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package azbfs

// For more information, see https://docs.microsoft.com/en-us/rest/api/storageservices/datalakestoragegen2/path/list.
type ListPathsFilesystemOptions struct {
	// Filters results to paths in this directory.
	Path *string
	// Whether or not to list recursively.
	Recursive bool
	// Whether or not AAD Object IDs will be converted to user principal name.
	UpnReturned *bool
	// The maximum number of items to return.
	MaxResults *int32
	// The continuation token to resume listing.
	ContinuationToken *string
}

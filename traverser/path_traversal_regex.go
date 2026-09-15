package traverser

import "github.com/Azure/azure-storage-azcopy/v10/common"

// PathTraversalNameRegex matches any path segment that is fully comprised of dots (".").
// It is defined in the common package (so it can also be used by the storage engine without
// an import cycle) and re-exported here for readability from the traverser package.
var PathTraversalNameRegex = common.PathTraversalNameRegex

package e2etest

import (
	"fmt"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

type GetResourceOptions struct {
	// Key for AccountRegistry when using account-based systems
	PreferredAccount *string
}

// GetRootResource differs from CreateResource, in that GetRootResource obtains the lowest possible resource for a particular location
// This eases the act of getting a base resource for tests that might utilize multiple "kinds" of resources (e.g. Local, Azure) interchangeably.
// on *Local*, this inherently creates a container. But that's fine, because it's likely to be used.
func GetRootResource(a Asserter, location common.Location, varOpts ...GetResourceOptions) ResourceManager {
	opts := FirstOrZero(varOpts)

	switch location {
	case common.ELocation.Local():
		return NewLocalContainer(a)
	case common.ELocation.Blob(), common.ELocation.BlobFS(), common.ELocation.File():
		// acct handles the dryrun case for us
		acct := GetAccount(a, DerefOrDefault(opts.PreferredAccount, PrimaryStandardAcct))
		return acct.GetService(a, location)
	default:
		a.Error(fmt.Sprintf("TODO: Location %s is not yet supported", location))
		return nil
	}
}

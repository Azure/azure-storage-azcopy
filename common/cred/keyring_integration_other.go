//go:build !linux && !windows

package cred

import (
	"fmt"
	"os"
	"strconv"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
)

func GetIntegrationKeyring() (Keyring, error) {
	return GetOSKeyring(GetOSKeyringOptions{
		// DPAPI file path is unnecessary as Windows has a custom implementation.

		RootKey: to.Ptr(fmt.Sprintf("azcopy/integration/%d", strconv.Itoa(os.Getpid()))),
	})
}

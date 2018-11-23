// +build !se_integration
// For public version azcopy, gnome keyring is not necessary, and no need to
// involve additional dependencies to libsecret-1 and glib-2.0

package common

import "errors"

type gnomeKeyring struct{}

func (p gnomeKeyring) Get(Service string, Account string) (string, error) {
	// By design, not useful for non integration scenario.
	return "", errors.New("Not implemented")
}

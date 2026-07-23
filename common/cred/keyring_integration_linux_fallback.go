//go:build linux && !se_integration

package cred

func GetIntegrationKeyring() (Keyring, error) {
	// no-op keyring; stgexp is reliant upon gnome keyring
	return NewMemKeyring(map[string]Token{}), nil
}

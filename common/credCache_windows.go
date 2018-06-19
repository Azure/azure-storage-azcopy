package common

import (
	"github.com/danieljoos/wincred"
)

type credCache struct{}

const cachedTokenKey = "AzCopyOAuthTokenCache"
const errNotFound = "Element not found."

// HasCachedToken returns if there is cached token in token manager for current executing user.
func (c *credCache) HasCachedToken() (bool, error) {
	_, err := wincred.GetGenericCredential(cachedTokenKey)
	if err != nil {
		if err.Error() == errNotFound {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

// RemoveCachedToken delete the cached token.
func (c *credCache) RemoveCachedToken() error {
	token, err := wincred.GetGenericCredential(cachedTokenKey)
	if err != nil {
		return err
	}

	return token.Delete()
}

func (c *credCache) SaveToken(token OAuthTokenInfo) error {
	b, err := token.ToJSON()
	if err != nil {
		return err
	}
	cred := wincred.NewGenericCredential(cachedTokenKey)
	cred.CredentialBlob = b
	return cred.Write()
}

func (c *credCache) LoadToken() (*OAuthTokenInfo, error) {
	cred, err := wincred.GetGenericCredential(cachedTokenKey)
	if err != nil {
		return nil, err
	}

	token, err := JSONToTokenInfo(cred.CredentialBlob)
	if err != nil {
		return nil, err
	}

	return token, nil
}

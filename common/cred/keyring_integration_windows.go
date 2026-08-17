//go:build windows

package cred

import (
	"encoding/json"
	"strings"

	"github.com/danieljoos/wincred"
)

func GetIntegrationKeyring() (Keyring, error) {
	return &integrationKeyring{}, nil
}

type integrationKeyring struct {
	keyPrefix string
}

func (i integrationKeyring) GetToken(nickname string) (Token, bool) {
	if nickname == "" {
		nickname = DefaultNickname
	}

	key := strings.TrimSuffix(i.keyPrefix, "*") + nickname
	cred, err := wincred.GetGenericCredential(key)
	if err != nil {
		if nickname != DefaultNickname {
			return i.getToken(DefaultNickname)
		}
		return &token{}, false
	}

	if cred == nil {
		if nickname != DefaultNickname {
			return i.getToken(DefaultNickname)
		}
		return &token{}, false
	}

	var out token
	err = json.Unmarshal(cred.CredentialBlob, &out)
	if err != nil {
		if nickname != DefaultNickname {
			return i.getToken(DefaultNickname)
		}
		return &token{}, false
	}

	return &out, true
}

func (i integrationKeyring) getToken(nickname string) (*token, bool) {
	key := strings.TrimSuffix(i.keyPrefix, "*") + nickname
	cred, err := wincred.GetGenericCredential(key)
	if err != nil || cred == nil {
		return &token{}, false
	}

	var out token
	err = json.Unmarshal(cred.CredentialBlob, &out)
	if err != nil {
		return &token{}, false
	}

	return &out, true
}

func (i integrationKeyring) keyringImpl() {}

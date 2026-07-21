package cred

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// KeyringEnvConf is a mapping of nicknames to tokens
type KeyringEnvConf map[string]token

var globalEnvKeyring = &keyringEnvVar{
	initOnce: &sync.Once{},
	stash:    make(KeyringEnvConf),
}

// keyringEnvVar reads it's keyring from the environment, stashes it, then deletes the env var.
// The config is read from
type keyringEnvVar struct {
	initOnce *sync.Once
	error    error
	stash    KeyringEnvConf
}

func GetEnvironmentKeyring() (keyring Keyring, err error) {
	globalEnvKeyring.initOnce.Do(func() {
		rawEnv, ok := enum.EEnvironmentVariable.KeyringConfig().Lookup()
		if ok {
			globalEnvKeyring.error = json.Unmarshal([]byte(rawEnv), &globalEnvKeyring.stash)
			if globalEnvKeyring.error != nil {
				globalEnvKeyring.error = fmt.Errorf("%w `%v` %v", globalEnvKeyring.error, rawEnv, ok)

				return
			}
		}

		classicEnv, ok := enum.EEnvironmentVariable.OAuthTokenInfo().Lookup()
		if ok {
			var classicToken compatTokenInfo
			err := json.Unmarshal([]byte(classicEnv), &classicToken)

			if err != nil && globalEnvKeyring.error == nil {
				// ignore the fallback
			} else if err == nil {
				newToken := classicToken.Upgrade()
				globalEnvKeyring.stash[newToken.Nickname] = newToken
			}
		}

		for nick, token := range globalEnvKeyring.stash {
			token.Nickname = nick
		}
	})

	if globalEnvKeyring.error != nil {
		return nil, globalEnvKeyring.error
	}

	return globalEnvKeyring, nil
}

func (e *keyringEnvVar) GetToken(nickname string) (Token, bool) {
	if nickname == "" {
		nickname = DefaultNickname
	}

	token, ok := e.stash[nickname]
	if !ok && nickname != DefaultNickname {
		token, ok = e.stash[DefaultNickname]
	}

	return &token, ok
}

func (e *keyringEnvVar) ListTokens() ([]TokenHeader, error) {
	var out []TokenHeader
	for _, v := range e.stash {
		out = append(out, v.TokenHeader)
	}

	return out, nil
}

func (e *keyringEnvVar) keyringImpl() {}

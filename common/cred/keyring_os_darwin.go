//go:build darwin

package cred

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/keybase/go-keychain"
)

const (
	darwinServiceName = "AzCopyV10"
)

func GetOSKeyring(opts GetOSKeyringOptions) (Keyring, error) {
	loginCacheName := enum.EEnvironmentVariable.LoginCacheName().Get()

	return &darwinCredCache{
		serviceName:      *ternary.DefaultValue(opts.RootKey, darwinServiceName) + loginCacheName,
		lock:             sync.RWMutex{},
		kcSecClass:       keychain.SecClassGenericPassword,
		kcSynchronizable: keychain.SynchronizableNo,
		kcAccessible:     keychain.AccessibleAfterFirstUnlockThisDeviceOnly,
	}, nil
}

type darwinCredCache struct {
	serviceName string
	lock        sync.RWMutex

	kcSecClass       keychain.SecClass
	kcSynchronizable keychain.Synchronizable
	kcAccessible     keychain.Accessible
}

func (c *darwinCredCache) ListTokens() ([]TokenHeader, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	query := keychain.NewItem()
	query.SetSecClass(c.kcSecClass)
	query.SetService(c.serviceName)
	query.SetMatchLimit(keychain.MatchLimitAll)
	query.SetReturnData(true)

	results, err := keychain.QueryItem(query)
	if err != nil {
		err = handleGenericKeyChainSecError(err)
		return nil, err
	}

	var out []TokenHeader
	type headerOnly struct {
		TokenHeader
	}
	for _, v := range results {
		var toParse headerOnly
		err := json.Unmarshal(v.Data, &toParse)
		if err != nil {
			continue
		}

		out = append(out, toParse.TokenHeader)
	}

	return out, nil
}

func (c *darwinCredCache) GetToken(nickname string) (token, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if nickname == "" {
		nickname = DefaultNickname
	}

	query := keychain.NewItem()
	query.SetSecClass(c.kcSecClass)
	query.SetService(c.serviceName)
	query.SetAccount(nickname)
	query.SetMatchLimit(keychain.MatchLimitOne)
	query.SetReturnData(true)

	results, err := keychain.QueryItem(query)
	if err != nil {
		err = handleGenericKeyChainSecError(err)
		return token{}, false
	}

	if len(results) == 0 {
		if nickname != DefaultNickname {
			return c.getToken(DefaultNickname)
		}
		return token{}, false
	}

	var out token
	err = json.Unmarshal(results[0].Data, &out)
	if err != nil {
		return token{}, false
	}

	return out, true
}

func (c *darwinCredCache) getToken(nickname string) (token, bool) {
	query := keychain.NewItem()
	query.SetSecClass(c.kcSecClass)
	query.SetService(c.serviceName)
	query.SetAccount(nickname)
	query.SetMatchLimit(keychain.MatchLimitOne)
	query.SetReturnData(true)

	results, err := keychain.QueryItem(query)
	if err != nil || len(results) == 0 {
		return token{}, false
	}

	var out token
	err = json.Unmarshal(results[0].Data, &out)
	if err != nil {
		return token{}, false
	}

	return out, true
}

func (c *darwinCredCache) DeleteToken(nickname string) bool {
	if nickname == "" {
		nickname = DefaultNickname
	}

	err := keychain.DeleteGenericPasswordItem(c.serviceName, nickname)
	if err != nil {
		err = handleGenericKeyChainSecError(err)
		if errors.Is(err, keychain.ErrorItemNotFound) {
			return false
		}
		return false
	}

	return true
}

func (c *darwinCredCache) SaveToken(info token) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	b, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal when saving token: %w", err)
	}
	item := keychain.NewItem()
	item.SetSecClass(c.kcSecClass)
	item.SetService(c.serviceName)
	item.SetAccount(info.Nickname)
	item.SetData(b)
	item.SetSynchronizable(c.kcSynchronizable)
	item.SetAccessible(c.kcAccessible)

	err = keychain.AddItem(item)
	if errors.Is(err, keychain.ErrorDuplicateItem) {
		query := keychain.NewItem()
		query.SetSecClass(c.kcSecClass)
		query.SetService(c.serviceName)
		query.SetAccount(info.Nickname)
		query.SetMatchLimit(keychain.MatchLimitOne)
		query.SetReturnData(true)
		err := keychain.UpdateItem(query, item)
		if err != nil {
			err = handleGenericKeyChainSecError(err)
			return fmt.Errorf("failed to save token, %v", err)
		}
	} else if err != nil {
		err = handleGenericKeyChainSecError(err)
		return fmt.Errorf("failed to save token: %w", err)
	}

	return nil
}

func (c *darwinCredCache) keyringImpl() {}

func handleGenericKeyChainSecError(err error) error {
	if err == keychain.ErrorInteractionNotAllowed {
		return fmt.Errorf(
			"if you are using SSH, please run 'security unlock-keychain' to unlock default(login) keychain from SSH first, and then re-run your azcopy command. (Error details: %v)", err)
	}

	return err
}

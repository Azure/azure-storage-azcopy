//go:build linux

package cred

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/wastore/keyctl"
)

const (
	DefaultRootKeyName = "AzCopyOAuthTokenCache"
)

func GetOSKeyring(opts GetOSKeyringOptions) (Keyring, error) {
	loginCacheName := enum.EEnvironmentVariable.LoginCacheName().Get()

	return &linuxCredCache{
		rootKeyName:      *ternary.DefaultValue(opts.RootKey, DefaultRootKeyName) + loginCacheName,
		lock:             sync.RWMutex{},
		fetchedKeysCache: nil,
		isPermSet:        false,
	}, nil
}

type credCacheEntry struct {
	Header TokenHeader

	Key *keyctl.Key `json:"-"`
}

type linuxCredCache struct {
	rootKeyName    string
	lock           sync.RWMutex
	sessionKeyring keyctl.Keyring

	initOnce sync.Once

	fetchedKeysCache map[string]credCacheEntry
	isPermSet        bool
}

func (c *linuxCredCache) ListTokens() ([]TokenHeader, error) {
	out := make([]TokenHeader, 0)
	for _, v := range c.fetchedKeysCache {
		out = append(out, v.Header)
	}

	return out, nil
}

func (c *linuxCredCache) init() {
	c.initOnce.Do(func() {
		key, err := c.sessionKeyring.Search(c.rootKeyName)
		if err != nil {
			return
		}

		buf, err := key.Get()
		if err != nil {
			return
		}

		err = json.Unmarshal(buf, &c.fetchedKeysCache)
		if err != nil {
			return
		}
	})
}

func (c *linuxCredCache) GetToken(nickname string) (token, bool) {
	c.init()
	c.lock.RLock()
	defer c.lock.RUnlock()

	if nickname == "" {
		nickname = DefaultNickname
	}

	entry, ok := c.fetchedKeysCache[nickname]
	if !ok {
		if nickname != DefaultNickname {
			return c.getToken(DefaultNickname)
		}
		return token{}, false
	}

	if entry.Key == nil {
		var err error
		entry.Key, err = c.sessionKeyring.Search(nickname)
		if err != nil {
			if nickname != DefaultNickname {
				return c.getToken(DefaultNickname)
			}
			return token{}, false
		}
	}

	buf, err := entry.Key.Get()
	if err != nil {
		if nickname != DefaultNickname {
			return c.getToken(DefaultNickname)
		}
		return token{}, false
	}

	var out token
	err = json.Unmarshal(buf, &out)
	if err != nil {
		if nickname != DefaultNickname {
			return c.getToken(DefaultNickname)
		}
		return token{}, false
	}

	return out, true
}

func (c *linuxCredCache) getToken(nickname string) (token, bool) {
	key, err := c.sessionKeyring.Search(nickname)
	if err != nil {
		return token{}, false
	}

	buf, err := key.Get()
	if err != nil {
		return token{}, false
	}

	var out token
	err = json.Unmarshal(buf, &out)
	if err != nil {
		return token{}, false
	}

	return out, true
}

func (c *linuxCredCache) DeleteToken(nickname string) bool {
	c.init()

	if nickname == "" {
		nickname = DefaultNickname
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	key, err := c.sessionKeyring.Search(nickname)
	if err != nil {
		if errors.Is(err, syscall.ENOKEY) {
			return false
		}
		return false
	}

	err = key.Unlink()
	if err != nil {
		return false
	}

	delete(c.fetchedKeysCache, nickname)
	return true
}

func (c *linuxCredCache) SaveToken(info token) error {
	c.init()
	c.lock.Lock()
	defer c.lock.Unlock()

	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}

	keyring, err := keyctl.SessionKeyring()
	if err != nil {
		return err
	}

	key, err := keyring.Add(info.Nickname, buf)
	if err != nil {
		return err
	}

	err = keyctl.SetPerm(key, keyctl.PermUserAll)
	if err != nil {
		unlinkErr := key.Unlink()
		if unlinkErr != nil {
			panic(errors.New("failed to set permissions, and cannot unlink key, for security reasons it is recommended to log out of the current session"))
		}
		return fmt.Errorf("failed to set permission for cached token, %v", err)
	}

	c.fetchedKeysCache[info.Nickname] = credCacheEntry{
		Header: info.TokenHeader,
		Key:    key,
	}

	return nil
}

func (c *linuxCredCache) keyringImpl() {}

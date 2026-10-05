//go:build linux

package cred

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"syscall"

	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
	"github.com/wastore/keyctl"
)

const (
	DefaultRootKeyName = "AzCopyOAuthTokenCache"
)

func GetOSKeyring(opts GetOSKeyringOptions) (Keyring, error) {
	loginCacheName := ternary.DerefOrZero(opts.OSKeyringCacheName)
	sessionKeyring, err := keyctl.SessionKeyring()
	if err != nil {
		return nil, fmt.Errorf("failed to get session keyring: %w", err)
	}

	return &linuxCredCache{
		rootKeyName:      *ternary.DefaultValue(opts.RootKey, DefaultRootKeyName) + loginCacheName,
		lock:             sync.RWMutex{},
		sessionKeyring:   sessionKeyring,
		fetchedKeysCache: make(map[string]credCacheEntry),
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
}

var _ RWKeyring = (*linuxCredCache)(nil)

func (c *linuxCredCache) ListTokens() ([]TokenHeader, error) {
	c.init()
	c.lock.RLock()
	defer c.lock.RUnlock()

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
		if c.fetchedKeysCache == nil {
			c.fetchedKeysCache = make(map[string]credCacheEntry)
		}
	})
}

func (c *linuxCredCache) GetToken(nickname string) (Token, bool) {
	c.init()
	c.lock.RLock()
	defer c.lock.RUnlock()

	if nickname == "" {
		nickname = DefaultNickname
	}

	entry, ok := c.fetchedKeysCache[nickname]
	if !ok {
		token, found := c.getToken(nickname)
		if found || nickname == DefaultNickname {
			return token, found
		}
		return c.getToken(DefaultNickname)
	}

	if entry.Key == nil {
		var err error
		entry.Key, err = c.sessionKeyring.Search(nickname)
		if err != nil {
			if nickname != DefaultNickname {
				return c.getToken(DefaultNickname)
			}
			return nil, false
		}
	}

	buf, err := entry.Key.Get()
	if err != nil {
		if nickname != DefaultNickname {
			return c.getToken(DefaultNickname)
		}
		return nil, false
	}

	var out token
	err = json.Unmarshal(buf, &out)
	if err != nil {
		if nickname != DefaultNickname {
			return c.getToken(DefaultNickname)
		}
		return nil, false
	}

	return &out, true
}

func (c *linuxCredCache) getToken(nickname string) (Token, bool) {
	key, err := c.sessionKeyring.Search(nickname)
	if err != nil {
		return nil, false
	}

	buf, err := key.Get()
	if err != nil {
		return nil, false
	}

	var out token
	err = json.Unmarshal(buf, &out)
	if err != nil {
		return nil, false
	}

	return &out, true
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
	return c.persistIndex() == nil
}

func (c *linuxCredCache) SaveToken(tok Token) error {
	c.init()
	c.lock.Lock()
	defer c.lock.Unlock()

	info, ok := tok.(*token)
	if !ok {
		return fmt.Errorf("unsupported token type %T", tok)
	}

	buf, err := json.Marshal(info)
	if err != nil {
		return err
	}

	key, err := c.addOrUpdateKey(info.Nickname, buf)
	if err != nil {
		return err
	}

	c.fetchedKeysCache[info.Nickname] = credCacheEntry{
		Header: info.TokenHeader,
		Key:    key,
	}

	return c.persistIndex()
}

func (c *linuxCredCache) addOrUpdateKey(name string, data []byte) (*keyctl.Key, error) {
	key, err := c.sessionKeyring.Add(name, data)
	if errors.Is(err, syscall.EEXIST) {
		key, err = c.sessionKeyring.Search(name)
		if err != nil {
			return nil, err
		}
		return key, key.Set(data)
	}
	if err != nil {
		return nil, err
	}

	if err = keyctl.SetPerm(key, keyctl.PermUserAll); err != nil {
		if unlinkErr := key.Unlink(); unlinkErr != nil {
			panic(errors.New("failed to set permissions, and cannot unlink key, for security reasons it is recommended to log out of the current session"))
		}
		return nil, fmt.Errorf("failed to set permission for cached token: %w", err)
	}
	return key, nil
}

func (c *linuxCredCache) persistIndex() error {
	buf, err := json.Marshal(c.fetchedKeysCache)
	if err != nil {
		return err
	}
	_, err = c.addOrUpdateKey(c.rootKeyName, buf)
	return err
}

func (c *linuxCredCache) keyringImpl() {}

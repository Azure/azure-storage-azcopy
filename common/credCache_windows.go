// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"
)

// CredCache manages credential caches.
type CredCache struct {
	dpapiFilePath string
	entropy       *dataBlob
	lock          sync.Mutex
}

const azcopyverbose = "azcopyverbose"
const defaultTokenFileName = "accessToken.json"

// NewCredCache creates a cred cache.
func NewCredCache(options CredCacheOptions) *CredCache {
	return &CredCache{
		dpapiFilePath: options.DPAPIFilePath,
		entropy:       newDataBlob([]byte(azcopyverbose)),
	}
}

// HasCachedToken returns if there is cached token for current executing user.
func (c *CredCache) HasCachedToken() (bool, error) {
	c.lock.Lock()
	has, err := c.hasCachedTokenInternal()
	c.lock.Unlock()
	return has, err
}

// RemoveCachedToken deletes the cached token.
func (c *CredCache) RemoveCachedToken() error {
	c.lock.Lock()
	err := c.removeCachedTokenInternal()
	c.lock.Unlock()
	return err
}

// SaveToken saves an oauth token.
func (c *CredCache) SaveToken(token OAuthTokenInfo) error {
	c.lock.Lock()
	err := c.saveTokenInternal(token)
	c.lock.Unlock()
	return err
}

// LoadToken gets the cached oauth token.
func (c *CredCache) LoadToken() (*OAuthTokenInfo, error) {
	c.lock.Lock()
	token, err := c.loadTokenInternal()
	c.lock.Unlock()
	return token, err
}

///////////////////////////////////////////////////////////////////////////////////////////////
// This internal method pattern is applied to avoid defer locks.
// The reason is:
// We use locks to protect shared state from being accessed from multiple threads/goroutines at the same time.
// If a bug is in this method that causes a panic,
// then the defer will unlock another thread/goroutine allowing it to access the shared state.
// BUT, if a panic happened, the shared state is hard to be decide whether in a good or corrupted state.
// So currently let the other threads/goroutines hang forever instead of letting them access the potentially corrupted shared state.
// Once having bad state, more bad state gets injected into app and figuring out how it happened and how to recover from it is near impossible.
// On the other hand, hanging threads is MUCH easier to detect and devs can fix the bug in code to make sure that the panic doesn't happen in the first place.
///////////////////////////////////////////////////////////////////////////////////////////////

// hasCachedTokenInternal returns if there is cached token in token manager.
func (c *CredCache) hasCachedTokenInternal() (bool, error) {
	if _, err := os.Stat(c.tokenFilePath()); err == nil {
		return true, nil
	} else {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
}

// removeCachedTokenInternal deletes all the cached token.
func (c *CredCache) removeCachedTokenInternal() error {
	tokenFilePath := c.tokenFilePath()

	if _, err := os.Stat(tokenFilePath); err == nil {
		// Cached token file existed
		err = os.Remove(tokenFilePath)
		if err != nil { // remove failed
			return fmt.Errorf("failed to remove cached token file with path %q, %v", tokenFilePath, err)
		}

		// remove succeeded
	} else {
		if !os.IsNotExist(err) { // Failed to stat cached token file
			return fmt.Errorf("failed to stat cached token file with path %q during removing, %v", tokenFilePath, err)
		}

		// token doesn't exist
		return errors.New("no cached token found for current user")
	}

	return nil
}

// loadTokenInternal restores a Token object from file cache.
func (c *CredCache) loadTokenInternal() (*OAuthTokenInfo, error) {
	tokenFilePath := c.tokenFilePath()
	b, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read token file %q during loading token: %v", tokenFilePath, err)
	}

	decryptedB, err := decrypt(b, c.entropy)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt bytes during loading token: %v", err)
	}

	token, err := jsonToTokenInfo(decryptedB)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal token during loading token, %v", err)
	}

	return token, nil
}

// saveTokenInternal persists an oauth token on disk.
// It moves the new file into place so it can safely be used to replace an existing file
// that maybe accessed by multiple processes.
func (c *CredCache) saveTokenInternal(token OAuthTokenInfo) error {
	tokenFilePath := c.tokenFilePath()
	dir := filepath.Dir(tokenFilePath)

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory %q to store token in, %v", dir, err)
	}

	newFile, err := os.CreateTemp(dir, "token")
	if err != nil {
		return fmt.Errorf("failed to create the temp file to write the token, %v", err)
	}
	tempPath := newFile.Name()

	json, err := token.toJSON()
	if err != nil {
		return fmt.Errorf("failed to marshal token, %v", err)
	}

	b, err := encrypt(json, c.entropy)
	if err != nil {
		return fmt.Errorf("failed to encrypt token, %v", err)
	}

	if _, err = newFile.Write(b); err != nil {
		return fmt.Errorf("failed to encode token to file %q while saving token, %v", tempPath, err)
	}

	if err := newFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file %q, %v", tempPath, err)
	}

	// Atomic replace to avoid multi-writer file corruptions
	if err := os.Rename(tempPath, tokenFilePath); err != nil {
		return fmt.Errorf("failed to move temporary token to desired output location. src=%q dst=%q, %v", tempPath, tokenFilePath, err)
	}
	if err := os.Chmod(tokenFilePath, 0600); err != nil { // read/write for current user
		return fmt.Errorf("failed to chmod the token file %q, %v", tokenFilePath, err)
	}
	return nil
}

func (c *CredCache) tokenFilePath() string {
	if cacheFile := GetEnvironmentVariable(EEnvironmentVariable.LoginCacheName()); cacheFile != "" {
		return path.Join(c.dpapiFilePath, "/", cacheFile)
	}

	return path.Join(c.dpapiFilePath, "/", defaultTokenFileName)
}

// ======================================================================================
// DPAPI facilities
// ======================================================================================

var dCrypt32 = syscall.NewLazyDLL("crypt32.dll") // lower case to tie in with Go's sysdll registration
var dKernel32 = syscall.NewLazyDLL("kernel32.dll")

// Refer to https://msdn.microsoft.com/en-us/library/windows/desktop/aa380261(v=vs.85).aspx for more details.
var mCryptProtectData = dCrypt32.NewProc("CryptProtectData")

// Refer to https://msdn.microsoft.com/en-us/library/windows/desktop/aa380882(v=vs.85).aspx for more details.
var mCryptUnprotectData = dCrypt32.NewProc("CryptUnprotectData")

// Refer to https://msdn.microsoft.com/en-us/library/windows/desktop/aa366730(v=vs.85).aspx for more details.
var mLocalFree = dKernel32.NewProc("LocalFree")

// dwFlags for protection. Remote situations where presenting a user interface (UI) is not an option.
// When this flag is set and a UI is specified for either the protect or unprotect operation, the operation fails and GetLastError returns the ERROR_PASSWORD_RESTRICTION code.
const cryptProtectUIForbidden = 0x1

type dataBlob struct {
	cbData uint32
	pbData *byte
}

func newDataBlob(d []byte) *dataBlob {
	if len(d) == 0 {
		return &dataBlob{}
	}
	return &dataBlob{
		cbData: uint32(len(d)),
		pbData: &d[0],
	}
}

func (b dataBlob) toByteArray() []byte {
	d := make([]byte, b.cbData)
	copy(d, (*[1 << 30]byte)(unsafe.Pointer(b.pbData))[:])
	return d
}

func encrypt(data []byte, entropy *dataBlob) ([]byte, error) {
	if entropy == nil {
		return nil, errors.New("entropy is enforced in AzCopy")
	}

	var outblob dataBlob
	defer func() {
		if outblob.pbData != nil {
			_, _, _ = mLocalFree.Call(uintptr(unsafe.Pointer(outblob.pbData)))
		}
	}()

	r, _, err := mCryptProtectData.Call(
		uintptr(unsafe.Pointer(newDataBlob(data))),
		0,
		uintptr(unsafe.Pointer(entropy)),
		0,
		0,
		cryptProtectUIForbidden,
		uintptr(unsafe.Pointer(&outblob)))
	if r == 0 {
		return nil, err
	}

	return outblob.toByteArray(), nil
}

func decrypt(data []byte, entropy *dataBlob) ([]byte, error) {
	if entropy == nil {
		return nil, errors.New("entropy is enforced in AzCopy")
	}

	var outblob dataBlob
	defer func() {
		if outblob.pbData != nil {
			_, _, _ = mLocalFree.Call(uintptr(unsafe.Pointer(outblob.pbData)))
		}
	}()

	r, _, err := mCryptUnprotectData.Call(
		uintptr(unsafe.Pointer(newDataBlob(data))),
		0,
		uintptr(unsafe.Pointer(entropy)),
		0,
		0,
		cryptProtectUIForbidden,
		uintptr(unsafe.Pointer(&outblob)))
	if r == 0 {
		return nil, err
	}

	return outblob.toByteArray(), nil
}

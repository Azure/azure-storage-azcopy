//go:build windows

package cred

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"unsafe"

	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
	"github.com/Azure/azure-storage-azcopy/v10/common/ternary"
)

const (
	// not sure the reasoning behind this naming scheme, or the constant status...?
	// I may try to correct this...
	azcopyverbose        = "azcopyverbose"
	defaultTokenFileName = "accessToken.json"
)

func GetOSKeyring(opts GetOSKeyringOptions) (Keyring, error) {
	dpapiFolder, dpapiFilename := filepath.Split(*ternary.DefaultValue(opts.DPAPIFilePath, filepath.Join(enum.EEnvironmentVariable.AppDir().Get(), defaultTokenFileName)))
	if opts.OSKeyringCacheName != nil {
		dpapiFolder = filepath.Join(dpapiFolder, *opts.OSKeyringCacheName)
	}

	out := &windowsCredCache{
		keyringData:   make(map[string]token),
		dpapiFilePath: filepath.Join(dpapiFolder, dpapiFilename),
		entropy:       newDataBlob([]byte(azcopyverbose)),
		lock:          sync.RWMutex{},
	}

	return out, out.loadTokens()
}

type windowsCredCache struct {
	keyringData map[string]token

	dpapiFilePath string
	entropy       *dataBlob
	lock          sync.RWMutex
}

func (c *windowsCredCache) ListTokens() ([]TokenHeader, error) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	out := make([]TokenHeader, 0)
	for _, v := range c.keyringData {
		out = append(out, v.TokenHeader)
	}

	return out, nil
}

func (c *windowsCredCache) loadTokens() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	buf, err := os.ReadFile(c.dpapiFilePath)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to read token file %q during loading token: %w", c.dpapiFilePath, err)
		}

		return nil
	}

	decBuf, err := decrypt(buf, c.entropy)
	if err != nil {
		return fmt.Errorf("failed to decrypt bytes during loading token: %w", err)
	}

	err = json.Unmarshal(decBuf, &c.keyringData)
	if err != nil {
		return fmt.Errorf("failed to unmarshal token during loading token, %w", err)
	}

	return nil
}

// writeTokens does *not* obtain the write lock; it must be obtained before calling.
func (c *windowsCredCache) writeTokens() error {
	buf, err := json.Marshal(c.keyringData)
	if err != nil {
		return fmt.Errorf("failed to marshal token file: %w", err)
	}

	buf, err = encrypt(buf, c.entropy)
	if err != nil {
		return fmt.Errorf("failed to encrypt bytes during saving tokens: %w", err)
	}

	dir := filepath.Dir(c.dpapiFilePath)

	err = os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory %q to store token in, %v", dir, err)
	}

	newFile, err := os.CreateTemp(dir, "token")
	if err != nil {
		return fmt.Errorf("failed to create the temp file to write the token, %v", err)
	}
	tempPath := newFile.Name()

	if _, err = newFile.Write(buf); err != nil {
		return fmt.Errorf("failed to encode token to file %q while saving token, %v", tempPath, err)
	}

	if err := newFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file %q, %v", tempPath, err)
	}

	// Atomic replace to avoid multi-writer file corruptions
	if err := os.Rename(tempPath, c.dpapiFilePath); err != nil {
		return fmt.Errorf("failed to move temporary token to desired output location. src=%q dst=%q, %v", tempPath, c.dpapiFilePath, err)
	}
	if err := os.Chmod(c.dpapiFilePath, 0600); err != nil { // read/write for current user
		return fmt.Errorf("failed to chmod the token file %q, %v", c.dpapiFilePath, err)
	}

	return err
}

func (c *windowsCredCache) GetToken(nickname string) (Token, bool) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	if nickname == "" {
		nickname = DefaultNickname
	}

	token, ok := c.keyringData[nickname]
	if !ok && nickname != DefaultNickname {
		token, ok = c.keyringData[DefaultNickname]
	}

	return &token, ok
}

func (c *windowsCredCache) DeleteToken(nickname string) bool {
	if nickname == "" {
		nickname = DefaultNickname
	}

	c.lock.Lock()
	defer c.lock.Unlock()

	if _, ok := c.keyringData[nickname]; !ok {
		return false
	}

	delete(c.keyringData, nickname)
	err := c.writeTokens()
	if err != nil {
		// if this fails too, it's probably not any worse than failing to write.
		_ = c.loadTokens()
		return false
	}

	return true
}

func (c *windowsCredCache) SaveToken(tok Token) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	info := tok.(*token)

	c.keyringData[info.Nickname] = *info
	err := c.writeTokens()
	if err != nil {
		delete(c.keyringData, info.Nickname)
	}

	return err
}

func (c *windowsCredCache) keyringImpl() {}

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

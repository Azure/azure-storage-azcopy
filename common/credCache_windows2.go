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
	"syscall"
	"unsafe"
)

var gEncryptionUtil EncryptionUtil

type EncryptionUtil struct{}

// // HasCachedToken returns if there is cached token in token manager.
// func (uotm *UserOAuthTokenManager) HasCachedToken() bool {
// 	fmt.Println("uotm", "HasCachedToken", uotm.tokenFilePath())
// 	if _, err := os.Stat(uotm.tokenFilePath()); err == nil {
// 		return true
// 	}
// 	return false
// }

// // RemoveCachedToken delete all the cached token.
// func (uotm *UserOAuthTokenManager) RemoveCachedToken() error {
// 	tokenFilePath := uotm.tokenFilePath()

// 	if _, err := os.Stat(tokenFilePath); err == nil {
// 		// Cached token file existed
// 		err = os.Remove(tokenFilePath)
// 		if err != nil { // remove failed
// 			return fmt.Errorf("failed to remove cached token file with path: %s, due to error: %v", tokenFilePath, err.Error())
// 		}

// 		// remove succeeded
// 	} else {
// 		if !os.IsNotExist(err) { // Failed to stat cached token file
// 			return fmt.Errorf("fail to stat cached token file with path: %s, due to error: %v", tokenFilePath, err.Error())
// 		}

// 		//token doesn't exist
// 		fmt.Println("no cached token found for current user.")
// 	}

// 	return nil
// }

// func (uotm *UserOAuthTokenManager) tokenFilePath() string {
// 	return path.Join(uotm.userTokenCachePath, "/", defaultTokenFileName)
// }

// func (uotm *UserOAuthTokenManager) loadTokenInfo() (*OAuthTokenInfo, error) {
// 	token, err := uotm.loadTokenInfoInternal(uotm.tokenFilePath())
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to load token from cache: %v", err)
// 	}

// 	return token, nil
// }

// // LoadToken restores a Token object from a file located at 'path'.
// func (uotm *UserOAuthTokenManager) loadTokenInfoInternal(path string) (*OAuthTokenInfo, error) {
// 	b, err := ioutil.ReadFile(path)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to read token file (%s) while loading token: %v", path, err)
// 	}

// 	decryptedB, err := decrypt(b)
// 	if err != nil {
// 		return nil, fmt.Errorf("fail to decrypt bytes: %s", err.Error())
// 	}

// 	token, err := JSONToTokenInfo(decryptedB)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to unmarshal token, due to error: %s", err.Error())
// 	}

// 	return token, nil
// }

// func (uotm *UserOAuthTokenManager) saveTokenInfo(token OAuthTokenInfo) error {
// 	err := uotm.saveTokenInfoInternal(uotm.tokenFilePath(), 0600, token) // Save token with read/write permissions for the owner of the file.
// 	if err != nil {
// 		return fmt.Errorf("failed to save token to cache: %v", err)
// 	}
// 	return nil
// }

// // saveTokenInternal persists an oauth token at the given location on disk.
// // It moves the new file into place so it can safely be used to replace an existing file
// // that maybe accessed by multiple processes.
// // get from adal and optimzied to involve more token info.
// func (uotm *UserOAuthTokenManager) saveTokenInfoInternal(path string, mode os.FileMode, token OAuthTokenInfo) error {
// 	dir := filepath.Dir(path)
// 	err := os.MkdirAll(dir, os.ModePerm)
// 	if err != nil {
// 		return fmt.Errorf("failed to create directory (%s) to store token in: %v", dir, err)
// 	}

// 	newFile, err := ioutil.TempFile(dir, "token")
// 	if err != nil {
// 		return fmt.Errorf("failed to create the temp file to write the token: %v", err)
// 	}
// 	tempPath := newFile.Name()

// 	json, err := token.ToJSON()
// 	if err != nil {
// 		return fmt.Errorf("failed to marshal token, due to error: %s", err.Error())
// 	}

// 	b, err := encrypt(json)
// 	if err != nil {
// 		return fmt.Errorf("failed to encrypt token: %v", err)
// 	}

// 	if _, err = newFile.Write(b); err != nil {
// 		return fmt.Errorf("failed to encode token to file (%s) while saving token: %v", tempPath, err)
// 	}

// 	if err := newFile.Close(); err != nil {
// 		return fmt.Errorf("failed to close temp file %s: %v", tempPath, err)
// 	}

// 	// Atomic replace to avoid multi-writer file corruptions
// 	if err := os.Rename(tempPath, path); err != nil {
// 		return fmt.Errorf("failed to move temporary token to desired output location. src=%s dst=%s: %v", tempPath, path, err)
// 	}
// 	if err := os.Chmod(path, mode); err != nil {
// 		return fmt.Errorf("failed to chmod the token file %s: %v", path, err)
// 	}
// 	return nil
// }

// ======================================================================================
// DPAPI facilities

var dCrypt32 = syscall.NewLazyDLL("Crypt32.dll")
var dKernel32 = syscall.NewLazyDLL("Kernel32.dll")

// Refer to https://msdn.microsoft.com/en-us/library/windows/desktop/aa380261(v=vs.85).aspx for more details.
var mCryptProtectData = dCrypt32.NewProc("CryptProtectData")

// Refer to https://msdn.microsoft.com/en-us/library/windows/desktop/aa380882(v=vs.85).aspx for more details.
var mCryptUnprotectData = dCrypt32.NewProc("CryptUnprotectData")

// Refer to https://msdn.microsoft.com/en-us/library/windows/desktop/aa366730(v=vs.85).aspx for more details.
var mLocalFree = dKernel32.NewProc("LocalFree")

// This flag is used for remote situations where presenting a user interface (UI) is not an option.
// When this flag is set and a UI is specified for either the protect or unprotect operation, the operation fails and GetLastError returns the ERROR_PASSWORD_RESTRICTION code.
const cryptProtectUIForbidden = 1

type dataBlob struct {
	cbData uint32
	pbData *byte
}

func newDataBlob(d []byte) *dataBlob {
	if len(d) == 0 {
		return &dataBlob{}
	}
	return &dataBlob{
		pbData: &d[0],
		cbData: uint32(len(d)),
	}
}

func (b dataBlob) toByteArray() []byte {
	d := make([]byte, b.cbData)
	copy(d, (*[1 << 30]byte)(unsafe.Pointer(b.pbData))[:])
	return d
}

func (EncryptionUtil) Encrypt(data []byte) ([]byte, error) {
	var outblob dataBlob
	r, _, err := mCryptProtectData.Call(uintptr(unsafe.Pointer(newDataBlob(data))), 0, 0, 0, 0, cryptProtectUIForbidden, uintptr(unsafe.Pointer(&outblob)))
	if r == 0 {
		return nil, err
	}
	defer mLocalFree.Call(uintptr(unsafe.Pointer(outblob.pbData)))
	return outblob.toByteArray(), nil
}

func (EncryptionUtil) Decrypt(data []byte) ([]byte, error) {
	var outblob dataBlob
	r, _, err := mCryptUnprotectData.Call(uintptr(unsafe.Pointer(newDataBlob(data))), 0, 0, 0, 0, cryptProtectUIForbidden, uintptr(unsafe.Pointer(&outblob)))
	if r == 0 {
		return nil, err
	}
	defer mLocalFree.Call(uintptr(unsafe.Pointer(outblob.pbData)))
	return outblob.toByteArray(), nil
}

func (EncryptionUtil) IsEncryptionRobust() bool {
	return true
}

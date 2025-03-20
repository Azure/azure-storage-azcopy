package common

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/danieljoos/wincred"
	"github.com/stretchr/testify/assert"
	"testing"
)

// Validates the SHA256 hash from LoadToken()
func TestCredCacheInternalIntegration_LoadToken(t *testing.T) {
	// Mock OAuth token
	token := OAuthTokenInfo{
		Token: Token{
			AccessToken: "mocked_access_token",
			ExpiresIn:   "0",
			ExpiresOn:   "0",
			NotBefore:   "0",
		},
	}
	// Serialize the token to bytes
	tokenBytes, err := json.Marshal(token)
	assert.Nil(t, err)

	sha256Hash := fmt.Sprintf("%x", sha256.Sum256(tokenBytes))
	header := segmentedTokenHeader{SegmentNum: `1`, SHA256Hash: sha256Hash}
	// Serialize the header to bytes
	headerBytes, err := json.Marshal(header)
	assert.Nil(t, err)

	// Mock Windows Credential Manager
	cred := wincred.NewGenericCredential("testTarget")
	cred.CredentialBlob = headerBytes
	err = cred.Write()
	assert.Nil(t, err)

	// Mock segmented token
	segmentedCred := wincred.NewGenericCredential("testTarget/1")
	segmentedCred.CredentialBlob = tokenBytes
	err = segmentedCred.Write()
	assert.Nil(t, err)

	internalInt := NewCredCacheInternalIntegration(CredCacheOptions{KeyName: "testTarget"})

	loadedToken, err := internalInt.LoadToken() // Call function
	assert.Nil(t, err)
	assert.Equal(t, token, *loadedToken) // Validate consistency of token returned from LoadToken

}

package common

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_VerifyIsURLResolvable(t *testing.T) {
	a := assert.New(t)
	t.Skip("Disabled the check in mainline code")
	valid_url := "https://github.com/"
	invalidUrl := "someString"
	invalidUrl2 := "https://$invalidAccount.blob.core.windows.net/"

	a.Nil(VerifyIsURLResolvable(valid_url))
	a.NotNil(VerifyIsURLResolvable(invalidUrl))
	a.NotNil(VerifyIsURLResolvable(invalidUrl2))
}

func Test_TryAddMetadata(t *testing.T) {
	a := assert.New(t)

	// Test case 1: metadata is empty
	metadata := make(Metadata)
	TryAddMetadata(metadata, "key", "value")
	a.Equal(1, len(metadata))
	a.Contains(metadata, "key")
	a.Equal("value", *metadata["key"])

	// Test case 2: metadata contains exact key
	metadata = make(Metadata)
	metadata["key"] = to.Ptr("value")
	TryAddMetadata(metadata, "key", "new_value")
	a.Equal(1, len(metadata))
	a.Contains(metadata, "key")
	a.Equal("value", *metadata["key"])

	// Test case 3: metadata contains key with different case
	metadata = make(Metadata)
	metadata["Key"] = to.Ptr("value")
	TryAddMetadata(metadata, "key", "new_value")
	a.Equal(1, len(metadata))
	a.Contains(metadata, "Key")
	a.Equal("value", *metadata["Key"])
	a.NotContains(metadata, "key")

	// Test case 4: metadata is not empty and does not contain key
	metadata = make(Metadata)
	metadata["other_key"] = to.Ptr("value")
	TryAddMetadata(metadata, "key", "new_value")
	a.Equal(2, len(metadata))
	a.Contains(metadata, "key")
	a.Equal("new_value", *metadata["key"])
}

func Test_TryReadMetadata(t *testing.T) {
	a := assert.New(t)
	// Test case 1: metadata is empty
	metadata := make(Metadata)
	v, ok := TryReadMetadata(metadata, "key")
	a.False(ok)
	a.Nil(v)

	// Test case 2: metadata contains exact key
	metadata = make(Metadata)
	metadata["key"] = to.Ptr("value")
	v, ok = TryReadMetadata(metadata, "key")
	a.True(ok)
	a.NotNil(v)
	a.Equal("value", *v)

	// Test case 3: metadata contains key with different case
	metadata = make(Metadata)
	metadata["Key"] = to.Ptr("value")
	v, ok = TryReadMetadata(metadata, "key")
	a.True(ok)
	a.NotNil(v)
	a.Equal("value", *v)

	// Test case 4: metadata is not empty and does not contain key
	metadata = make(Metadata)
	metadata["other_key"] = to.Ptr("value")
	v, ok = TryReadMetadata(metadata, "key")
	a.False(ok)
	a.Nil(v)
}

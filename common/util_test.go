package common

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	fileService "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/service"
	"github.com/stretchr/testify/assert"
)

func GetAccountAndKey() (string, string) {
	name := os.Getenv("ACCOUNT_NAME")
	key := os.Getenv("ACCOUNT_KEY")
	if name == "" || key == "" {
		panic("ACCOUNT_NAME and ACCOUNT_KEY environment vars must be set before running tests")
	}

	return name, key
}

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

func TestDoWithOverrideReadonlyonAzureFiles(t *testing.T) {
	a := assert.New(t)

	acc, key := GetAccountAndKey()
	rawURL := fmt.Sprintf("https://%s.file.core.windows.net/", acc)

	credential, err := file.NewSharedKeyCredential(acc, key)
	a.Nil(err)

	fsc, err := fileService.NewClientWithSharedKeyCredential(rawURL, credential, nil)
	a.Nil(err)

	sc := fsc.NewShareClient(NewUUID().String())
	_, err = sc.Create(context.TODO(), nil)
	a.Nil(err)

	defer sc.Delete(context.Background(), nil)

	f := sc.NewRootDirectoryClient().NewFileClient("testfile")
	_, err = f.Create(context.TODO(), 0, &file.CreateOptions{
		SMBProperties: &file.SMBProperties{
			Attributes: &file.NTFSFileAttributes{
				ReadOnly: true,
			},
		},
	})

	a.Nil(err)

	testMeta := make(map[string]*string)
	testMeta["Testkey"] = to.Ptr("Testvalue")

	//verify that with force=false, we'll still fail the op.
	err = DoWithOverrideReadOnlyOnAzureFiles(context.TODO(), func() (interface{}, error) {
		return f.Create(context.TODO(), 0, &file.CreateOptions{
			Metadata: testMeta,
		})
	}, f, false)
	a.Contains(err.Error(), "target is readonly")

	// verify with force=true, we'll overwrite
	err = DoWithOverrideReadOnlyOnAzureFiles(context.TODO(), func() (interface{}, error) {
		return f.Create(context.TODO(), 0, &file.CreateOptions{
			Metadata: testMeta,
		})
	}, f, true)
	a.Nil(err)

	props, err := f.GetProperties(context.TODO(), nil)
	a.Nil(err)
	a.Equal(*props.Metadata["Testkey"], "Testvalue")

}

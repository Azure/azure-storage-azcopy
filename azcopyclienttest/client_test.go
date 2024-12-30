package azcopyclient

import (
	"github.com/Azure/azure-storage-azcopy/v10/azcopyclient"
	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestClient(t *testing.T) {
	a := assert.New(t)

	client := azcopyclient.Client{
		ClientOptions: azcopyclient.ClientOptions{
			SkipVersionCheck: true,
		},
	}
	err := client.Initialize(common.JobID{}, false)

	a.NotNil(client)
	a.Nil(err)
}

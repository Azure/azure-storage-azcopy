package e2etest

import (
	"fmt"
	"github.com/google/uuid"
	"net/http"
	"strings"
	"time"
)

var CommonARMClient *ARMClient
var CommonARMResourceGroup *ARMResourceGroup // separated in case needed.

func SetupArmClient(a Asserter) {
	if GlobalConfig.StaticResources() {
		return // no setup
	}

	maxRetries := 2
	var err error
	var spt *AzCoreAccessToken
	for i := 0; i < maxRetries; i++ {
		spt, err = PrimaryOAuthCache.GetAccessToken(AzureManagementResource)
		if err == nil {
			break
		}
	}
	a.NoError("get management access token", err)

	CommonARMClient = &ARMClient{
		OAuth:      spt,
		HttpClient: http.DefaultClient, // todo if we want something more special
	}

	uuidSegments := strings.Split(uuid.NewString(), "-")

	CommonARMResourceGroup = CommonARMClient.
		NewSubscriptionClient(GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.SubscriptionID).
		NewResourceGroupClient("azcopy-newe2e-" + uuidSegments[len(uuidSegments)-1])

	_, err = CommonARMResourceGroup.CreateOrUpdate(ARMResourceGroupCreateParams{
		Location: "West US", // todo configurable
		Tags: map[string]string{
			"creation": fmt.Sprintf("%d", time.Now().UTC().Unix()),
		},
	})
	a.NoError("create resource group", err)
}

func TeardownArmClient(a Asserter) {
	if GlobalConfig.StaticResources() {
		return // no need to attempt cleanup
	}

	a.NoError("delete resource group", CommonARMResourceGroup.Delete(nil))
}

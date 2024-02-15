package e2etest

import (
	"github.com/google/uuid"
	"net/http"
	"strings"
)

var CommonARMClient *ARMClient
var CommonARMResourceGroup *ARMResourceGroup // separated in case needed.

func SetupArmClient(a Asserter) {
	if GlobalConfig.StaticResources() {
		return // no setup
	}

	spt, err := PrimaryOAuthCache.GetAccessToken(AzureManagementResource)
	a.NoError("get management access token", err)

	CommonARMClient = &ARMClient{
		OAuth:      spt,
		HttpClient: http.DefaultClient, // todo if we want something more special
	}

	uuidSegments := strings.Split(uuid.NewString(), "-")

	CommonARMResourceGroup = &ARMResourceGroup{
		ARMSubscription: &ARMSubscription{
			ARMClient:      CommonARMClient,
			SubscriptionID: GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.SubscriptionID,
		},
		ResourceGroupName: "azcopy-newe2e-" + uuidSegments[len(uuidSegments)-1],
	}

	_, err = CommonARMResourceGroup.CreateOrUpdate(ARMResourceGroupCreateParams{
		Location: "West US", // todo configurable
	})
	a.NoError("create resource group", err)
}

func TeardownArmClient(a Asserter) {
	if GlobalConfig.StaticResources() {
		return // no need to attempt cleanup
	}

	a.NoError("delete resource group", CommonARMResourceGroup.Delete(nil))
}

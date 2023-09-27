package e2etest

import (
	"fmt"
	"github.com/google/uuid"
	"net/http"
)

var CommonARMClient *ARMClient
var CommonARMResourceGroup *ARMResourceGroup // separated in case needed.

func SetupArmClient() TieredError {
	if GlobalConfig.StaticResources() {
		return nil // no setup
	}

	spt, err := PrimaryOAuthCache.GetAccessToken(AzureManagementResource)
	if err != nil {
		return TieredErrorWrapper{
			error:     fmt.Errorf("getting OAuth token: %w", err),
			ErrorTier: ErrorTierFatal,
		}
	}

	CommonARMClient = &ARMClient{
		OAuth:      spt,
		HttpClient: http.DefaultClient, // todo if we want something more special
	}

	CommonARMResourceGroup = &ARMResourceGroup{
		ARMSubscription: &ARMSubscription{
			ARMClient:      CommonARMClient,
			SubscriptionID: GlobalConfig.E2EAuthConfig.SubscriptionLoginInfo.SubscriptionID,
		},
		ResourceGroupName: "azcopy-newe2e-" + uuid.NewString(),
	}

	return nil
}

func TeardownArmClient() TieredError {
	err := CommonARMResourceGroup.Delete(nil)
	if err != nil {
		return TieredErrorWrapper{
			error:     fmt.Errorf("deleting resource group %s: %w", CommonARMResourceGroup.ResourceGroupName, err),
			ErrorTier: ErrorTierFatal,
		}
	}

	return nil
}

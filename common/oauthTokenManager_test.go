package common

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	chk "gopkg.in/check.v1"
	"time"
)

type oauthTokenManagerTestSuite struct{}

var _ = chk.Suite(&oauthTokenManagerTestSuite{})

// Tests toJSON and jsonToTokenInfo
func (s *oauthTokenManagerTestSuite) TestJson(c *chk.C) {
	expiry, err := time.Parse("2006-01-02T15:04:05.999999999Z", "2031-11-02T21:17:37.345154365Z")
	//expiry := time.Now().Add(12 * time.Hour)
	oauthTokenInfo := OAuthTokenInfo{
		AccessToken: azcore.AccessToken{
			Token:     "faketoken",
			ExpiresOn: expiry,
		},
		Tenant:                  "faketenant",
		ActiveDirectoryEndpoint: "fakeadendpoint",
		TokenRefreshSource:      "tokenrefresh",
		ApplicationID:           "fakeappid",
		Identity:                true,
		IdentityInfo: IdentityInfo{
			ClientID: "fakeclientid",
			ObjectID: "fakeobjid",
			MSIResID: "fakeresid",
		},
		ServicePrincipalName: true,
		Resource:             "fakeresource",
		SPNInfo: SPNInfo{
			CertPath: "fakecertpath",
			Secret:   "fakesecret",
		},
		ClientID: "fakeclientid",
	}

	// toJSON test
	output, err := oauthTokenInfo.toJSON()
	c.Assert(err, chk.IsNil)
	outputString := string(output)
	expectedString := "{\"Token\":\"faketoken\",\"ExpiresOn\":\"" + expiry.Format("2006-01-02T15:04:05.999999999Z") + "\",\"_tenant\":\"faketenant\",\"_ad_endpoint\":\"fakeadendpoint\",\"_token_refresh_source\":\"tokenrefresh\",\"_application_id\":\"fakeappid\",\"_identity\":true,\"IdentityInfo\":{\"_identity_client_id\":\"fakeclientid\",\"_identity_object_id\":\"fakeobjid\",\"_identity_msi_res_id\":\"fakeresid\"},\"_spn\":true,\"_resource\":\"fakeresource\",\"SPNInfo\":{\"_spn_secret\":\"fakesecret\",\"_spn_cert_path\":\"fakecertpath\"},\"_client_id\":\"fakeclientid\"}"
	c.Assert(outputString, chk.Equals, expectedString)

	// jsonToTokenInfo test
	info, err := jsonToTokenInfo(output)
	c.Assert(err, chk.IsNil)
	c.Assert(*info, chk.DeepEquals, oauthTokenInfo)
	c.Assert(info.Token, chk.Equals, oauthTokenInfo.Token)
	c.Assert(info.ExpiresOn, chk.Equals, expiry)
	c.Assert(oauthTokenInfo.ExpiresOn, chk.Equals, expiry)
	c.Assert(info.Tenant, chk.Equals, oauthTokenInfo.Tenant)
	c.Assert(info.ActiveDirectoryEndpoint, chk.Equals, oauthTokenInfo.ActiveDirectoryEndpoint)
	c.Assert(info.TokenRefreshSource, chk.Equals, oauthTokenInfo.TokenRefreshSource)
	c.Assert(info.ApplicationID, chk.Equals, oauthTokenInfo.ApplicationID)
	c.Assert(info.Identity, chk.Equals, oauthTokenInfo.Identity)
	c.Assert(info.IdentityInfo, chk.DeepEquals, oauthTokenInfo.IdentityInfo)
	c.Assert(info.IdentityInfo.ClientID, chk.Equals, oauthTokenInfo.IdentityInfo.ClientID)
	c.Assert(info.IdentityInfo.ObjectID, chk.Equals, oauthTokenInfo.IdentityInfo.ObjectID)
	c.Assert(info.IdentityInfo.MSIResID, chk.Equals, oauthTokenInfo.IdentityInfo.MSIResID)
	c.Assert(info.ServicePrincipalName, chk.Equals, oauthTokenInfo.ServicePrincipalName)
	c.Assert(info.Resource, chk.Equals, oauthTokenInfo.Resource)
	c.Assert(info.SPNInfo, chk.DeepEquals, oauthTokenInfo.SPNInfo)
	c.Assert(info.SPNInfo.CertPath, chk.Equals, oauthTokenInfo.SPNInfo.CertPath)
	c.Assert(info.SPNInfo.Secret, chk.Equals, oauthTokenInfo.SPNInfo.Secret)
	c.Assert(info.ClientID, chk.Equals, oauthTokenInfo.ClientID)
}

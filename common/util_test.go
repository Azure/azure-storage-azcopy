package common

import chk "gopkg.in/check.v1"

type utilityFunctionsSuite struct{}

var _ = chk.Suite(&utilityFunctionsSuite{})

func (*utilityFunctionsSuite) Test_VerifyIsURLResolvable(c *chk.C) {
	valid_url := "https://github.com/"
	invalidUrl := "someString"
	invalidUrl2 := "https://$invalidAccount.blob.core.windows.net/"

	c.Assert(VerifyIsURLResolvable(valid_url), chk.IsNil)
	c.Assert(VerifyIsURLResolvable(invalidUrl), chk.NotNil)
	c.Assert(VerifyIsURLResolvable(invalidUrl2), chk.NotNil)
}
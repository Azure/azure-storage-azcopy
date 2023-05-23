package common

import (
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
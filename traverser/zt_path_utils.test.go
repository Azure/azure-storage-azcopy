package traverser

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
	chk "gopkg.in/check.v1"
)

type pathUtilsSuite struct{}

var _ = chk.Suite(&pathUtilsSuite{})

func TestStripQueryFromSaslessUrl(t *testing.T) {
	a := assert.New(t)
	tests := []struct {
		full          string
		isRemote      bool
		expectedMain  string
		expectedQuery string
	}{
		// remote urls
		{"http://example.com/abc?foo=bar", true, "http://example.com/abc", "foo=bar"},
		{"http://example.com/abc", true, "http://example.com/abc", ""},
		{"http://example.com/abc?", true, "http://example.com/abc", ""}, // no query string if ? is at very end

		// things that are not URLs, or not to be interpreted as such
		{"http://foo/bar?eee", false, "http://foo/bar?eee", ""}, // note isRemote == false
		{`c:\notUrl`, false, `c:\notUrl`, ""},
		{`\\?\D:\longStyle\Windows\path`, false, `\\?\D:\longStyle\Windows\path`, ""},
	}

	for _, t := range tests {
		loc := common.ELocation.Local()
		if t.isRemote {
			loc = common.ELocation.File()
		}
		m, q := splitQueryFromSaslessResource(t.full, loc)
		a.Equal(t.expectedMain, m)
		a.Equal(t.expectedQuery, q)
	}
}

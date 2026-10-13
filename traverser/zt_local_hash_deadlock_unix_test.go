//go:build unix

package traverser

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func createUnreadableFile(t *testing.T, path string) {
	if os.Geteuid() == 0 {
		t.Skip("root can read files regardless of permissions")
	}
	require.NoError(t, os.WriteFile(path, []byte(path), 0))
}

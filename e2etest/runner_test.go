package e2etest

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestOutputMarkerBufferDetectsMarker(t *testing.T) {
	const marker = "AZCOPY_E2E_ENUMERATION_READY"

	for name, writes := range map[string][]string{
		"single write": {"before " + marker + " after"},
		"split writes": {"before AZCOPY_E2E_ENUM", "ERATION_READY after"},
	} {
		t.Run(name, func(t *testing.T) {
			buffer := newOutputMarkerBuffer(marker)
			for _, value := range writes {
				_, err := buffer.Write([]byte(value))
				require.NoError(t, err)
			}

			select {
			case <-buffer.matched:
			case <-time.After(time.Second):
				t.Fatal("output marker was not detected")
			}
		})
	}

	t.Run("io copy", func(t *testing.T) {
		buffer := newOutputMarkerBuffer(marker)
		_, err := io.Copy(buffer, strings.NewReader("before "+marker+" after"))
		require.NoError(t, err)

		select {
		case <-buffer.matched:
		case <-time.After(time.Second):
			t.Fatal("output marker was not detected")
		}
	})
}

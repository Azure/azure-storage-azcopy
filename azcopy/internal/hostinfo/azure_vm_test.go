package hostinfo

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func smbiosFixture(records ...[]byte) []byte {
	raw := make([]byte, 8)
	for _, record := range records {
		raw = append(raw, record...)
	}
	binary.LittleEndian.PutUint32(raw[4:8], uint32(len(raw)-8))
	return raw
}

func TestAzurePublicCloudAssetTag(t *testing.T) {
	for _, value := range []string{azurePublicCloudAssetTag, azurePublicCloudAssetTag + "\n", " " + azurePublicCloudAssetTag + "\r\n"} {
		require.True(t, isAzurePublicCloudAssetTag(value))
	}
	for _, value := range []string{"", "Microsoft Corporation", "Virtual Machine", "other", azurePublicCloudAssetTag + "extra", strings.Repeat(" ", 128) + azurePublicCloudAssetTag} {
		require.False(t, isAzurePublicCloudAssetTag(value))
	}
}

func TestAzurePublicCloudFromSMBIOS(t *testing.T) {
	chassis := append([]byte{3, 9, 0, 0, 0, 0, 0, 0, 2}, []byte("manufacturer\x00"+azurePublicCloudAssetTag+"\x00\x00")...)
	other := append([]byte{1, 4, 0, 0}, []byte(azurePublicCloudAssetTag+"\x00\x00")...)
	end := []byte{127, 4, 0, 0, 0, 0}
	for _, raw := range [][]byte{smbiosFixture(chassis), smbiosFixture(other, chassis, end)} {
		require.True(t, azurePublicCloudFromSMBIOS(raw))
	}
	t.Run("declared length shorter than input", func(t *testing.T) {
		raw := append(smbiosFixture(chassis), end...)
		declaredLength := binary.LittleEndian.Uint32(raw[4:8])
		require.Positive(t, declaredLength)
		require.Less(t, uint64(declaredLength), uint64(len(raw)-8))
		require.True(t, azurePublicCloudFromSMBIOS(raw))
	})
	for name, raw := range map[string][]byte{
		"empty":                 nil,
		"header only":           make([]byte, 8),
		"short header":          make([]byte, 7),
		"other record":          smbiosFixture(other),
		"end before chassis":    smbiosFixture(end, chassis),
		"short chassis":         smbiosFixture([]byte{3, 4, 0, 0, 0, 0}),
		"invalid record length": smbiosFixture([]byte{3, 3, 0, 0, 0, 0}),
		"record beyond input":   smbiosFixture([]byte{3, 30, 0, 0, 0, 0}),
		"unterminated strings":  smbiosFixture(chassis[:len(chassis)-1]),
		"oversized":             make([]byte, maxSMBIOSTableBytes+1),
	} {
		t.Run(name, func(t *testing.T) { require.False(t, azurePublicCloudFromSMBIOS(raw)) })
	}
	for _, index := range []byte{0, 1, 3, 255} {
		invalid := append([]byte(nil), chassis...)
		invalid[8] = index
		require.False(t, azurePublicCloudFromSMBIOS(smbiosFixture(invalid)))
	}
	valid := smbiosFixture(chassis)
	for length := 0; length < len(valid); length++ {
		require.False(t, azurePublicCloudFromSMBIOS(valid[:length]))
	}
	binary.LittleEndian.PutUint32(valid[4:8], ^uint32(0))
	require.False(t, azurePublicCloudFromSMBIOS(valid))
}

func FuzzAzurePublicCloudFromSMBIOS(f *testing.F) {
	f.Add(smbiosFixture(append([]byte{3, 9, 0, 0, 0, 0, 0, 0, 1}, []byte(azurePublicCloudAssetTag+"\x00\x00")...)))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, raw []byte) { _ = azurePublicCloudFromSMBIOS(raw) })
}

func TestAzurePublicCloudFromFirmware(t *testing.T) {
	raw := smbiosFixture(append([]byte{3, 9, 0, 0, 0, 0, 0, 0, 1}, []byte(azurePublicCloudAssetTag+"\x00\x00")...))
	for _, test := range []struct {
		name    string
		size    uint32
		written uint32
		want    bool
	}{
		{"success", uint32(len(raw)), uint32(len(raw)), true},
		{"size query failed", 0, 0, false},
		{"short size", 7, 0, false},
		{"oversized", maxSMBIOSTableBytes + 1, 0, false},
		{"read failed", uint32(len(raw)), 0, false},
		{"grew between calls", uint32(len(raw)), uint32(len(raw) + 1), false},
		{"truncated read", uint32(len(raw)), uint32(len(raw) - 1), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			got := AzurePublicCloudFromFirmware(func(buffer []byte) uint32 {
				calls++
				if buffer == nil {
					return test.size
				}
				require.EqualValues(t, test.size, len(buffer))
				copy(buffer, raw)
				return test.written
			})
			require.Equal(t, test.want, got)
			require.LessOrEqual(t, calls, 2)
		})
	}
}

func TestAzurePublicCloudAssetTagFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "asset-tag")
	require.False(t, AzurePublicCloudFromAssetTagFile(path))
	for _, test := range []struct {
		value string
		want  bool
	}{
		{azurePublicCloudAssetTag + "\n", true},
		{"", false},
		{"Virtual Machine\n", false},
		{strings.Repeat(" ", 129) + azurePublicCloudAssetTag, false},
	} {
		require.NoError(t, os.WriteFile(path, []byte(test.value), 0600))
		require.Equal(t, test.want, AzurePublicCloudFromAssetTagFile(path))
	}
	require.False(t, AzurePublicCloudFromAssetTagFile(filepath.Dir(path)))
}

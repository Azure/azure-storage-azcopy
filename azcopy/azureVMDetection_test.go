package azcopy

import (
	"encoding/binary"
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

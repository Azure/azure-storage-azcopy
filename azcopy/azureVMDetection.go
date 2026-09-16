package azcopy

import (
	"bytes"
	"encoding/binary"
	"strings"
)

const azurePublicCloudAssetTag = "7783-7084-3265-9085-8269-3286-77"
const maxSMBIOSTableBytes = 1024 * 1024
const maxChassisAssetTagBytes = 128

func isAzurePublicCloudAssetTag(tag string) bool {
	return len(tag) <= maxChassisAssetTagBytes && strings.TrimSpace(tag) == azurePublicCloudAssetTag
}

func azurePublicCloudFromSMBIOS(raw []byte) bool {
	if len(raw) < 8 || len(raw) > maxSMBIOSTableBytes {
		return false
	}
	length := binary.LittleEndian.Uint32(raw[4:8])
	if uint64(length) > uint64(len(raw)-8) {
		return false
	}
	table := raw[8 : 8+int(length)]
	for len(table) >= 4 {
		recordType, recordLength := table[0], int(table[1])
		if recordLength < 4 || recordLength > len(table) || recordType == 127 {
			return false
		}
		terminator := bytes.Index(table[recordLength:], []byte{0, 0})
		if terminator < 0 {
			return false
		}
		if recordType == 3 && recordLength >= 9 {
			assetIndex := int(table[8])
			stringsBlock := table[recordLength : recordLength+terminator]
			for index := 1; assetIndex > 0 && len(stringsBlock) > 0; index++ {
				tag, rest, found := bytes.Cut(stringsBlock, []byte{0})
				if index == assetIndex {
					if isAzurePublicCloudAssetTag(string(tag)) {
						return true
					}
					break
				}
				if !found {
					break
				}
				stringsBlock = rest
			}
		}
		table = table[recordLength+terminator+2:]
	}
	return false
}

// Copyright © 2017 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package common_test

import (
	"testing"

	"github.com/Azure/azure-storage-azcopy/v10/common"
	"github.com/stretchr/testify/assert"
)

func TestEnhanceJobStatusInfo(t *testing.T) {
	a := assert.New(t)
	status := common.EJobStatus

	status = status.EnhanceJobStatusInfo(true, true, true)
	a.Equal(common.EJobStatus.CompletedWithErrorsAndSkipped(), status)

	status = status.EnhanceJobStatusInfo(true, true, false)
	a.Equal(common.EJobStatus.CompletedWithErrorsAndSkipped(), status)

	status = status.EnhanceJobStatusInfo(true, false, true)
	a.Equal(common.EJobStatus.CompletedWithSkipped(), status)

	status = status.EnhanceJobStatusInfo(true, false, false)
	a.Equal(common.EJobStatus.CompletedWithSkipped(), status)

	status = status.EnhanceJobStatusInfo(false, true, true)
	a.Equal(common.EJobStatus.CompletedWithErrors(), status)

	status = status.EnhanceJobStatusInfo(false, true, false)
	a.Equal(common.EJobStatus.Failed(), status)

	status = status.EnhanceJobStatusInfo(false, false, true)
	a.Equal(common.EJobStatus.Completed(), status)

	// No-op if all are false
	status = status.EnhanceJobStatusInfo(false, false, false)
	a.Equal(common.EJobStatus.Completed(), status)
}

func TestIsJobDone(t *testing.T) {
	a := assert.New(t)
	status := common.EJobStatus.InProgress()
	a.False(status.IsJobDone())

	status = status.Paused()
	a.False(status.IsJobDone())

	status = status.Cancelling()
	a.False(status.IsJobDone())

	status = status.Cancelled()
	a.True(status.IsJobDone())

	status = status.Completed()
	a.True(status.IsJobDone())

	status = status.CompletedWithErrors()
	a.True(status.IsJobDone())

	status = status.CompletedWithSkipped()
	a.True(status.IsJobDone())

	status = status.CompletedWithErrors()
	a.True(status.IsJobDone())

	status = status.CompletedWithErrorsAndSkipped()
	a.True(status.IsJobDone())

	status = status.Failed()
	a.True(status.IsJobDone())
}

func getInvalidMetadataSample() common.Metadata {
	temp := make(map[string]string)

	// number could not be first char for azure metadata key.
	temp["1abc"] = "v:1abc"

	// special char
	temp["a!@#"] = "v:a!@#"
	temp["a-metadata-samplE"] = "v:a-metadata-samplE"

	// valid metadata
	temp["abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_"] = "v:abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_"
	temp["Am"] = "v:Am"
	temp["_123"] = "v:_123"

	return toCommonMetadata(temp)
}

func getValidMetadataSample() common.Metadata {
	m := make(map[string]*string)
	v := "value"
	m["Key"] = &v

	return m
}

func toCommonMetadata(temp map[string]string) common.Metadata {
	m := make(map[string]*string)
	for k, v := range temp {
		value := v
		m[k] = &value
	}
	return m
}

func validateMapEqual(a *assert.Assertions, m1 map[string]*string, m2 map[string]string) {
	a.Equal(len(m2), len(m1))

	for k1, v1 := range m1 {
		a.Equal(*v1, m2[k1])
	}
}

func TestMetadataExcludeInvalidKey(t *testing.T) {
	a := assert.New(t)
	mInvalid := getInvalidMetadataSample()
	mValid := getValidMetadataSample()

	retainedMetadata, excludedMetadata, invalidKeyExists := mInvalid.ExcludeInvalidKey()
	a.True(invalidKeyExists)
	validateMapEqual(a, retainedMetadata,
		map[string]string{"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_": "v:abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_",
			"Am": "v:Am", "_123": "v:_123"})
	validateMapEqual(a, excludedMetadata,
		map[string]string{"1abc": "v:1abc", "a!@#": "v:a!@#", "a-metadata-samplE": "v:a-metadata-samplE"})

	retainedMetadata, excludedMetadata, invalidKeyExists = mValid.ExcludeInvalidKey()
	a.False(invalidKeyExists)
	validateMapEqual(a, retainedMetadata, map[string]string{"Key": "value"})
	a.Zero(len(excludedMetadata))
	a.Equal("'Key' ", retainedMetadata.ConcatenatedKeys())
}

func TestMetadataResolveInvalidKey(t *testing.T) {
	a := assert.New(t)
	mInvalid := getInvalidMetadataSample()
	mValid := getValidMetadataSample()

	resolvedMetadata, err := mInvalid.ResolveInvalidKey()
	a.Nil(err)
	validateMapEqual(a, resolvedMetadata,
		map[string]string{"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_": "v:abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_",
			"Am": "v:Am", "_123": "v:_123", "rename_1abc": "v:1abc", "rename_key_1abc": "1abc", "rename_a___": "v:a!@#", "rename_key_a___": "a!@#",
			"rename_a_metadata_samplE": "v:a-metadata-samplE", "rename_key_a_metadata_samplE": "a-metadata-samplE"})

	resolvedMetadata, err = mValid.ResolveInvalidKey()
	a.Nil(err)
	validateMapEqual(a, resolvedMetadata, map[string]string{"Key": "value"})
}

// In this phase we keep the resolve logic easy, and whenever there is key resolving collision found, error reported.
func TestMetadataResolveInvalidKeyNegative(t *testing.T) {
	a := assert.New(t)
	mNegative1 := toCommonMetadata(map[string]string{"!": "!", "*": "*"})
	mNegative2 := toCommonMetadata(map[string]string{"!": "!", "rename__": "rename__"})
	mNegative3 := toCommonMetadata(map[string]string{"!": "!", "rename_key__": "rename_key__"})

	_, err := mNegative1.ResolveInvalidKey()
	a.NotNil(err)

	_, err = mNegative2.ResolveInvalidKey()
	a.NotNil(err)

	_, err = mNegative3.ResolveInvalidKey()
	a.NotNil(err)
}

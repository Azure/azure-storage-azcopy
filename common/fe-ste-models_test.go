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
	"github.com/Azure/azure-storage-azcopy/common"
	chk "gopkg.in/check.v1"
)

type feSteModelsTestSuite struct{}

var _ = chk.Suite(&feSteModelsTestSuite{})

func (s *feSteModelsTestSuite) TestEnhanceJobStatusInfo(c *chk.C) {
	status := common.EJobStatus

	status = status.EnhanceJobStatusInfo(true, true, true)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithErrorsAndSkipped())

	status = status.EnhanceJobStatusInfo(true, true, false)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithErrorsAndSkipped())

	status = status.EnhanceJobStatusInfo(true, false, true)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithSkipped())

	status = status.EnhanceJobStatusInfo(true, false, false)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithSkipped())

	status = status.EnhanceJobStatusInfo(false, true, true)
	c.Assert(status, chk.Equals, common.EJobStatus.CompletedWithErrors())

	status = status.EnhanceJobStatusInfo(false, true, false)
	c.Assert(status, chk.Equals, common.EJobStatus.Failed())

	status = status.EnhanceJobStatusInfo(false, false, true)
	c.Assert(status, chk.Equals, common.EJobStatus.Completed())

	// No-op if all are false
	status = status.EnhanceJobStatusInfo(false, false, false)
	c.Assert(status, chk.Equals, common.EJobStatus.Completed())
}

func (s *feSteModelsTestSuite) TestIsJobDone(c *chk.C) {
	status := common.EJobStatus.InProgress()
	c.Assert(status.IsJobDone(), chk.Equals, false)

	status = status.Paused()
	c.Assert(status.IsJobDone(), chk.Equals, false)

	status = status.Cancelling()
	c.Assert(status.IsJobDone(), chk.Equals, false)

	status = status.Cancelled()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.Completed()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.CompletedWithErrors()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.CompletedWithSkipped()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.CompletedWithErrors()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.CompletedWithErrorsAndSkipped()
	c.Assert(status.IsJobDone(), chk.Equals, true)

	status = status.Failed()
	c.Assert(status.IsJobDone(), chk.Equals, true)
}

func getInvalidMetadataSample() common.Metadata {
	m := make(map[string]string)

	// number could not be first char for azure metadata key.
	m["1abc"] = "v:1abc"

	// special char
	m["a!@#"] = "v:a!@#"
	m["a-metadata-samplE"] = "v:a-metadata-samplE"

	// valid metadata
	m["abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_"] = "v:abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_"
	m["Am"] = "v:Am"
	m["_123"] = "v:_123"

	return m
}

func getValidMetadataSample() common.Metadata {
	m := make(map[string]string)
	m["Key"] = "value"

	return m
}

func validateMapEqual(c *chk.C, m1 map[string]string, m2 map[string]string) {
	c.Assert(len(m1), chk.Equals, len(m2))

	for k1, v1 := range m1 {
		c.Assert(m2[k1], chk.Equals, v1)
	}
}

func (s *feSteModelsTestSuite) TestMetadataExcludeInvalidKey(c *chk.C) {
	mInvalid := getInvalidMetadataSample()
	mValid := getValidMetadataSample()

	reservedMetadata, excludedMetadata, invalidKeyExists := mInvalid.ExcludeInvalidKey()
	c.Assert(invalidKeyExists, chk.Equals, true)
	validateMapEqual(c, reservedMetadata,
		map[string]string{"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_": "v:abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_",
			"Am": "v:Am", "_123": "v:_123"})
	validateMapEqual(c, excludedMetadata,
		map[string]string{"1abc": "v:1abc", "a!@#": "v:a!@#", "a-metadata-samplE": "v:a-metadata-samplE"})

	reservedMetadata, excludedMetadata, invalidKeyExists = mValid.ExcludeInvalidKey()
	c.Assert(invalidKeyExists, chk.Equals, false)
	validateMapEqual(c, reservedMetadata, map[string]string{"Key": "value"})
	c.Assert(len(excludedMetadata), chk.Equals, 0)
	c.Assert(reservedMetadata.ConcatenatedKeys(), chk.Equals, "'Key' ")
}

func (s *feSteModelsTestSuite) TestMetadataResolveInvalidKey(c *chk.C) {
	mInvalid := getInvalidMetadataSample()
	mValid := getValidMetadataSample()

	resolvedMetadata, err := mInvalid.ResolveInvalidKey()
	c.Assert(err, chk.IsNil)
	validateMapEqual(c, resolvedMetadata,
		map[string]string{"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_": "v:abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRUSTUVWXYZ1234567890_",
			"Am": "v:Am", "_123": "v:_123", "rename_1abc": "v:1abc", "rename_key_1abc": "1abc", "rename_a___": "v:a!@#", "rename_key_a___": "a!@#",
			"rename_a_metadata_samplE": "v:a-metadata-samplE", "rename_key_a_metadata_samplE": "a-metadata-samplE"})

	resolvedMetadata, err = mValid.ResolveInvalidKey()
	c.Assert(err, chk.IsNil)
	validateMapEqual(c, resolvedMetadata, map[string]string{"Key": "value"})
}

// In this phase we keep the resolve logic easy, and whenever there is key resolving collision found, error reported.
func (s *feSteModelsTestSuite) TestMetadataResolveInvalidKeyNegative(c *chk.C) {
	mNegative1 := common.Metadata(map[string]string{"!": "!", "*": "*"})
	mNegative2 := common.Metadata(map[string]string{"!": "!", "rename__": "rename__"})
	mNegative3 := common.Metadata(map[string]string{"!": "!", "rename_key__": "rename_key__"})

	_, err := mNegative1.ResolveInvalidKey()
	c.Assert(err, chk.NotNil)

	_, err = mNegative2.ResolveInvalidKey()
	c.Assert(err, chk.NotNil)

	_, err = mNegative3.ResolveInvalidKey()
	c.Assert(err, chk.NotNil)
}

// Copyright © 2026 Microsoft <azcopydev@microsoft.com>
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

package ste

import (
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/file"
	"github.com/stretchr/testify/assert"
)

func TestPrepareSMBPropertiesForFileCreationUsesMinimalLastWriteTime(t *testing.T) {
	sourceLastWriteTime := time.Date(2026, time.October, 5, 12, 0, 0, 0, time.UTC)
	sourceProperties := &file.SMBProperties{
		Attributes:    &file.NTFSFileAttributes{ReadOnly: true},
		LastWriteTime: &sourceLastWriteTime,
	}

	creationProperties := prepareSMBPropertiesForFileCreation(sourceProperties, true)

	assert.Equal(t, time.Unix(0, 0), *creationProperties.LastWriteTime)
	assert.False(t, creationProperties.Attributes.ReadOnly)
	assert.Equal(t, sourceLastWriteTime, *sourceProperties.LastWriteTime)
	assert.True(t, sourceProperties.Attributes.ReadOnly)
}

func TestPrepareSMBPropertiesForFileCreationWithoutInfoPreservation(t *testing.T) {
	sourceLastWriteTime := time.Date(2026, time.October, 5, 12, 0, 0, 0, time.UTC)
	sourceProperties := &file.SMBProperties{
		Attributes:    &file.NTFSFileAttributes{ReadOnly: true},
		LastWriteTime: &sourceLastWriteTime,
	}

	creationProperties := prepareSMBPropertiesForFileCreation(sourceProperties, false)

	assert.Equal(t, sourceLastWriteTime, *creationProperties.LastWriteTime)
	assert.False(t, creationProperties.Attributes.ReadOnly)
	assert.True(t, sourceProperties.Attributes.ReadOnly)
}

func TestPrepareSMBPropertiesForFileCreationHandlesNilProperties(t *testing.T) {
	assert.Nil(t, prepareSMBPropertiesForFileCreation(nil, true))
}

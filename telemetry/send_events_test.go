// Copyright © Microsoft <wastore@microsoft.com>
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

package telemetry

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventToEnvelopes(t *testing.T) {
	envelopes := eventToEnvelopes("ikey-1", sampleFinished())
	require.Len(t, envelopes, 1)
	envelope := envelopes[0]
	assert.Equal(t, "Microsoft.ApplicationInsights.Event", envelope.Name)
	assert.Equal(t, "ikey-1", envelope.IKey)
	assert.Equal(t, "EventData", envelope.Data.BaseType)
	assert.Equal(t, 2, envelope.Data.BaseData.Version)
	assert.Equal(t, "azcopy.job.finished", envelope.Data.BaseData.Name)
	assert.Len(t, envelope.Data.BaseData.Measurements, 49)
	assert.NotContains(t, envelope.Data.BaseData.Measurements, "azcopy.job.finished")
	assert.Equal(t, float64(1024), envelope.Data.BaseData.Measurements["azcopy.bytes_transferred"])
	assert.Equal(t, float64(100), envelope.Data.BaseData.Measurements["azcopy.percent_complete"])
	assert.Equal(t, "copy", envelope.Data.BaseData.Properties["Command"])
	assert.Equal(t, "", envelope.Data.BaseData.Properties["SourceCloudType"])
	assert.Equal(t, "public", envelope.Data.BaseData.Properties["DestCloudType"])
	_, hasSourceEndpointIdentity := envelope.Data.BaseData.Properties["SourceEndpointIdentity"]
	assert.False(t, hasSourceEndpointIdentity)
	_, hasDestEndpointIdentity := envelope.Data.BaseData.Properties["DestEndpointIdentity"]
	assert.False(t, hasDestEndpointIdentity)
	assert.Contains(t, envelope.Data.BaseData.Properties, "SourceStorageAccount")
	assert.Empty(t, envelope.Data.BaseData.Properties["SourceStorageAccount"])
	assert.Equal(t, "account", envelope.Data.BaseData.Properties["DestStorageAccount"])
	_, hasCloudType := envelope.Data.BaseData.Properties["CloudType"]
	assert.False(t, hasCloudType)
}

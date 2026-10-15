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

// MountType is the SourceMountType value: the storage backing a transfer source.
type MountType string

const (
	MountTypeLocalDisk  MountType = "local-disk"
	MountTypeNASSMB     MountType = "nas-smb"
	MountTypeNASNFS     MountType = "nas-nfs"
	MountTypeNASOther   MountType = "nas-other"
	MountTypeVMShare    MountType = "vm-share"
	MountTypeCloudAzure MountType = "cloud-azure"
	MountTypeCloudS3    MountType = "cloud-s3"
	MountTypeCloudGCS   MountType = "cloud-gcs"
)

// ResourceScope is the SourceScope and DestScope value: the level of the selected resource.
type ResourceScope string

const (
	ResourceScopeService        ResourceScope = "service"
	ResourceScopeContainer      ResourceScope = "container"
	ResourceScopeShare          ResourceScope = "share"
	ResourceScopeBucket         ResourceScope = "bucket"
	ResourceScopeObjectOrPrefix ResourceScope = "object-or-prefix"
	ResourceScopeLocalDirectory ResourceScope = "local-directory"
	ResourceScopeLocalObject    ResourceScope = "local-object"
	ResourceScopeStream         ResourceScope = "stream"
	ResourceScopeBenchmark      ResourceScope = "benchmark"
	ResourceScopeNone           ResourceScope = "none"
	ResourceScopeUnknown        ResourceScope = "unknown"
)

// EndpointKind is the SourceEndpointKind and DestEndpointKind value, classified from the hostname.
type EndpointKind string

const (
	EndpointKindPublic          EndpointKind = "public"
	EndpointKindPrivateEndpoint EndpointKind = "private-endpoint"
	EndpointKindUnknown         EndpointKind = "unknown"
)

// CloudType is the SourceCloudType and DestCloudType value, classified from the hostname suffix.
type CloudType string

const (
	CloudTypePublic  CloudType = "public"
	CloudTypeUSGov   CloudType = "usgov"
	CloudTypeChina   CloudType = "china"
	CloudTypeUSSec   CloudType = "ussec"
	CloudTypeUSNat   CloudType = "usnat"
	CloudTypeBleu    CloudType = "bleu"
	CloudTypeDelos   CloudType = "delos"
	CloudTypeGovSG   CloudType = "govsg"
	CloudTypeUnknown CloudType = "unknown"
)

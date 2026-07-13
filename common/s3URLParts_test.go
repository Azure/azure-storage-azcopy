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

package common

import (
	"net/url"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/assert"
)

func TestS3URLParse(t *testing.T) {
	a := assert.New(t)
	u, _ := url.Parse("http://bucket.s3.amazonaws.com")
	p, err := NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("bucket.s3.amazonaws.com", p.Host)
	a.Equal("s3.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3.amazonaws.com", p.String())

	u, _ = url.Parse("http://bucket.s3.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("bucket", p.BucketName)
	a.Equal("s3.amazonaws.com", p.Endpoint)
	a.Equal("", p.ObjectKey)
	a.Equal("", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3.amazonaws.com", p.String())

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keydir/keysubdir/keyname")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3-aws-region.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("keydir/keysubdir/keyname", p.ObjectKey)
	a.Equal("aws-region", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3-aws-region.amazonaws.com/keydir/keysubdir/keyname", p.String())

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keyname")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3-aws-region.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("keyname", p.ObjectKey)
	a.Equal("aws-region", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3-aws-region.amazonaws.com/keyname", p.String())

	u, _ = url.Parse("http://bucket.s3-aws-region.amazonaws.com/keyname/")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3-aws-region.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("keyname/", p.ObjectKey)
	a.Equal("aws-region", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3-aws-region.amazonaws.com/keyname/", p.String())

	// dual stack
	u, _ = url.Parse("http://bucket.s3.dualstack.aws-region.amazonaws.com/keyname/")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.dualstack.aws-region.amazonaws.com", p.Endpoint)
	a.Equal("bucket", p.BucketName)
	a.Equal("keyname/", p.ObjectKey)
	a.Equal("aws-region", p.Region)
	a.Equal("", p.Version)
	a.Equal("http://bucket.s3.dualstack.aws-region.amazonaws.com/keyname/", p.String())

	u, _ = url.Parse("https://s3.amazonaws.com")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.amazonaws.com", p.Endpoint)
	a.Equal("", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3.amazonaws.com", p.String())

	u, _ = url.Parse("https://s3.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.amazonaws.com", p.Endpoint)
	a.Equal("", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3.amazonaws.com", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket01", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket01", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/Test.pdf")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket01", p.BucketName)
	a.Equal("Test.pdf", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/Test.pdf", p.String())

	u, _ = url.Parse("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/space+folder/Test.pdf")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3-ap-southeast-1.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket01", p.BucketName)
	a.Equal("space+folder/Test.pdf", p.ObjectKey)
	a.Equal("ap-southeast-1", p.Region)
	a.Equal("", p.Version)
	a.Equal("https://s3-ap-southeast-1.amazonaws.com/jiac-art-awsbucket01/space+folder/Test.pdf", p.String())

	// Version testing
	u, _ = url.Parse("https://s3.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.ap-northeast-2.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket02-versionenabled", p.BucketName)
	a.Equal("Test.pdf", p.ObjectKey)
	a.Equal("ap-northeast-2", p.Region)
	a.Equal("Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ", p.Version)
	a.Equal("https://s3.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ", p.String())

	// Version and dualstack testing
	u, _ = url.Parse("https://s3.dualstack.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.dualstack.ap-northeast-2.amazonaws.com", p.Endpoint)
	a.Equal("jiac-art-awsbucket02-versionenabled", p.BucketName)
	a.Equal("Test.pdf", p.ObjectKey)
	a.Equal("ap-northeast-2", p.Region)
	a.Equal("Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ", p.Version)
	a.Equal("https://s3.dualstack.ap-northeast-2.amazonaws.com/jiac-art-awsbucket02-versionenabled/Test.pdf?versionId=Cy0pgpqHDTR7RlMEwU_BxDVER2QN5lJJ", p.String())

}

func TestS3URLParseNegative(t *testing.T) {
	a := assert.New(t)
	u, _ := url.Parse("http://bucket.amazonawstypo.com")
	_, err := NewS3URLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidS3URLErrorMessage))

	u, _ = url.Parse("http://bucket.s3.amazonawstypo.com")
	_, err = NewS3URLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidS3URLErrorMessage))

	u, _ = url.Parse("http://s3-test.blob.core.windows.net")
	_, err = NewS3URLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidS3URLErrorMessage))
}

func TestOCIURLParse(t *testing.T) {
	a := assert.New(t)
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "mytenant.compat.objectstorage.us-sanjose-1.oraclecloud.com")

	// Test OCI bucket URL
	u, _ := url.Parse("https://mytenant.compat.objectstorage.us-sanjose-1.oraclecloud.com/mybucket/")
	p, err := NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("mytenant.compat.objectstorage.us-sanjose-1.oraclecloud.com", p.Host)
	a.Equal("mytenant.compat.objectstorage.us-sanjose-1.oraclecloud.com", p.Endpoint)
	a.Equal("us-sanjose-1", p.Region)
	a.Equal("mybucket", p.BucketName)
	a.Equal("", p.ObjectKey)
	a.True(p.IsOracleCloudStorage())

	// Test OCI object URL
	u, _ = url.Parse("https://mytenant.compat.objectstorage.us-sanjose-1.oraclecloud.com/mybucket/file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("mybucket", p.BucketName)
	a.Equal("file.txt", p.ObjectKey)
	a.Equal("us-sanjose-1", p.Region)

	// Test OCI with nested object path
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "mynamespace.compat.objectstorage.us-phoenix-1.oraclecloud.com")
	u, _ = url.Parse("https://mynamespace.compat.objectstorage.us-phoenix-1.oraclecloud.com/mybucket/folder/subfolder/file.pdf")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("mybucket", p.BucketName)
	a.Equal("folder/subfolder/file.pdf", p.ObjectKey)
	a.Equal("us-phoenix-1", p.Region)
	a.True(p.IsOracleCloudStorage())

	// Test OCI with different region
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "myns.compat.objectstorage.eu-zurich-1.oraclecloud.com")
	u, _ = url.Parse("https://myns.compat.objectstorage.eu-zurich-1.oraclecloud.com/testbucket/")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("eu-zurich-1", p.Region)
	a.Equal("testbucket", p.BucketName)

	// Test OCI bucket only without trailing slash
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "namespace123.compat.objectstorage.ca-montreal-1.oraclecloud.com")
	u, _ = url.Parse("https://namespace123.compat.objectstorage.ca-montreal-1.oraclecloud.com/bucket456")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("bucket456", p.BucketName)
	a.Equal("", p.ObjectKey)

	// Test round-trip URL reconstruction
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "mytenant.compat.objectstorage.us-sanjose-1.oraclecloud.com")
	u, _ = url.Parse("https://mytenant.compat.objectstorage.us-sanjose-1.oraclecloud.com/mybucket/folder/file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("https://mytenant.compat.objectstorage.us-sanjose-1.oraclecloud.com/mybucket/folder/file.txt", p.String())

	// Validate namespace must exist in host for OCI compat URL
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "compat.objectstorage.us-sanjose-1.oraclecloud.com")
	u, _ = url.Parse("https://compat.objectstorage.us-sanjose-1.oraclecloud.com/mybucket/file.txt")
	_, err = NewS3URLParts(*u)
	a.NotNil(err)
	a.True(strings.Contains(err.Error(), invalidS3URLErrorMessage))

	// OCI public path-style customer endpoint
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "mytenant.compat.objectstorage.us-ashburn-1.oci.customer-oci.com")
	u, _ = url.Parse("https://mytenant.compat.objectstorage.us-ashburn-1.oci.customer-oci.com/mybucket/myobject.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("mytenant.compat.objectstorage.us-ashburn-1.oci.customer-oci.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("myobject.txt", p.ObjectKey)
	a.Equal("us-ashburn-1", p.Region)
	a.True(p.IsOracleCloudStorage())

	// OCI public virtual-hosted endpoint
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "vhcompat.objectstorage.us-ashburn-1.oci.customer-oci.com")
	u, _ = url.Parse("https://mybucket.vhcompat.objectstorage.us-ashburn-1.oci.customer-oci.com/folder/file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("mybucket.vhcompat.objectstorage.us-ashburn-1.oci.customer-oci.com", p.Host)
	a.Equal("vhcompat.objectstorage.us-ashburn-1.oci.customer-oci.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("folder/file.txt", p.ObjectKey)
	a.Equal("us-ashburn-1", p.Region)
	a.True(p.IsOracleCloudStorage())

	// OCI private networking endpoint
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "myprefix-mytenant.private.compat.objectstorage.us-phoenix-1.oci.customer-oci.com")
	u, _ = url.Parse("https://myprefix-mytenant.private.compat.objectstorage.us-phoenix-1.oci.customer-oci.com/mybucket/private/file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("myprefix-mytenant.private.compat.objectstorage.us-phoenix-1.oci.customer-oci.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("private/file.txt", p.ObjectKey)
	a.Equal("us-phoenix-1", p.Region)
	a.True(p.IsOracleCloudStorage())
}

func TestGCSURLParse(t *testing.T) {
	a := assert.New(t)

	// Global path-style: endpoint comes from path-style host
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "storage.googleapis.com")
	u, _ := url.Parse("https://storage.googleapis.com/mybucket/seed_2/fileooo1.txt")
	p, err := NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("storage.googleapis.com", p.Host)
	a.Equal("storage.googleapis.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("seed_2/fileooo1.txt", p.ObjectKey)
	a.Equal("", p.Region)
	a.True(p.IsGoogleCloudStorage())

	// Regional path-style REP endpoint
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "storage.us-west2.rep.googleapis.com")
	u, _ = url.Parse("https://storage.us-west2.rep.googleapis.com/mybucket/file001.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("storage.us-west2.rep.googleapis.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("file001.txt", p.ObjectKey)
	a.Equal("us-west2", p.Region)
	a.True(p.IsGoogleCloudStorage())

	// PSC path-style endpoint
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "storage-psc123.p.googleapis.com")
	u, _ = url.Parse("https://storage-psc123.p.googleapis.com/mybucket/folder/file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("storage-psc123.p.googleapis.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("folder/file.txt", p.ObjectKey)
	a.Equal("", p.Region)
	a.True(p.IsGoogleCloudStorage())

	// Virtual-hosted style: endpoint extracted by worker would be storage.googleapis.com
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "storage.googleapis.com")
	u, _ = url.Parse("https://mybucket.storage.googleapis.com/path/to/file.bin")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("mybucket.storage.googleapis.com", p.Host)
	a.Equal("storage.googleapis.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("path/to/file.bin", p.ObjectKey)
	a.Equal("", p.Region)
	a.True(p.IsGoogleCloudStorage())
	a.Equal("https://mybucket.storage.googleapis.com/path/to/file.bin", p.String())

	// Virtual-hosted style with dotted bucket name
	u, _ = url.Parse("https://my.bucket.storage.googleapis.com/object.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("my.bucket.storage.googleapis.com", p.Host)
	a.Equal("storage.googleapis.com", p.Endpoint)
	a.Equal("my.bucket", p.BucketName)
	a.Equal("object.txt", p.ObjectKey)
	a.Equal("", p.Region)
	a.True(p.IsGoogleCloudStorage())
	a.Equal("https://my.bucket.storage.googleapis.com/object.txt", p.String())
}

func TestIBMURLParse(t *testing.T) {
	a := assert.New(t)

	// IBM regional endpoint (path-style)
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "s3.eu-de.cloud-object-storage.appdomain.cloud")
	u, _ := url.Parse("https://s3.eu-de.cloud-object-storage.appdomain.cloud/mybucket/folder/file.txt")
	p, err := NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.eu-de.cloud-object-storage.appdomain.cloud", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("folder/file.txt", p.ObjectKey)
	a.Equal("eu-de", p.Region)
	a.True(p.IsIBMCloudObjectStorage())
	a.Equal(S3ProviderIBM, p.ProviderKind())

	// IBM geo endpoint (path-style)
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "s3.us.cloud-object-storage.appdomain.cloud")
	u, _ = url.Parse("https://s3.us.cloud-object-storage.appdomain.cloud/mybucket/file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.us.cloud-object-storage.appdomain.cloud", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("file.txt", p.ObjectKey)
	a.Equal("us", p.Region)
	a.True(p.IsIBMCloudObjectStorage())
	a.Equal(S3ProviderIBM, p.ProviderKind())

	// IBM virtual-hosted style
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "s3.us-south.cloud-object-storage.appdomain.cloud")
	u, _ = url.Parse("https://mybucket.s3.us-south.cloud-object-storage.appdomain.cloud/folder/file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.us-south.cloud-object-storage.appdomain.cloud", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("folder/file.txt", p.ObjectKey)
	a.Equal("us-south", p.Region)
	a.True(p.IsIBMCloudObjectStorage())
	a.Equal(S3ProviderIBM, p.ProviderKind())

	// IBM private endpoint (path-style)
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "s3.private.us-south.cloud-object-storage.appdomain.cloud")
	u, _ = url.Parse("https://s3.private.us-south.cloud-object-storage.appdomain.cloud/mybucket/private-file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.private.us-south.cloud-object-storage.appdomain.cloud", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("private-file.txt", p.ObjectKey)
	a.Equal("us-south", p.Region)
	a.True(p.IsIBMCloudObjectStorage())
	a.Equal(S3ProviderIBM, p.ProviderKind())

	// IBM private endpoint (virtual-hosted)
	u, _ = url.Parse("https://mybucket.s3.private.us-south.cloud-object-storage.appdomain.cloud/folder/file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.private.us-south.cloud-object-storage.appdomain.cloud", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("folder/file.txt", p.ObjectKey)
	a.Equal("us-south", p.Region)
	a.True(p.IsIBMCloudObjectStorage())
	a.Equal(S3ProviderIBM, p.ProviderKind())

	// Generic IBM suffix should also recognize private endpoints.
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "cloud-object-storage.appdomain.cloud")
	u, _ = url.Parse("https://mybucket.s3.private.us-south.cloud-object-storage.appdomain.cloud/folder/file2.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("s3.private.us-south.cloud-object-storage.appdomain.cloud", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("folder/file2.txt", p.ObjectKey)
	a.Equal("us-south", p.Region)
	a.True(p.IsIBMCloudObjectStorage())
	a.Equal(S3ProviderIBM, p.ProviderKind())
}

func TestAlibabaURLParseAndBucketLookup(t *testing.T) {
	a := assert.New(t)

	t.Setenv("S3_COMPATIBLE_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com")
	u, _ := url.Parse("https://mybucket.oss-cn-hangzhou.aliyuncs.com/path/to/file.txt")
	p, err := NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("oss-cn-hangzhou.aliyuncs.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("path/to/file.txt", p.ObjectKey)
	a.Equal("cn-hangzhou", p.Region)
	a.True(p.IsAlibabaObjectStorage())
	a.Equal(S3ProviderAlibaba, p.ProviderKind())

	// Alibaba OSS only supports virtual-hosted style, so lookup must be DNS.
	a.Equal(minio.BucketLookupDNS, getS3BucketLookup("oss-cn-hangzhou.aliyuncs.com"))

	// IBM should keep existing S3-compatible behavior (path-style via env-based compatible endpoint).
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "s3.us.cloud-object-storage.appdomain.cloud")
	a.Equal(minio.BucketLookupPath, getS3BucketLookup("s3.us.cloud-object-storage.appdomain.cloud"))

	// Alibaba internal endpoint should also parse as Alibaba OSS.
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "oss-cn-hangzhou-internal.aliyuncs.com")
	u, _ = url.Parse("https://mybucket.oss-cn-hangzhou-internal.aliyuncs.com/path/to/internal-file.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("oss-cn-hangzhou-internal.aliyuncs.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("path/to/internal-file.txt", p.ObjectKey)
	a.Equal("cn-hangzhou", p.Region)
	a.True(p.IsAlibabaObjectStorage())
	a.Equal(S3ProviderAlibaba, p.ProviderKind())
	a.Equal(minio.BucketLookupDNS, getS3BucketLookup("oss-cn-hangzhou-internal.aliyuncs.com"))

	// Generic Alibaba suffix should also recognize internal virtual-hosted endpoints.
	t.Setenv("S3_COMPATIBLE_ENDPOINT", "aliyuncs.com")
	u, _ = url.Parse("https://mybucket.oss-cn-hangzhou-internal.aliyuncs.com/path/to/internal-file2.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("oss-cn-hangzhou-internal.aliyuncs.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("path/to/internal-file2.txt", p.ObjectKey)
	a.Equal("cn-hangzhou", p.Region)
	a.True(p.IsAlibabaObjectStorage())
	a.Equal(S3ProviderAlibaba, p.ProviderKind())

	// Generic suffix with service endpoint path-style should also parse region correctly.
	u, _ = url.Parse("https://oss-cn-hangzhou-internal.aliyuncs.com/mybucket/path/to/internal-file3.txt")
	p, err = NewS3URLParts(*u)
	a.Nil(err)
	a.Equal("oss-cn-hangzhou-internal.aliyuncs.com", p.Endpoint)
	a.Equal("mybucket", p.BucketName)
	a.Equal("path/to/internal-file3.txt", p.ObjectKey)
	a.Equal("cn-hangzhou", p.Region)
}

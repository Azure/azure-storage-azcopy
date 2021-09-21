package ste

import (
	gcpUtils "cloud.google.com/go/storage"
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/nitin-deamon/azure-storage-azcopy/v10/common"
	"golang.org/x/oauth2/google"
	"io/ioutil"
	"net/url"
	"time"
)

type gcpSourceInfoProvider struct {
	jptm         IJobPartTransferMgr
	transferInfo TransferInfo

	rawSourceURL *url.URL

	gcpClient   *gcpUtils.Client
	gcpURLParts common.GCPURLParts
}

var gcpClientFactory = common.NewGCPClientFactory()
var jsonKey []byte

func newGCPSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	var err error
	p := gcpSourceInfoProvider{jptm: jptm, transferInfo: jptm.Info()}

	p.rawSourceURL, err = url.Parse(p.transferInfo.Source)
	if err != nil {
		return nil, err
	}
	p.gcpURLParts, err = common.NewGCPURLParts(*p.rawSourceURL)
	if err != nil {
		return nil, err
	}

	p.gcpClient, err = gcpClientFactory.GetGCPClient(
		p.jptm.Context(),
		common.CredentialInfo{
			CredentialType:    common.ECredentialType.GoogleAppCredentials(),
			GCPCredentialInfo: common.GCPCredentialInfo{},
		},
		common.CredentialOpOptions{
			LogInfo:  func(str string) { p.jptm.Log(pipeline.LogInfo, str) },
			LogError: func(str string) { p.jptm.Log(pipeline.LogError, str) },
			Panic:    func(err error) { panic(err) },
		})
	if err != nil {
		return nil, err
	}
	glcm := common.GetLifecycleMgr()
	jsonKey, err = ioutil.ReadFile(glcm.GetEnvironmentVariable(common.EEnvironmentVariable.GoogleAppCredentials()))
	if err != nil {
		return nil, fmt.Errorf("Cannot read JSON key file. Please verify you have correctly set GOOGLE_APPLICATION_CREDENTIALS environment variable")
	}
	return &p, nil
}

func (p *gcpSourceInfoProvider) PreSignedSourceURL() (*url.URL, error) {

	conf, err := google.JWTConfigFromJSON(jsonKey)
	if err != nil {
		return nil, fmt.Errorf("Could not get config from json key. Error: %v", err)
	}
	opts := &gcpUtils.SignedURLOptions{
		Scheme:         gcpUtils.SigningSchemeV4,
		Method:         "GET",
		GoogleAccessID: conf.Email,
		PrivateKey:     conf.PrivateKey,
		Expires:        time.Now().Add(defaultPresignExpires),
	}
	u, err := gcpUtils.SignedURL(p.gcpURLParts.BucketName, p.gcpURLParts.ObjectKey, opts)

	if err != nil {
		return nil, fmt.Errorf("Unable to Generate Signed URL for given GCP Object: %v", err)
	}

	parsedURL, err := url.Parse(u)
	if err != nil {
		return nil, fmt.Errorf("Unable to parse signed URL: %v", err)
	}

	return parsedURL, nil
}

func (p *gcpSourceInfoProvider) Properties() (*SrcProperties, error) {
	srcProperties := SrcProperties{
		SrcHTTPHeaders: p.transferInfo.SrcHTTPHeaders,
		SrcMetadata:    p.transferInfo.SrcMetadata,
	}
	if p.transferInfo.S2SGetPropertiesInBackend {
		objectInfo, err := p.gcpClient.Bucket(p.gcpURLParts.BucketName).Object(p.gcpURLParts.ObjectKey).Attrs(p.jptm.Context())
		if err != nil {
			return nil, err
		}
		oie := common.GCPObjectInfoExtension{ObjectInfo: *objectInfo}
		srcProperties = SrcProperties{
			SrcHTTPHeaders: common.ResourceHTTPHeaders{
				ContentType:        objectInfo.ContentType,
				ContentEncoding:    oie.ContentEncoding(),
				ContentDisposition: oie.ContentDisposition(),
				ContentLanguage:    oie.ContentLanguage(),
				CacheControl:       oie.CacheControl(),
				ContentMD5:         oie.ContentMD5(),
			},
			SrcMetadata: oie.NewCommonMetadata(),
		}
	}
	resolvedMetadata, err := p.handleInvalidMetadataKeys(srcProperties.SrcMetadata)

	if err != nil {
		return nil, err
	}
	srcProperties.SrcMetadata = resolvedMetadata
	return &srcProperties, nil
}

func (p *gcpSourceInfoProvider) handleInvalidMetadataKeys(m common.Metadata) (common.Metadata, error) {
	if m == nil {
		return m, nil
	}
	switch p.transferInfo.S2SInvalidMetadataHandleOption {
	case common.EInvalidMetadataHandleOption.ExcludeIfInvalid():
		retainedMetadata, excludedMetadata, invalidKeyExists := m.ExcludeInvalidKey()
		if invalidKeyExists && p.jptm.ShouldLog(pipeline.LogWarning) {
			p.jptm.Log(pipeline.LogWarning,
				fmt.Sprintf("METADATAWARNING: For source %q, invalid metadata with keys %s are excluded", p.transferInfo.Source, excludedMetadata.ConcatenatedKeys()))
		}
		return retainedMetadata, nil
	case common.EInvalidMetadataHandleOption.FailIfInvalid():
		_, invalidMetadata, invalidKeyExists := m.ExcludeInvalidKey()
		if invalidKeyExists {
			return nil, fmt.Errorf("metadata with keys %s in source is invalid, and application parameters specify that error should be reported when invalid keys are found", invalidMetadata.ConcatenatedKeys())
		}
		return m, nil
	case common.EInvalidMetadataHandleOption.RenameIfInvalid():
		return m.ResolveInvalidKey()
	}
	return m, nil
}

func (p *gcpSourceInfoProvider) SourceSize() int64 {
	return p.transferInfo.SourceSize
}

func (p *gcpSourceInfoProvider) RawSource() string {
	return p.transferInfo.Source
}

func (p *gcpSourceInfoProvider) IsLocal() bool {
	return false
}

func (p *gcpSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	objectInfo, err := p.gcpClient.Bucket(p.gcpURLParts.BucketName).Object(p.gcpURLParts.ObjectKey).Attrs(p.jptm.Context())
	if err != nil {
		return time.Time{}, err
	}
	return objectInfo.Updated, nil
}

func (p *gcpSourceInfoProvider) EntityType() common.EntityType {
	return common.EEntityType.File() // All folders are virtual in GCP and only files exist.
}

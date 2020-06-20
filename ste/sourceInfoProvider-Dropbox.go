package ste

import (
	"fmt"
	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-azcopy/common"
	"github.com/dropbox/dropbox-sdk-go-unofficial/dropbox/files"
	"net/url"
	"time"
)

type dropboxSourceInfoProvider struct {
	jptm         IJobPartTransferMgr
	transferInfo TransferInfo

	rawSourceURL *url.URL

	client          files.Client
	dropboxURLParts common.DropboxURLParts
}

const defaultDropboxPresignExpires = time.Hour * 4

func newDropboxSourceInfoProvider(jptm IJobPartTransferMgr) (ISourceInfoProvider, error) {
	var err error
	p := dropboxSourceInfoProvider{jptm: jptm, transferInfo: jptm.Info()}

	p.rawSourceURL, err = url.Parse(p.transferInfo.Source)
	if err != nil {
		return nil, err
	}

	p.dropboxURLParts, err = common.NewDropboxURLParts(*p.rawSourceURL)
	if err != nil {
		return nil, err
	}

	p.client, err = common.CreateDropboxClient()

	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (p *dropboxSourceInfoProvider) PreSignedSourceURL() (*url.URL, error) {
	res, err := p.client.GetTemporaryLink(files.NewGetTemporaryLinkArg("/" + p.dropboxURLParts.ObjectKey))
	if err != nil {
		return nil, err
	}
	presignURL, err := url.Parse(res.Link)
	if err != nil {
		return nil, err
	}
	return presignURL, nil
}

func (p *dropboxSourceInfoProvider) Properties() (*SrcProperties, error) {
	srcProperties := SrcProperties{
		SrcHTTPHeaders: p.transferInfo.SrcHTTPHeaders,
		SrcMetadata:    p.transferInfo.SrcMetadata,
	}

	if p.transferInfo.S2SGetPropertiesInBackend {
		metadata, err := p.client.GetMetadata(files.NewGetMetadataArg("/" + p.dropboxURLParts.ObjectKey))
		if err != nil {
			return nil, err
		}
		fileMetadata, ok := metadata.(*files.FileMetadata)
		if !ok {
			return nil, fmt.Errorf("could not get FileMetadata for given file %s", p.transferInfo.Source)
		}
		oie := common.DropboxObjectInfoExtension{Metadata: *fileMetadata}
		srcProperties = SrcProperties{
			SrcHTTPHeaders: common.ResourceHTTPHeaders{
				ContentType:        oie.ContentLanguage(),
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

func (p *dropboxSourceInfoProvider) handleInvalidMetadataKeys(m common.Metadata) (common.Metadata, error) {
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

func (p *dropboxSourceInfoProvider) SourceSize() int64 {
	return p.transferInfo.SourceSize
}

func (p *dropboxSourceInfoProvider) RawSource() string {
	return p.transferInfo.Source
}

func (p *dropboxSourceInfoProvider) IsLocal() bool {
	return false
}

func (p *dropboxSourceInfoProvider) GetFreshFileLastModifiedTime() (time.Time, error) {
	metadata, err := p.client.GetMetadata(files.NewGetMetadataArg("/" + p.dropboxURLParts.ObjectKey))
	if err != nil {
		return time.Time{}, err
	}
	fileMetadata, ok := metadata.(*files.FileMetadata)
	if !ok {
		return time.Time{}, fmt.Errorf("could not retrieve LMT for source %s", p.dropboxURLParts.ObjectKey)
	}
	oie := common.DropboxObjectInfoExtension{Metadata: *fileMetadata}
	return oie.LMT(), nil
}

func (p *dropboxSourceInfoProvider) EntityType() common.EntityType {
	return common.EEntityType.File()
}

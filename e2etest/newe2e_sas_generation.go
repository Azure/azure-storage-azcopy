package e2etest

import (
	"strings"
	"time"

	blobsas "github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
	datalakesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/sas"
	filesas "github.com/Azure/azure-sdk-for-go/sdk/storage/azfile/sas"
	"github.com/Azure/azure-storage-azcopy/v10/common"
)

// BlobSignatureValues makes both account/service signature values generic for output from GenericSignatureValues
type BlobSignatureValues interface {
	SignWithSharedKey(credential *blobsas.SharedKeyCredential) (blobsas.QueryParameters, error)
}

// FileSignatureValues makes both account/service signature values generic for output from GenericSignatureValues
type FileSignatureValues interface {
	SignWithSharedKey(credential *filesas.SharedKeyCredential) (filesas.QueryParameters, error)
}

// DatalakeSignatureValues makes both account/service signature values generic for output from GenericSignatureValues
type DatalakeSignatureValues interface {
	SignWithSharedKey(credential *datalakesas.SharedKeyCredential) (datalakesas.QueryParameters, error)
}

// GenericSignatureValues are a set of signature values that are portable to all 3 Azure storage services and both service and account level SAS.
// Distinct limitations of these generic values are documented in the struct.
type GenericSignatureValues interface {
	AsBlob() BlobSignatureValues
	AsFile() FileSignatureValues
	AsDatalake() DatalakeSignatureValues
}

// GenericServiceSignatureValues is a generic struct encompassing the possible values for Blob, Files, and Datalake service SAS tokens.
// Check the comments within the struct for info about defaults or valid values for each service.
type GenericServiceSignatureValues struct {
	// Gets defaulted by the relevant SDK to the SDK version
	Version string
	// Protocol defaults to HTTPS.
	Protocol blobsas.Protocol
	// StartTime, if unspecified, is this moment.
	StartTime time.Time
	// ExpiryTime, if unspecified, is 24 hours from StartTime.
	ExpiryTime time.Time
	// SnapshotTime is unused on datalake, but refers to the blob snapshot time or the file share snapshot time.
	SnapshotTime time.Time
	// BlobVersion is unique to Blob.
	BlobVersion string
	// Permissions should be supplied by calling .String() on any of the below structs.
	// Blob and Datalake overlap nicely
	// blobsas.ContainerPermissions blobsas.BlobPermissions
	// filesas.SharePermissions filesas.FilePermissions
	// datalakesas.FileSystemPermissions datalakesas.FilePermissions datalakesas.DirectoryPermissions
	// If zero, defaults to racwdl (read, add, create, write, delete, list
	Permissions   string
	IPRange       blobsas.IPRange
	Identifier    string
	ContainerName string
	ObjectName    string
	// DirectoryPath is used on Blob & Datalake, and is intended to limit to a particular subdirectory.
	// On Files, replaces ObjectName if present.
	DirectoryPath        string
	CacheControl         string
	ContentDisposition   string
	ContentEncoding      string
	ContentLanguage      string
	ContentType          string
	AuthorizedObjectID   string
	UnauthorizedObjectID string
	// CorrelationID is used on Blob & Datalake
	CorrelationID string
}

// withDefaults will never have to be called by
func (vals GenericServiceSignatureValues) withDefaults() GenericServiceSignatureValues {
	out := vals

	SetIfZero(&out.Protocol, blobsas.ProtocolHTTPS)
	//time.Now().Add()
	SetIfZero(&out.StartTime, time.Now().UTC().Add(-time.Minute*30))
	SetIfZero(&out.ExpiryTime, time.Now().UTC().Add(time.Minute*30))
	SetIfZero(&out.Permissions, (&blobsas.ContainerPermissions{
		Read: true, Add: true, Create: true, Write: true, Delete: true, List: true,
	}).String())

	return out
}

func (vals GenericServiceSignatureValues) AsBlob() BlobSignatureValues {
	s := vals.withDefaults()

	return &blobsas.BlobSignatureValues{
		Version:              s.Version,
		Protocol:             s.Protocol,
		StartTime:            s.StartTime,
		ExpiryTime:           s.ExpiryTime,
		SnapshotTime:         s.SnapshotTime,
		Permissions:          s.Permissions,
		IPRange:              s.IPRange,
		Identifier:           s.Identifier,
		ContainerName:        s.ContainerName,
		BlobName:             s.ObjectName,
		Directory:            s.DirectoryPath,
		CacheControl:         s.CacheControl,
		ContentDisposition:   s.ContentDisposition,
		ContentEncoding:      s.ContentEncoding,
		ContentLanguage:      s.ContentLanguage,
		ContentType:          s.ContentType,
		BlobVersion:          s.BlobVersion,
		AuthorizedObjectID:   s.AuthorizedObjectID,
		UnauthorizedObjectID: s.UnauthorizedObjectID,
		CorrelationID:        s.CorrelationID,
	}
}

func (vals GenericServiceSignatureValues) AsFile() FileSignatureValues {
	s := vals.withDefaults()

	s.Permissions = strings.ReplaceAll(s.Permissions, "a", "")

	return &filesas.SignatureValues{
		Version:            s.Version,
		Protocol:           filesas.Protocol(s.Protocol),
		StartTime:          s.StartTime,
		ExpiryTime:         s.ExpiryTime,
		SnapshotTime:       s.SnapshotTime,
		Permissions:        s.Permissions,
		IPRange:            filesas.IPRange(s.IPRange),
		Identifier:         s.Identifier,
		ShareName:          s.ContainerName,
		FilePath:           common.Iff(s.DirectoryPath != "", s.DirectoryPath, s.ObjectName),
		CacheControl:       s.CacheControl,
		ContentDisposition: s.ContentDisposition,
		ContentEncoding:    s.ContentEncoding,
		ContentLanguage:    s.ContentLanguage,
		ContentType:        s.ContentType,
	}
}

func (vals GenericServiceSignatureValues) AsDatalake() DatalakeSignatureValues {
	s := vals.withDefaults()

	return &datalakesas.DatalakeSignatureValues{
		Version:              s.Version,
		Protocol:             datalakesas.Protocol(s.Protocol),
		StartTime:            s.StartTime,
		ExpiryTime:           s.ExpiryTime,
		Permissions:          s.Permissions,
		IPRange:              datalakesas.IPRange(s.IPRange),
		Identifier:           s.Identifier,
		FileSystemName:       s.ContainerName,
		FilePath:             s.ObjectName,
		DirectoryPath:        s.DirectoryPath,
		CacheControl:         s.CacheControl,
		ContentDisposition:   s.ContentDisposition,
		ContentEncoding:      s.ContentEncoding,
		ContentLanguage:      s.ContentLanguage,
		ContentType:          s.ContentType,
		AuthorizedObjectID:   s.AuthorizedObjectID,
		UnauthorizedObjectID: s.UnauthorizedObjectID,
		CorrelationID:        s.CorrelationID,
	}
}

type GenericAccountSignatureValues struct {
	Version string
	// Defaults to HTTPS
	Protocol blobsas.Protocol
	// Defaults to now
	StartTime time.Time
	// Defaults to 24hr past StartTime
	ExpiryTime time.Time
	// Defaults to racwdl, uses blobsas.AccountPermissions, filesas.AccountPermissions, or datalakesas.AccountPermissions
	Permissions string
	IPRange     blobsas.IPRange
	// defaults to sco, uses blobsas.AccountResourceTypes, filesas.AccountResourceTypes, or datalakesas.AccountResourceTypes
	ResourceTypes string
}

func (vals GenericAccountSignatureValues) withDefaults() GenericAccountSignatureValues {
	out := vals

	SetIfZero(&out.Protocol, blobsas.ProtocolHTTPS)
	SetIfZero(&out.StartTime, time.Now().UTC().Add(-time.Minute*30))
	SetIfZero(&out.ExpiryTime, time.Now().UTC().Add(time.Minute*30))
	SetIfZero(&out.Permissions, (&blobsas.AccountPermissions{
		Read: true, Add: true, Create: true, Write: true, Delete: true, List: true,
	}).String())
	SetIfZero(&out.ResourceTypes, (&blobsas.AccountResourceTypes{Service: true, Container: true, Object: true}).String())

	return out
}

func (vals GenericAccountSignatureValues) AsBlob() BlobSignatureValues {
	s := vals.withDefaults()

	return blobsas.AccountSignatureValues{
		Version:       s.Version,
		Protocol:      s.Protocol,
		StartTime:     s.StartTime,
		ExpiryTime:    s.ExpiryTime,
		Permissions:   s.Permissions,
		IPRange:       s.IPRange,
		ResourceTypes: s.ResourceTypes,
	}
}

func (vals GenericAccountSignatureValues) AsFile() FileSignatureValues {
	s := vals.withDefaults()

	s.Permissions = strings.ReplaceAll(s.Permissions, "a", "") // remove 'a', because it's invalid and causes panics.

	return filesas.AccountSignatureValues{
		Version:       s.Version,
		Protocol:      filesas.Protocol(s.Protocol),
		StartTime:     s.StartTime,
		ExpiryTime:    s.ExpiryTime,
		Permissions:   s.Permissions,
		IPRange:       filesas.IPRange(s.IPRange),
		ResourceTypes: s.ResourceTypes,
	}
}

func (vals GenericAccountSignatureValues) AsDatalake() DatalakeSignatureValues {
	s := vals.withDefaults()

	return datalakesas.AccountSignatureValues{
		Version:       s.Version,
		Protocol:      datalakesas.Protocol(s.Protocol),
		StartTime:     s.StartTime,
		ExpiryTime:    s.ExpiryTime,
		Permissions:   s.Permissions,
		IPRange:       datalakesas.IPRange(s.IPRange),
		ResourceTypes: s.ResourceTypes,
	}
}

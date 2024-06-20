package cmd

import (
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"time"
)

// ListBlobsFlatSegmentResponse - An enumeration of blobs
type ListBlobsFlatSegmentResponse struct {
	// REQUIRED
	ContainerName *string `xml:"ContainerName,attr"`

	// REQUIRED
	Segment *BlobFlatListSegment `xml:"Blobs"`

	// REQUIRED
	ServiceEndpoint *string `xml:"ServiceEndpoint,attr"`
	Marker          *string `xml:"Marker"`
	MaxResults      *int32  `xml:"MaxResults"`
	NextMarker      *string `xml:"NextMarker"`
	Prefix          *string `xml:"Prefix"`
}

type BlobFlatListSegment struct {
	// REQUIRED
	BlobItems []*BlobItem `xml:"Blob"`
}

// BlobItem - An Azure Storage blob
type BlobItem struct {
	// REQUIRED
	Deleted *bool `xml:"Deleted"`

	// REQUIRED
	Name *string `xml:"Name"`

	// REQUIRED; Properties of a blob
	Properties *BlobProperties `xml:"Properties"`

	// REQUIRED
	Snapshot *string `xml:"Snapshot"`

	// Blob tags
	BlobTags         *BlobTags `xml:"Tags"`
	HasVersionsOnly  *bool     `xml:"HasVersionsOnly"`
	IsCurrentVersion *bool     `xml:"IsCurrentVersion"`

	// Dictionary of
	Metadata map[string]*string `xml:"Metadata"`

	// Dictionary of
	OrMetadata map[string]*string `xml:"OrMetadata"`
	VersionID  *string            `xml:"VersionId"`
}

// BlobProperties - Properties of a blob
type BlobProperties struct {
	// REQUIRED
	ETag *azcore.ETag `xml:"Etag"`

	// REQUIRED
	LastModified         *time.Time     `xml:"Last-Modified"`
	AccessTier           *AccessTier    `xml:"AccessTier"`
	AccessTierChangeTime *time.Time     `xml:"AccessTierChangeTime"`
	AccessTierInferred   *bool          `xml:"AccessTierInferred"`
	ArchiveStatus        *ArchiveStatus `xml:"ArchiveStatus"`
	BlobSequenceNumber   *int64         `xml:"x-ms-blob-sequence-number"`
	BlobType             *BlobType      `xml:"BlobType"`
	CacheControl         *string        `xml:"Cache-Control"`
	ContentDisposition   *string        `xml:"Content-Disposition"`
	ContentEncoding      *string        `xml:"Content-Encoding"`
	ContentLanguage      *string        `xml:"Content-Language"`

	// Size in bytes
	ContentLength             *int64          `xml:"Content-Length"`
	ContentMD5                []byte          `xml:"Content-MD5"`
	ContentType               *string         `xml:"Content-Type"`
	CopyCompletionTime        *time.Time      `xml:"CopyCompletionTime"`
	CopyID                    *string         `xml:"CopyId"`
	CopyProgress              *string         `xml:"CopyProgress"`
	CopySource                *string         `xml:"CopySource"`
	CopyStatus                *CopyStatusType `xml:"CopyStatus"`
	CopyStatusDescription     *string         `xml:"CopyStatusDescription"`
	CreationTime              *time.Time      `xml:"Creation-Time"`
	CustomerProvidedKeySHA256 *string         `xml:"CustomerProvidedKeySha256"`
	DeletedTime               *time.Time      `xml:"DeletedTime"`
	DestinationSnapshot       *string         `xml:"DestinationSnapshot"`

	// The name of the encryption scope under which the blob is encrypted.
	EncryptionScope             *string                 `xml:"EncryptionScope"`
	ExpiresOn                   *time.Time              `xml:"Expiry-Time"`
	ImmutabilityPolicyExpiresOn *time.Time              `xml:"ImmutabilityPolicyUntilDate"`
	ImmutabilityPolicyMode      *ImmutabilityPolicyMode `xml:"ImmutabilityPolicyMode"`
	IncrementalCopy             *bool                   `xml:"IncrementalCopy"`
	IsSealed                    *bool                   `xml:"Sealed"`
	LastAccessedOn              *time.Time              `xml:"LastAccessTime"`
	LeaseDuration               *LeaseDurationType      `xml:"LeaseDuration"`
	LeaseState                  *LeaseStateType         `xml:"LeaseState"`
	LeaseStatus                 *LeaseStatusType        `xml:"LeaseStatus"`
	LegalHold                   *bool                   `xml:"LegalHold"`

	// If an object is in rehydrate pending state then this header is returned with priority of rehydrate. Valid values are High
	// and Standard.
	RehydratePriority      *RehydratePriority `xml:"RehydratePriority"`
	RemainingRetentionDays *int32             `xml:"RemainingRetentionDays"`
	ServerEncrypted        *bool              `xml:"ServerEncrypted"`
	TagCount               *int32             `xml:"TagCount"`
}

type BlobTags struct {
	// REQUIRED
	BlobTagSet []*BlobTag `xml:"TagSet>Tag"`
}

type BlobTag struct {
	// REQUIRED
	Key *string `xml:"Key"`

	// REQUIRED
	Value *string `xml:"Value"`
}

type BlobType string
type AccessTier string
type ArchiveStatus string
type CopyStatusType string
type ImmutabilityPolicyMode string
type LeaseDurationType string
type LeaseStateType string
type LeaseStatusType string
type RehydratePriority string

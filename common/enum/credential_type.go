package enum

import "github.com/Azure/azure-storage-azcopy/v10/common/enum/enum_def"

type CredentialType uint8

func (ct CredentialType) String() string {
	return ECredentialType.String(ct)
}

type eCredentialType struct {
	enum_def.EnumImpl[CredentialType, eCredentialType]
}

var ECredentialType = eCredentialType{}

// CredentialType defines the different types of credentials
func (eCredentialType) Unknown() CredentialType      { return CredentialType(0) }
func (eCredentialType) OAuthToken() CredentialType   { return CredentialType(1) } // For Azure, OAuth
func (eCredentialType) MDOAuthToken() CredentialType { return CredentialType(2) } // For Azure MD impexp
func (eCredentialType) Anonymous() CredentialType    { return CredentialType(3) } // For Azure, SAS or public.
func (eCredentialType) SharedKey() CredentialType    { return CredentialType(4) } // For Azure, SharedKey

func (eCredentialType) S3AccessKey() CredentialType    { return CredentialType(5) } // For S3, AccessKeyID and SecretAccessKey
func (eCredentialType) S3PublicBucket() CredentialType { return CredentialType(6) } // For S3, Anon Credentials & public bucket

func (eCredentialType) GoogleAppCredentials() CredentialType { return CredentialType(7) } // For GCP, App Credentials

func (ct CredentialType) IsAzureOAuth() bool {
	return ct == ECredentialType.OAuthToken() || ct == ECredentialType.MDOAuthToken()
}

func (ct CredentialType) IsSharedKey() bool {
	return ct == ECredentialType.SharedKey()
}

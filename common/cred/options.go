package cred

type GetOSKeyringOptions struct {
	OSKeyringCacheName *string

	DPAPIFilePath *string
	RootKey       *string
}

const DefaultNickname = "*"

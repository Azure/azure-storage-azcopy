package cred

type GetOSKeyringOptions struct {
	DPAPIFilePath *string

	RootKey *string
}

const DefaultNickname = "*"

package cred

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
)

// Manager pulls from multiple registered Keyring(s) and hands out
// azcore.TokenCredential values. Callers should not need to inspect the
// underlying token struct; that is treated as a serialization detail.
type Manager interface {
	ListCredentials() ([]TokenHeader, error)

	// GetCredentials returns the credential matching the nickname, or nil if not found.
	// For device code tokens that lack an auth record, manager calls Authenticate to acquire one.
	// If nickname is empty or "*", the default token is returned.
	GetCredentials(nickname string, ctx context.Context) (azcore.TokenCredential, error)

	// DoLogin uses the login token options to perform a login,
	// and saves the token, if SaveCredential is set.
	DoLogin(opts LoginNewTokenOptions, ctx context.Context) (azcore.TokenCredential, error)

	// DeleteCredentials deletes the token matching the nickname across every writable Keyring.
	// Read-only Keyrings are skipped. Returns true if any token was deleted.
	DeleteCredentials(nickname string) bool

	// ProbeToken returns the metadata for the credential matching the nickname.
	// Returns the header and true if found, or empty header and false if not found.
	ProbeToken(nickname string) (TokenHeader, bool)

	// GetKeyrings returns the list of keyrings configured on the manager, for direct interactions.
	// First returned, first searched. First returned, first used to store tokens.
	// The returned list should not be modified.
	GetKeyrings() []Keyring
}

type Keyring interface {
	ReadOnlyKeyring

	keyringImpl()
}

type ReadOnlyKeyring interface {
	// GetToken returns the token matching the nickname, and whether it was found.
	// If nickname is empty or "*", the default token is returned.
	GetToken(nickname string) (Token, bool)
}

type EnumerableKeyring interface {
	ListTokens() ([]TokenHeader, error)
}

type RWKeyring interface {
	ReadOnlyKeyring

	// DeleteToken deletes the token matching the nickname. Returns true if deleted.
	DeleteToken(nickname string) bool

	// SaveToken will use the tenant ID as the nickname if the nickname is an empty string.
	SaveToken(info Token) error
}

// NewTokenOptions creates a Token from explicit per-type configuration.
type NewTokenOptions interface {
	NewToken() Token
}

// tokenImpl provides a TokenCredential provider, backwards compat
type tokenImpl interface {
	tokenImpl()
	getTokenCredential(header TokenHeader, ctx context.Context) (azcore.TokenCredential, error)
	fromCompat(compat compatTokenInfo) tokenImpl
}

type Token interface {
	tokenStruct()

	Header() TokenHeader
	TokenCredential(ctx context.Context) (azcore.TokenCredential, error)
}

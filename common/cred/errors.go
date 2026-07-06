package cred

import (
	"fmt"
)

// ErrorCredentialNotFound is returned when a token lookup does not find a matching token.
type ErrorCredentialNotFound struct {
	nickname string
}

func (e ErrorCredentialNotFound) Error() string {
	return fmt.Sprintf("no credential found for nickname %q", e.nickname)
}

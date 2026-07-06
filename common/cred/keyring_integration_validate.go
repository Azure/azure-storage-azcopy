package cred

// intentional; enforces typing is consistent across OSes, and that all compilation targets have an implementation.
//
//goland:noinspection GoVarAndConstTypeMayBeOmitted
var _ func() (Keyring, error) = GetIntegrationKeyring

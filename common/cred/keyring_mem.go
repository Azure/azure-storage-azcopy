package cred

type memKeyring struct {
	identities map[string]token
}

func NewMemKeyring(identities map[string]token) Keyring {
	var out = make(map[string]token)
	for k, v := range identities {
		out[k] = v
	}

	return &memKeyring{
		identities: out,
	}
}

func (m *memKeyring) GetToken(nickname string) (token, bool) {
	if nickname == "" {
		nickname = DefaultNickname
	}

	token, ok := m.identities[nickname]
	if !ok && nickname != DefaultNickname {
		token, ok = m.identities[DefaultNickname]
	}

	return token, ok
}

func (m *memKeyring) ListTokens() ([]TokenHeader, error) {
	var out []TokenHeader
	for _, v := range m.identities {
		out = append(out, v.TokenHeader)
	}

	return out, nil
}

func (m *memKeyring) keyringImpl() {}

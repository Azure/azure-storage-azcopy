package cred

type memKeyring struct {
	identities map[string]Token
}

func NewMemKeyring(identities map[string]Token) Keyring {
	var out = make(map[string]Token)
	for k, v := range identities {
		out[k] = v
	}

	return &memKeyring{
		identities: out,
	}
}

func (m *memKeyring) GetToken(nickname string) (Token, bool) {
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
		out = append(out, v.Header())
	}

	return out, nil
}

func (m *memKeyring) keyringImpl() {}

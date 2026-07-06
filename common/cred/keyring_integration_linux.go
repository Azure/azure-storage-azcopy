package cred

import "C"
import (
	"encoding/json"
	"os"
	"strconv"

	libsecret "github.com/lescuer97/go-libsecret"
)

const (
	IntegrationPidAttrName      = "pid"
	IntegrationTenantIdAttrName = "tenantId"
	IntegrationNicknameAttrName = "nickname"
)

func GetIntegrationKeyring() (Keyring, error) {
	schema, err := libsecret.NewSchema("org.microsoft.AzCopy.Identities",
		libsecret.SchemaFlagsNone,
		map[string]libsecret.SchemaAttributeType{
			IntegrationPidAttrName:      libsecret.SchemaAttributeString,
			IntegrationTenantIdAttrName: libsecret.SchemaAttributeString,
			IntegrationNicknameAttrName: libsecret.SchemaAttributeString,
		},
	)
	if err != nil {
		return nil, err
	}

	return &gnomeKeyring{
		schema: schema,
	}, nil
}

type gnomeKeyring struct {
	schema *libsecret.Schema
}

func (g gnomeKeyring) GetToken(nickname string) (token, bool) {
	if nickname == "" {
		nickname = DefaultNickname
	}

	attributes := map[string]string{
		"pid":      strconv.Itoa(os.Getpid()),
		"nickname": nickname,
	}

	results, err := libsecret.SearchPasswords(g.schema, attributes, libsecret.SearchFlagsAll)
	if err != nil || len(results) == 0 {
		if nickname != DefaultNickname {
			return g.getToken(DefaultNickname)
		}
		return token{}, false
	}

	v := results[0]
	val, err := v.RetrieveSecret()
	if err != nil {
		if nickname != DefaultNickname {
			return g.getToken(DefaultNickname)
		}
		return token{}, false
	}

	buf, n, err := val.Get()
	if err != nil {
		if nickname != DefaultNickname {
			return g.getToken(DefaultNickname)
		}
		return token{}, false
	}

	var out token
	err = json.Unmarshal(buf[:n], &out)
	if err != nil {
		if nickname != DefaultNickname {
			return g.getToken(DefaultNickname)
		}
		return token{}, false
	}

	return out, true
}

func (g gnomeKeyring) getToken(nickname string) (token, bool) {
	attributes := map[string]string{
		"pid":      strconv.Itoa(os.Getpid()),
		"nickname": nickname,
	}

	results, err := libsecret.SearchPasswords(g.schema, attributes, libsecret.SearchFlagsAll)
	if err != nil || len(results) == 0 {
		return token{}, false
	}

	v := results[0]
	val, err := v.RetrieveSecret()
	if err != nil {
		return token{}, false
	}

	buf, n, err := val.Get()
	if err != nil {
		return token{}, false
	}

	var out token
	err = json.Unmarshal(buf[:n], &out)
	if err != nil {
		return token{}, false
	}

	return out, true
}

func (g gnomeKeyring) keyringImpl() {}

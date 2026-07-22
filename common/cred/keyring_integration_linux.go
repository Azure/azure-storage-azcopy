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

func (g gnomeKeyring) GetToken(nickname string) (Token, bool) {
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
		return nil, false
	}

	v := results[0]
	val, err := v.RetrieveSecret()
	if err != nil {
		if nickname != DefaultNickname {
			return g.getToken(DefaultNickname)
		}
		return nil, false
	}

	buf, n, err := val.Get()
	if err != nil {
		if nickname != DefaultNickname {
			return g.getToken(DefaultNickname)
		}
		return nil, false
	}

	var out token
	err = json.Unmarshal(buf[:n], &out)
	if err != nil {
		if nickname != DefaultNickname {
			return g.getToken(DefaultNickname)
		}
		return nil, false
	}

	return &out, true
}

func (g gnomeKeyring) getToken(nickname string) (Token, bool) {
	attributes := map[string]string{
		"pid":      strconv.Itoa(os.Getpid()),
		"nickname": nickname,
	}

	results, err := libsecret.SearchPasswords(g.schema, attributes, libsecret.SearchFlagsAll)
	if err != nil || len(results) == 0 {
		return nil, false
	}

	v := results[0]
	val, err := v.RetrieveSecret()
	if err != nil {
		return nil, false
	}

	buf, n, err := val.Get()
	if err != nil {
		return nil, false
	}

	var out token
	err = json.Unmarshal(buf[:n], &out)
	if err != nil {
		return nil, false
	}

	return &out, true
}

func (g gnomeKeyring) keyringImpl() {}

package cred

import (
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/Azure/azure-storage-azcopy/v10/common/enum"
)

// tokenTypeImplMapping should not be referenced directly; Instead use the wrapper newTokenImpl, as it handles the few pointer cases (tokenStore, userLogin)
var tokenTypeImplMapping = map[enum.AutoLoginType]reflect.Type{
	enum.EAutoLoginType.SPN():         reflect.TypeFor[tokenInfoSPN](),
	enum.EAutoLoginType.MSI():         reflect.TypeFor[tokenInfoManagedIdentity](),
	enum.EAutoLoginType.Workload():    reflect.TypeFor[tokenInfoWorkload](),
	enum.EAutoLoginType.AzCLI():       reflect.TypeFor[tokenInfoCLI](),
	enum.EAutoLoginType.Device():      reflect.TypeFor[*tokenInfoUserLogin](),
	enum.EAutoLoginType.Interactive(): reflect.TypeFor[*tokenInfoUserLogin](),
	enum.EAutoLoginType.PsCred():      reflect.TypeFor[tokenInfoPSCred](),
	enum.EAutoLoginType.TokenStore():  reflect.TypeFor[*tokenInfoTokenStore](),
}

func init() {
	// ensure all types specified above comply with the expected type
	ifType := reflect.TypeFor[tokenImpl]()
	for _, v := range tokenTypeImplMapping {
		if !v.AssignableTo(ifType) {
			panic(fmt.Sprintf("%s must be assignable to TokenImpl!", v.Name()))
		}
	}
}

func newTokenImpl(loginType enum.AutoLoginType) tokenImpl {
	outType := tokenTypeImplMapping[loginType]

	var val reflect.Value
	if outType.Kind() == reflect.Pointer {
		val = reflect.New(outType.Elem())
	} else {
		val = reflect.New(outType)
	}

	return val.Interface().(tokenImpl)
}

func unmarshalTokenImpl(buf []byte, loginType enum.AutoLoginType) (tokenImpl, error) {
	outType := tokenTypeImplMapping[loginType]

	var val, ref reflect.Value
	if outType.Kind() == reflect.Pointer {
		val = reflect.New(outType.Elem())
		ref = val
	} else {
		ref = reflect.New(outType)
		val = ref.Elem()
	}

	err := json.Unmarshal(buf, ref.Interface())

	return val.Interface().(tokenImpl), err
}

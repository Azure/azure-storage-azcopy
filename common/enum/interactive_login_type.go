package enum

import (
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common/enum/enum_def"
)

type InteractiveLoginType uint8

func (i InteractiveLoginType) String() string {
	return EInteractiveLoginType.String(i)
}

type eInteractiveLoginType struct {
	enum_def.EnumImpl[InteractiveLoginType, eInteractiveLoginType]
}

var EInteractiveLoginType = eInteractiveLoginType{}

func (eInteractiveLoginType) Device() InteractiveLoginType  { return InteractiveLoginType(0) }
func (eInteractiveLoginType) Browser() InteractiveLoginType { return InteractiveLoginType(1) }

func (i InteractiveLoginType) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.String())
}

func (i *InteractiveLoginType) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	val, ok := EInteractiveLoginType.Parse(s)
	if !ok {
		return fmt.Errorf("couldn't parse %q into an InteractiveLoginType", s)
	}
	*i = val
	return nil
}

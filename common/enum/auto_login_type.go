package enum

import (
	"encoding/json"
	"fmt"

	"github.com/Azure/azure-storage-azcopy/v10/common/data_structures"
	"github.com/Azure/azure-storage-azcopy/v10/common/enum/enum_def"
)

type AutoLoginType uint8

func (a AutoLoginType) String() string {
	return EAutoLoginType.String(a)
}

type eAutoLoginType struct {
	enum_def.EnumImpl[AutoLoginType, eAutoLoginType]
}

var EAutoLoginType = eAutoLoginType{}

func (eAutoLoginType) Device() AutoLoginType      { return AutoLoginType(0) }
func (eAutoLoginType) SPN() AutoLoginType         { return AutoLoginType(1) }
func (eAutoLoginType) MSI() AutoLoginType         { return AutoLoginType(2) }
func (eAutoLoginType) AzCLI() AutoLoginType       { return AutoLoginType(3) }
func (eAutoLoginType) PsCred() AutoLoginType      { return AutoLoginType(4) }
func (eAutoLoginType) Workload() AutoLoginType    { return AutoLoginType(5) }
func (eAutoLoginType) Interactive() AutoLoginType { return AutoLoginType(6) }
func (eAutoLoginType) NoRefresh() AutoLoginType   { return AutoLoginType(254) } // NoRefresh indicates the credential should not be refreshed; used for pre-obtained tokens injected via environment keyring.
func (eAutoLoginType) TokenStore() AutoLoginType  { return AutoLoginType(255) } // Storage Explorer internal integration only. Do not add this to ValidAutoLoginTypes.

var ValidAutoLoginTypes = func() []string {
	nonPublic := data_structures.NewSet(
		EAutoLoginType.TokenStore(),
		EAutoLoginType.NoRefresh(),
	)
	helpTexts := map[AutoLoginType]string{
		EAutoLoginType.Device():      "Device code authentication (browser-based login)",
		EAutoLoginType.SPN():         "Service principal (client secret or certificate)",
		EAutoLoginType.MSI():         "Managed identity (Azure resource authentication)",
		EAutoLoginType.AzCLI():       "Azure CLI credentials",
		EAutoLoginType.PsCred():      "PowerShell credentials",
		EAutoLoginType.Workload():    "Workload identity federation",
		EAutoLoginType.Interactive(): "Interactive browser login",
	}

	var out []string

	for val := range EAutoLoginType.Values() {
		if nonPublic.Contains(val) {
			continue
		}

		if helpText, ok := helpTexts[val]; ok {
			out = append(out, fmt.Sprintf("%s (%s)", val.String(), helpText))
			continue
		}

		// Panic if a docstring isn't present, because we want this to be an obvious, show-stopping problem.
		panic(fmt.Sprintf("AutoLoginType `%s` is not included in either nonPublic or helpTexts!", val.String()))
	}

	return out
}

func (a AutoLoginType) IsInteractive() bool {
	return a == EAutoLoginType.Device()
}

// MarshalJSON customizes the JSON encoding for AutoLoginType
func (a AutoLoginType) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

// UnmarshalJSON customizes the JSON decoding for AutoLoginType
func (a *AutoLoginType) UnmarshalJSON(data []byte) error {
	var v interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	if strValue, ok := v.(string); ok {
		*a, ok = EAutoLoginType.Parse(strValue)
		if !ok {
			return fmt.Errorf("couldn't parse `%s` into a %s", strValue)
		}

		return nil
	}
	// Handle numeric values
	if numValue, ok := v.(float64); ok {
		if numValue < 0 || numValue > 255 {
			return fmt.Errorf("value out of range for _token_source_refresh: %v", numValue)
		}
		*a = AutoLoginType(uint8(numValue))
		return nil
	}

	return fmt.Errorf("unsupported type for AutoLoginType: %T", v)
}

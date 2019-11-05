package sddl

import (
	"fmt"
	"strings"
)

type ACEResourceAttributeValuesType uint8
type eACEResourceAttributeValuesType uint8

var EACEResourceAttributeValuesType eACEResourceAttributeValuesType = 0
var shorthandACEResourceAttributeValuesTypes = map[string]ACEResourceAttributeValuesType{
	"TI": EACEResourceAttributeValuesType.SDDL_INT(),
	"TU": EACEResourceAttributeValuesType.SDDL_UINT(),
	"TS": EACEResourceAttributeValuesType.SDDL_WSTRING(),
	"TD": EACEResourceAttributeValuesType.SDDL_SID(),
	"TX": EACEResourceAttributeValuesType.SDDL_BLOB(),
	"TB": EACEResourceAttributeValuesType.SDDL_BOOLEAN(),
}

var reverseShorthandACEResourceAttributeValuesTypes = map[ACEResourceAttributeValuesType]string{
	EACEResourceAttributeValuesType.SDDL_INT():     "TI",
	EACEResourceAttributeValuesType.SDDL_UINT():    "TU",
	EACEResourceAttributeValuesType.SDDL_WSTRING(): "TS",
	EACEResourceAttributeValuesType.SDDL_SID():     "TD",
	EACEResourceAttributeValuesType.SDDL_BLOB():    "TX",
	EACEResourceAttributeValuesType.SDDL_BOOLEAN(): "TB",
}

func ParseACEResourceAttributeValuesType(input string) (ACEResourceAttributeValuesType, error) {
	t, ok := shorthandACEResourceAttributeValuesTypes[strings.ToUpper(input)]

	if !ok {
		return 0, fmt.Errorf("%s is an invalid ACE Resource Attribute Type", input)
	}

	return t, nil
}

func (a ACEResourceAttributeValuesType) String() string {
	output, ok := reverseShorthandACEResourceAttributeValuesTypes[a]

	if !ok {
		panic(fmt.Sprintf("%d is an invalid ACE Resource Attribute Type", a))
	}

	return output
}

func (eACEResourceAttributeValuesType) SDDL_INT() ACEResourceAttributeValuesType {
	return 0
}

func (eACEResourceAttributeValuesType) SDDL_UINT() ACEResourceAttributeValuesType {
	return 1
}

func (eACEResourceAttributeValuesType) SDDL_WSTRING() ACEResourceAttributeValuesType {
	return 2
}

func (eACEResourceAttributeValuesType) SDDL_SID() ACEResourceAttributeValuesType {
	return 3
}

func (eACEResourceAttributeValuesType) SDDL_BLOB() ACEResourceAttributeValuesType {
	return 4
}

func (eACEResourceAttributeValuesType) SDDL_BOOLEAN() ACEResourceAttributeValuesType {
	return 5
}

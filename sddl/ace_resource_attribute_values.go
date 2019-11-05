package sddl

import (
	"fmt"
	"strconv"
	"strings"
)

type ACEResourceAttributeValues struct {
	Name           string
	Type           ACEResourceAttributeValuesType
	AttributeFlags int32 // check https://docs.microsoft.com/en-us/openspecs/windows_protocols/ms-dtyp/f4296d69-1c0f-491f-9587-a960b292d070 for the attr-flags spec
	Items          []string
}

func (a ACEResourceAttributeValues) StringifyResourceAttribute() string {
	output := fmt.Sprintf(`("%s",%s,%d,`, a.Name, a.Type.String(), a.AttributeFlags)

	for k, v := range a.Items {
		output += v

		if k < len(a.Items)-1 {
			output += ","
		}
	}

	output += ")"

	return output
}

// internal method used to shorten parsing
func (a *ACEResourceAttributeValues) commitResourcePart(raPart int, data string) error {
	switch raPart {
	case 0: // We're just doing the name. Trim the string identifiers and commit it.
		a.Name = strings.TrimPrefix(strings.TrimSuffix(data, `"`), `"`)
	case 1: // We're applying the RA type. Parse it and commit it.
		t, err := ParseACEResourceAttributeValuesType(data)
		if err != nil {
			return err
		}

		a.Type = t
	case 2: // We're applying attribute flags. Parse it as an int and commit it.
		i, err := strconv.ParseInt(data, 10, 32)
		if err != nil {
			return err
		}

		a.AttributeFlags = int32(i)
	default: // Now we're in the territory of Items. Just append the string and don't worry.
		a.Items = append(a.Items, data)
	}

	return nil
}

func ParseACEResourceAttributeValues(input string) (ACEResourceAttributeValues, error) {
	output := ACEResourceAttributeValues{}

	raPart := 0
	inString := false
	partStarted := false
	partStart := 0

	if !strings.HasSuffix(input, ")") || !strings.HasPrefix(input, "(") {
		return output, fmt.Errorf("resource attribute lacks surrounding parentheses")
	}

	// Trim the () from around the resource attribute value.
	workingInput := strings.TrimPrefix(strings.TrimSuffix(input, ")"), "(")
	for k, v := range strings.Split(workingInput, "") {
		switch {
		case v == `"`: // support string escaping
			if k == 0 || input[k-1] != '\\' {
				inString = !inString
			}
		case inString:
			// Do nothing.
		case v == " ": // Data should be contiguous.
			if !partStarted {
				partStart++
			} else {
				return output, fmt.Errorf("at char %d of resource attribute: data stopped being contiguous", k)
			}
		default: // The part has started, as it isn't any kind of interrupt character.
			partStarted = true
		}

		if (v == "," || k == len(workingInput)-1) && !inString {
			err := output.commitResourcePart(raPart, workingInput[partStart:ternaryInt(k == len(workingInput)-1, len(workingInput), k)])

			if err != nil {
				return output, fmt.Errorf("at char %d of resource attribute: %s", k+1, err.Error())
			}

			partStart = k + 1
			partStarted = false
			raPart++
		}
	}

	return output, nil
}

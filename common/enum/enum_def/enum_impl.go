package enum_def

import (
	"fmt"
	"iter"
	"reflect"
	"strings"

	"github.com/Azure/azure-storage-azcopy/v10/common/data_structures"
)

type iEnumSelf interface {
	enumImpl()
}

type iEnumValue interface {
	comparable
	fmt.Stringer
}

var initMap = data_structures.NewSyncMap[reflect.Type, bool]()

type EnumImpl[Val iEnumValue, Self iEnumSelf] struct {
	enumBackend[Val, Self]
}

type EnumImplRawString[Self iEnumSelf] struct {
	enumBackend[string, Self]
}

// EnumImpl should be embedded in it's parent struct, with the value the functions enumerate as Val, and the parent struct as Self.
// All exported functions with no inputs and a Val output will be registered as values of the enumeration.
// Values prefixed Alias_ will parse, but when stringified, will return their canonical value.
type enumBackend[Val comparable, Self iEnumSelf] struct {
	nameDict  map[string]Val
	valueDict map[Val]string
}

func (e enumBackend[Val, Self]) enumImpl() {
	panic("used for interface fulfillment; no function")
}

func (e *enumBackend[Val, Self]) init() {
	enumType := reflect.TypeFor[Self]()
	// A "once" in map form
	_, loaded := initMap.LoadOrStore(enumType, true)

	if loaded {
		// short circuit, we've already initialized.
		return
	}

	if e.valueDict != nil || e.nameDict != nil {
		panic("sanity check: init successfully ran twice?")
	}

	e.valueDict = make(map[Val]string)
	e.nameDict = make(map[string]Val)

	valType := reflect.TypeFor[Val]()
	enumVal := reflect.Zero(enumType)

	type aliasEntry struct {
		name string
		val  Val
	}
	var aliases []aliasEntry

	for m := range enumType.Methods() {
		// Ensure format of `func (Self) FooBar() Val` or `func (*Self) FooBar() Val`
		if m.Type.NumIn() != 1 ||
			(m.Type.In(0) != enumType &&
				m.Type.In(0) != reflect.PointerTo(enumType)) {
			continue
		}

		if m.Type.NumOut() != 1 ||
			m.Type.Out(0) != valType {
			continue
		}

		// Call it, pull value
		out := (enumVal.Method(m.Index).Call([]reflect.Value{})[0].Interface()).(Val)

		if strings.HasPrefix(m.Name, "Alias_") {
			parseName := strings.TrimPrefix(m.Name, "Alias_")
			aliases = append(aliases, aliasEntry{name: parseName, val: out})
		} else {
			e.valueDict[out] = m.Name
			e.nameDict[strings.ToLower(m.Name)] = out
		}
	}

	// Validate and register aliases after all canonical values are known.
	for _, a := range aliases {
		if _, exists := e.valueDict[a.val]; !exists {
			panic(fmt.Sprintf("enum alias %q has value %v that does not match any canonical enum value", a.name, a.val))
		}
		e.nameDict[strings.ToLower(a.name)] = a.val
	}
}

func (e *enumBackend[Val, Self]) String(val Val) string {
	e.init()

	out, ok := e.valueDict[val]
	if !ok {
		return ""
	}

	return out
}

func (e *enumBackend[Val, Self]) Parse(str string) (val Val, ok bool) {
	e.init()

	val, ok = e.nameDict[strings.ToLower(str)]
	return
}

func (e *enumBackend[Val, Self]) Values() iter.Seq[Val] {
	e.init()

	return func(yield func(Val) bool) {
		for _, v := range e.nameDict {
			if !yield(v) {
				return
			}
		}
	}
}

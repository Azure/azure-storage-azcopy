package enum_def

import (
	"slices"
	"testing"
)

// --- EnumImpl via reflection ---

type testPillVal uint8

func (t testPillVal) String() string {
	return pillEnum.String(t)
}

type pillEnumImpl struct {
	EnumImpl[testPillVal, pillEnumImpl]
}

var pillEnum = pillEnumImpl{}

func (pillEnumImpl) Red() testPillVal   { return testPillVal(0) }
func (pillEnumImpl) Blue() testPillVal  { return testPillVal(1) }
func (pillEnumImpl) Green() testPillVal { return testPillVal(2) }

func TestEnumImpl_String(t *testing.T) {
	if v := pillEnum.String(testPillVal(0)); v != "Red" {
		t.Errorf("expected Red, got %q", v)
	}
	if v := pillEnum.String(testPillVal(1)); v != "Blue" {
		t.Errorf("expected Blue, got %q", v)
	}
	if v := pillEnum.String(testPillVal(2)); v != "Green" {
		t.Errorf("expected Green, got %q", v)
	}
}

func TestEnumImpl_String_Unknown(t *testing.T) {
	if v := pillEnum.String(testPillVal(99)); v != "" {
		t.Errorf("expected empty, got %q", v)
	}
}

func TestEnumImpl_Parse(t *testing.T) {
	val, ok := pillEnum.Parse("Red")
	if !ok || val != testPillVal(0) {
		t.Errorf("expected Red -> 0, got %v, %v", val, ok)
	}
	val, ok = pillEnum.Parse("red")
	if !ok || val != testPillVal(0) {
		t.Errorf("expected red -> 0, got %v, %v", val, ok)
	}
	val, ok = pillEnum.Parse("BLUE")
	if !ok || val != testPillVal(1) {
		t.Errorf("expected BLUE -> 1, got %v, %v", val, ok)
	}
}

func TestEnumImpl_Parse_Unknown(t *testing.T) {
	_, ok := pillEnum.Parse("missing")
	if ok {
		t.Errorf("expected not ok for missing")
	}
}

func TestEnumImpl_Values(t *testing.T) {
	all := slices.Collect(pillEnum.Values())
	if len(all) != 3 {
		t.Fatalf("expected 3 values, got %d", len(all))
	}
	m := map[testPillVal]bool{}
	for _, v := range all {
		m[v] = true
	}
	if !m[0] || !m[1] || !m[2] {
		t.Errorf("Values() didn't yield all expected values: got %v", all)
	}
}

func TestEnumImpl_RoundTrip(t *testing.T) {
	for _, v := range []testPillVal{0, 1, 2} {
		s := pillEnum.String(v)
		parsed, ok := pillEnum.Parse(s)
		if !ok || parsed != v {
			t.Errorf("round-trip failed for %d: String=%q, Parse=%v,%v", v, s, parsed, ok)
		}
	}
}

func TestEnumImpl_ValueString(t *testing.T) {
	if v := testPillVal(0).String(); v != "Red" {
		t.Errorf("expected Red, got %q", v)
	}
}

// --- EnumImpl with empty enum (no methods) ---

type emptyEnumVal uint8

func (e emptyEnumVal) String() string {
	return ""
}

type emptyEnumImpl struct {
	EnumImpl[emptyEnumVal, emptyEnumImpl]
}

var emptyEnum = emptyEnumImpl{}

func TestEnumImpl_EmptyEnum(t *testing.T) {
	found := false
	for range emptyEnum.Values() {
		found = true
	}
	if found {
		t.Error("expected no values from empty enum")
	}
	if v := emptyEnum.String(emptyEnumVal(0)); v != "" {
		t.Errorf("expected empty string, got %q", v)
	}
	_, ok := emptyEnum.Parse("anything")
	if ok {
		t.Errorf("expected not ok for parse on empty enum")
	}
}

// --- EnumImpl with aliases ---

type testAliasPillVal uint8

func (t testAliasPillVal) String() string {
	return aliasPillEnum.String(t)
}

type aliasPillEnumImpl struct {
	EnumImpl[testAliasPillVal, aliasPillEnumImpl]
}

var aliasPillEnum = aliasPillEnumImpl{}

func (aliasPillEnumImpl) Painkiller() testAliasPillVal        { return testAliasPillVal(0) }
func (aliasPillEnumImpl) Antibiotic() testAliasPillVal        { return testAliasPillVal(1) }
func (aliasPillEnumImpl) Alias_Analgesic() testAliasPillVal   { return testAliasPillVal(0) }
func (aliasPillEnumImpl) Alias_Antibacterial() testAliasPillVal { return testAliasPillVal(1) }

func TestEnumImpl_Alias_Parses(t *testing.T) {
	val, ok := aliasPillEnum.Parse("Analgesic")
	if !ok || val != testAliasPillVal(0) {
		t.Errorf("expected alias Analgesic -> 0, got %v, %v", val, ok)
	}
	val, ok = aliasPillEnum.Parse("Antibacterial")
	if !ok || val != testAliasPillVal(1) {
		t.Errorf("expected alias Antibacterial -> 1, got %v, %v", val, ok)
	}
}

func TestEnumImpl_Alias_CanonicalString(t *testing.T) {
	if v := aliasPillEnum.String(testAliasPillVal(0)); v != "Painkiller" {
		t.Errorf("expected canonical Painkiller, got %q", v)
	}
	if v := aliasPillEnum.String(testAliasPillVal(1)); v != "Antibiotic" {
		t.Errorf("expected canonical Antibiotic, got %q", v)
	}
}

func TestEnumImpl_Alias_CanonicalNameAccepted(t *testing.T) {
	val, ok := aliasPillEnum.Parse("Painkiller")
	if !ok || val != testAliasPillVal(0) {
		t.Errorf("expected Painkiller -> 0, got %v, %v", val, ok)
	}
}

// --- EnumImplRawString via reflection ---

type fruitEnumImpl struct {
	EnumImplRawString[fruitEnumImpl]
}

var fruitEnum = fruitEnumImpl{}

func (fruitEnumImpl) Apple() string  { return "apple-fruit" }
func (fruitEnumImpl) Banana() string { return "banana-fruit" }

func TestEnumImplRawString_String(t *testing.T) {
	if v := fruitEnum.String("apple-fruit"); v != "Apple" {
		t.Errorf("expected Apple, got %q", v)
	}
	if v := fruitEnum.String("banana-fruit"); v != "Banana" {
		t.Errorf("expected Banana, got %q", v)
	}
}

func TestEnumImplRawString_String_Unknown(t *testing.T) {
	if v := fruitEnum.String("durian"); v != "" {
		t.Errorf("expected empty, got %q", v)
	}
}

func TestEnumImplRawString_Parse(t *testing.T) {
	val, ok := fruitEnum.Parse("Apple")
	if !ok || val != "apple-fruit" {
		t.Errorf("expected Apple -> 'apple-fruit', got %v, %v", val, ok)
	}
	val, ok = fruitEnum.Parse("apple")
	if !ok || val != "apple-fruit" {
		t.Errorf("expected apple -> 'apple-fruit', got %v, %v", val, ok)
	}
}

func TestEnumImplRawString_Parse_Unknown(t *testing.T) {
	_, ok := fruitEnum.Parse("missing")
	if ok {
		t.Errorf("expected not ok for missing")
	}
}

func TestEnumImplRawString_Values(t *testing.T) {
	all := slices.Collect(fruitEnum.Values())
	if len(all) != 2 {
		t.Fatalf("expected 2 values, got %d", len(all))
	}
	m := map[string]bool{}
	for _, v := range all {
		m[v] = true
	}
	if !m["apple-fruit"] || !m["banana-fruit"] {
		t.Errorf("Values() didn't yield all expected: got %v", all)
	}
}

func TestEnumImplRawString_RoundTrip(t *testing.T) {
	for _, v := range []string{"apple-fruit", "banana-fruit"} {
		s := fruitEnum.String(v)
		parsed, ok := fruitEnum.Parse(s)
		if !ok || parsed != v {
			t.Errorf("round-trip failed for %q: String=%q, Parse=%v,%v", v, s, parsed, ok)
		}
	}
}

// --- Init idempotency ---

type colorEnumImpl struct {
	EnumImpl[testPillVal, colorEnumImpl]
}

var colorEnum = colorEnumImpl{}

func (colorEnumImpl) Cyan() testPillVal    { return testPillVal(10) }
func (colorEnumImpl) Magenta() testPillVal { return testPillVal(11) }
func (colorEnumImpl) Yellow() testPillVal  { return testPillVal(12) }
func (colorEnumImpl) Key() testPillVal     { return testPillVal(13) }

func TestEnumImpl_InitOnce(t *testing.T) {
	if v := colorEnum.String(testPillVal(10)); v != "Cyan" {
		t.Errorf("expected Cyan, got %q", v)
	}
	if v := colorEnum.String(testPillVal(10)); v != "Cyan" {
		t.Errorf("expected Cyan on second call, got %q", v)
	}
	val, ok := colorEnum.Parse("Cyan")
	if !ok || val != testPillVal(10) {
		t.Errorf("expected Cyan -> 10, got %v, %v", val, ok)
	}
}

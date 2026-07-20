package azcopy

import (
	"testing"
)

func TestStartsWith(t *testing.T) {
	scenarios := []struct {
		name            string
		s               string
		potentialPrefix string
		expected        bool
	}{
		// The method we're testing doesn't trim either string, so there's no need to test whitespace.

		// Empty-string Handling:
		{"Empty String: Empty String and Non-Empty Potential Prefix", "", "non-empty", false},
		{"Empty String: Empty String and Empty Potential Prefix", "", "", true},
		{"Empty String: Non-Empty String and Empty Potential Prefix", "non-empty", "", true},

		// Simple Strings to Test the Basic Logic:
		{"Simple Strings: Longer Potential Prefix", "n", "nn", false},
		{"Simple Strings: Equal-Length Potential Prefix That Doesn't Match", "n", "o", false},
		{"Simple Strings: Equal-Length Potential Prefix That Matches (Same Case)", "n", "n", true},
		{"Simple Strings: Equal-Length Potential Prefix That Matches (Lower vs. Upper Case)", "n", "N", true},
		{"Simple Strings: Equal-Length Potential Prefix That Matches (Upper vs. Lower Case)", "N", "n", true},
		{"Simple Strings: Shorter Potential Prefix That Doesn't Match", "nn", "o", false},
		{"Simple Strings: Shorter Potential Prefix That Matches (Same Case)", "nn", "n", true},
		{"Simple Strings: Shorter Potential Prefix That Matches (Upper vs. Lower Case)", "NN", "n", true},
		{"Simple Strings: Shorter Potential Prefix That Matches (Lower vs. Upper Case)", "nn", "N", true},

		// Complex Strings to Test Handling of Characters Other Than ASCII Letters:
		{"Complex Strings: Longer Potential Prefix", "jalapeño-123æ", "jalapeño-123æ4", false},
		{"Complex Strings: Equal-Length Potential Prefix That Doesn't Match", "jalapeño-123æ", "jalapeño-123_", false},
		{"Complex Strings: Equal-Length Potential Prefix That Matches (Same Case)", "jalapeño-123æ", "jalapeño-123æ", true},
		{"Complex Strings: Equal-Length Potential Prefix That Matches (Lower vs. Upper Case)", "jalapeño-123æ", "JALAPEÑO-123Æ", true},
		{"Complex Strings: Equal-Length Potential Prefix That Matches (Upper vs. Lower Case)", "JALAPEÑO-123Æ", "jalapeño-123æ", true},
		{"Complex Strings: Shorter Potential Prefix That Doesn't Match", "jalapeño-123æ", "jalapeño-12*", false},
		{"Complex Strings: Shorter Potential Prefix That Matches (Same Case)", "jalapeño-123æ", "jalapeño-12", true},
		{"Complex Strings: Shorter Potential Prefix That Matches (Upper vs. Lower Case)", "JALAPEÑO-123Æ", "jalapeño-1", true},
		{"Complex Strings: Shorter Potential Prefix That Matches (Lower vs. Upper Case)", "jalapeño-123æ", "JALAPEÑO-", true},
		// If we need to support full case folding or languages other than English, see the '"ẞ" vs. "ss"' example in the
		// official Go documentation for strings.EqualFold, as well as the examples at https://www.w3.org/TR/charmod-norm.
		// NOTE: English has ligatures like "æ", "œ", and their upper-case versions, which have digraphs like "ae", "oe", etc.
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			actual := StartsWith(scenario.s, scenario.potentialPrefix)
			if actual != scenario.expected {
				// We're not using `assert` here, because `assert` doesn't handle scenarios properly.
				// With assert, when a scenario fails, `go test` labels the test `FAIL` and prints the error,
				// BUT it labels all scenarios `PASS`.
				t.Errorf("Scenario %q failed: StartsWith(%q, %q) returned %v, expected %v", scenario.name, scenario.s, scenario.potentialPrefix, actual, scenario.expected)
			}
		})
	}
}

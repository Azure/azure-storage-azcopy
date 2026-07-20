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

		// Simple ASCII Scenarios:
		{"ASCII: Longer Potential Prefix", "n", "nn", false},
		{"ASCII: Equal-Length Potential Prefix That Doesn't Match", "n", "o", false},
		{"ASCII: Equal-Length Potential Prefix That Matches (Same Case)", "n", "n", true},
		{"ASCII: Equal-Length Potential Prefix That Matches (Lower vs. Upper Case)", "n", "N", true},
		{"ASCII: Equal-Length Potential Prefix That Matches (Upper vs. Lower Case)", "N", "n", true},
		{"ASCII: Shorter Potential Prefix That Doesn't Match", "nn", "o", false},
		{"ASCII: Shorter Potential Prefix That Matches (Same Case)", "nn", "n", true},
		{"ASCII: Shorter Potential Prefix That Matches (Upper vs. Lower Case)", "NN", "n", true},
		{"ASCII: Shorter Potential Prefix That Matches (Lower vs. Upper Case)", "nn", "N", true},

		// Non-ASCII Scenarios:
		{"Non-ASCII: Longer Potential Prefix", "ñ", "ññ", false},
		{"Non-ASCII: Equal-Length Potential Prefix That Doesn't Match", "ñ", "ó", false},
		{"Non-ASCII: Equal-Length Potential Prefix That Matches (Same Case)", "ñ", "ñ", true},
		{"Non-ASCII: Equal-Length Potential Prefix That Matches (Lower vs. Upper Case)", "ñ", "Ñ", true},
		{"Non-ASCII: Equal-Length Potential Prefix That Matches (Upper vs. Lower Case)", "Ñ", "ñ", true},
		{"Non-ASCII: Shorter Potential Prefix That Doesn't Match", "ññ", "ó", false},
		{"Non-ASCII: Shorter Potential Prefix That Matches (Same Case)", "ññ", "ñ", true},
		{"Non-ASCII: Shorter Potential Prefix That Matches (Upper vs. Lower Case)", "ÑÑ", "ñ", true},
		{"Non-ASCII: Shorter Potential Prefix That Matches (Lower vs. Upper Case)", "ññ", "Ñ", true},
		// If we need to support full case folding or languages other than English, see the '"ẞ" vs. "ss"' example in the
		// official Go documentation for strings.EqualFold, as well as the examples at https://www.w3.org/TR/charmod-norm.
		// NOTE: English has ligatures like "æ", "œ", and their upper-case versions, which have digraphs like "ae", etc.
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

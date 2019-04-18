package cmd

import (
	"testing"
)

func TestBToString(t *testing.T) {
	inputs := []int64{50, 100, 125}
	expects := []string{"50.00 B", "100.00 B", "125.00 B"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		if output != expects[k] {
			t.Errorf("Expected %s, got %s", expects[k], output)
		} else {
			t.Logf("PASS: Expected %s, got %s", expects[k], output)
		}
	}
}

func TestKBToString(t *testing.T) {
	inputs := []int64{1024, 51200, 128000, 5632, 5376}
	expects := []string{"1.00 KB", "50.00 KB", "125.00 KB", "5.50 KB", "5.25 KB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		if output != expects[k] {
			t.Errorf("FAIL: Expected %s, got %s", expects[k], output)
		} else {
			t.Logf("PASS: Expected %s, got %s", expects[k], output)
		}
	}
}

func TestMBToString(t *testing.T) {
	inputs := []int64{1048576, 52428800, 131072000, 5767168, 5505024}
	expects := []string{"1.00 MB", "50.00 MB", "125.00 MB", "5.50 MB", "5.25 MB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		if output != expects[k] {
			t.Errorf("FAIL: Expected %s, got %s", expects[k], output)
		} else {
			t.Logf("PASS: Expected %s, got %s", expects[k], output)
		}
	}
}

func TestGBToString(t *testing.T) {
	inputs := []int64{1073741824, 53687091200, 134217728000, 5905580032, 5637144576}
	expects := []string{"1.00 GB", "50.00 GB", "125.00 GB", "5.50 GB", "5.25 GB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		if output != expects[k] {
			t.Errorf("FAIL: Expected %s, got %s", expects[k], output)
		} else {
			t.Logf("PASS: Expected %s, got %s", expects[k], output)
		}
	}
}

func TestTBToString(t *testing.T) {
	inputs := []int64{1099511627776, 54975581388800, 137438953472000, 6047313952768, 5772436045824}
	expects := []string{"1.00 TB", "50.00 TB", "125.00 TB", "5.50 TB", "5.25 TB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		if output != expects[k] {
			t.Errorf("FAIL: Expected %s, got %s", expects[k], output)
		} else {
			t.Logf("PASS: Expected %s, got %s", expects[k], output)
		}
	}
}

func TestPBToString(t *testing.T) {
	inputs := []int64{1125899906842624, 56294995342131200, 140737488355328000, 6192449487634432, 5910974510923776}
	expects := []string{"1.00 PB", "50.00 PB", "125.00 PB", "5.50 PB", "5.25 PB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		if output != expects[k] {
			t.Errorf("FAIL: Expected %s, got %s", expects[k], output)
		} else {
			t.Logf("PASS: Expected %s, got %s", expects[k], output)
		}
	}
}

func TestEBToString(t *testing.T) {
	inputs := []int64{1152921504606846976, 6341068275337658368, 6052837899185946624}
	expects := []string{"1.00 EB", "5.50 EB", "5.25 EB"} //50 & 125 aren't present because they overflow int64

	for k, v := range inputs {
		output := byteSizeToString(v)
		if output != expects[k] {
			t.Errorf("FAIL: Expected %s, got %s", expects[k], output)
		} else {
			t.Logf("PASS: Expected %s, got %s", expects[k], output)
		}
	}
}

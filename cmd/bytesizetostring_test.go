package cmd

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBToString(t *testing.T) {
	a := assert.New(t)
	inputs := []int64{50, 100, 125}
	expects := []string{"50.00 B", "100.00 B", "125.00 B"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		a.Equal(expects[k], output)
	}
}

func TestKiBToString(t *testing.T) {
	a := assert.New(t)
	inputs := []int64{1024, 51200, 128000, 5632, 5376}
	expects := []string{"1.00 KiB", "50.00 KiB", "125.00 KiB", "5.50 KiB", "5.25 KiB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		a.Equal(expects[k], output)
	}
}

func TestMiBToString(t *testing.T) {
	a := assert.New(t)
	inputs := []int64{1048576, 52428800, 131072000, 5767168, 5505024}
	expects := []string{"1.00 MiB", "50.00 MiB", "125.00 MiB", "5.50 MiB", "5.25 MiB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		a.Equal(expects[k], output)
	}
}

func TestGiBToString(t *testing.T) {
	a := assert.New(t)
	inputs := []int64{1073741824, 53687091200, 134217728000, 5905580032, 5637144576}
	expects := []string{"1.00 GiB", "50.00 GiB", "125.00 GiB", "5.50 GiB", "5.25 GiB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		a.Equal(expects[k], output)
	}
}

func TestTiBToString(t *testing.T) {
	a := assert.New(t)
	inputs := []int64{1099511627776, 54975581388800, 137438953472000, 6047313952768, 5772436045824}
	expects := []string{"1.00 TiB", "50.00 TiB", "125.00 TiB", "5.50 TiB", "5.25 TiB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		a.Equal(expects[k], output)
	}
}

func TestPiBToString(t *testing.T) {
	a := assert.New(t)
	inputs := []int64{1125899906842624, 56294995342131200, 140737488355328000, 6192449487634432, 5910974510923776}
	expects := []string{"1.00 PiB", "50.00 PiB", "125.00 PiB", "5.50 PiB", "5.25 PiB"}

	for k, v := range inputs {
		output := byteSizeToString(v)
		a.Equal(expects[k], output)
	}
}

func TestEiBToString(t *testing.T) {
	a := assert.New(t)
	inputs := []int64{1152921504606846976, 6341068275337658368, 6052837899185946624}
	expects := []string{"1.00 EiB", "5.50 EiB", "5.25 EiB"} //50 & 125 aren't present Because they overflow int64

	for k, v := range inputs {
		output := byteSizeToString(v)
		a.Equal(expects[k], output)
	}
}
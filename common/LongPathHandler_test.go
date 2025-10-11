package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShortToLong(t *testing.T) {
	a := assert.New(t)
	if OS_PATH_SEPARATOR == `\` {
		a.Equal(`\\?\C:\`, ToExtendedPath(`c:`))
		a.Equal(`\\?\C:\`, ToExtendedPath(`c:/`))
		a.Equal(`\\?\C:\myPath`, ToExtendedPath(`c:/myPath`))
		a.Equal(`\\?\C:\myPath`, ToExtendedPath(`C:\myPath`))
		a.Equal(`\\?\UNC\myHost\myPath`, ToExtendedPath(`\\myHost\myPath`))
		a.Equal(`\\?\C:\myPath`, ToExtendedPath(`\\?\C:\myPath`))
		a.Equal(`\\?\UNC\myHost\myPath`, ToExtendedPath(`\\?\UNC\myHost\myPath`))
	} else {
		t.Skip("Test only pertains to Windows.")
	}
}

func TestLongToShort(t *testing.T) {
	a := assert.New(t)
	if OS_PATH_SEPARATOR == `\` {
		a.Equal(`C:\myPath`, ToShortPath(`\\?\C:\myPath`))
		a.Equal(`\\myHost\myPath`, ToShortPath(`\\?\UNC\myHost\myPath`))
		a.Equal(`\\myHost\myPath`, ToShortPath(`\\myHost\myPath`))
		a.Equal(`C:\myPath`, ToShortPath(`C:\myPath`))
	} else {
		t.Skip("Test only pertains to Windows.")
	}
}

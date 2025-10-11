//go:build windows

package ste

import (
	"syscall"

	"golang.org/x/sys/windows"
)

func init() {
	platformRetriedErrnos = []syscall.Errno{
		windows.WSAECONNRESET,    // It's entirely possible something in between closed our connection.
		windows.WSAECONNREFUSED,  // Ditto
		windows.WSAECONNABORTED,  // Potential variant of CONNREFUSED
		windows.WSAEADDRINUSE,    // This can sometimes happen if we're trying too many connections. It usually resolves itself quickly.
		windows.WSAEADDRNOTAVAIL, // This can sometimes happen if we're trying too many connections. It usually resolves itself quickly.
		windows.WSAETIMEDOUT,     // It's possible something in between might time us out too, in which case windows will respond WSAETIMEDOUT
	}
}

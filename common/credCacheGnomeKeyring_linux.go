// Note: This implementation is initially forked from https://github.com/tmc/keyring.
// And Azcopy customized the code for its own usage.

package common

/*
#cgo pkg-config: libsecret-1 glib-2.0
#include <stdlib.h>
#include "libsecret/secret.h"

SecretSchema keyring_schema =
  {
    "org.freedesktop.Secret.Generic",
    SECRET_SCHEMA_NONE,
    {
      { "service", SECRET_SCHEMA_ATTRIBUTE_STRING },
      { "account",  SECRET_SCHEMA_ATTRIBUTE_STRING },
      {  NULL, 0 },
    }
  };

// wrap the gnome calls because cgo can't deal with vararg functions
gchar * gkr_get_password(gchar *service, gchar *account, GError **err) {
	return secret_password_lookup_sync(
		&keyring_schema,
    	NULL, // Using gnome "default" keyring
		err,
		"service", service,
		"account", account,
		NULL);
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

type gnomeKeyring struct{}

func (p gnomeKeyring) Get(Service string, Account string) (string, error) {
	var gerr *C.GError
	var pw *C.gchar

	username := (*C.gchar)(C.CString(Account))
	service := (*C.gchar)(C.CString(Service))
	defer C.free(unsafe.Pointer(username))
	defer C.free(unsafe.Pointer(service))

	pw = C.gkr_get_password(service, username, &gerr)
	defer C.free(unsafe.Pointer(gerr))
	defer C.secret_password_free((*C.gchar)(pw))

	if pw == nil {
		return "", fmt.Errorf("GnomeKeyring failed to lookup: %+v", gerr)
	}
	return C.GoString((*C.char)(pw)), nil
}

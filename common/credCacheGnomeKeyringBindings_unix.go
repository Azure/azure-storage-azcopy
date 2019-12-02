// +build freebsd se_integration

// Copyright © 2020 Conrad Meyer <cem@FreeBSD.org>
// Copyright © 2018 Microsoft <wastore@microsoft.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// Note: This implementation is initially forked from https://github.com/tmc/keyring.
// And Azcopy customized the code for its own usage.

package common

/*
#cgo pkg-config: libsecret-1 glib-2.0
#include <stdbool.h>
#include <stdlib.h>
#include "libsecret/secret.h"

static SecretSchema keyring_schema =
  {
    "org.freedesktop.Secret.Generic",
    SECRET_SCHEMA_NONE,
    {
      { "service", SECRET_SCHEMA_ATTRIBUTE_STRING },
      { "account",  SECRET_SCHEMA_ATTRIBUTE_STRING },
      {  NULL, 0 },
    }
  };

// Wrap the gnome calls because cgo can't deal with vararg functions.

// It would be nice to use the binary-safe libsecret interface ("_binary"
// family of functions and SecretValue objects), but those are new in the
// 0.19 branch of libsecret and not widely available.  For now, azcopy only
// uses non-nul character strings, so it is not a strict requirement.

static gchar * gkr_get_password(const gchar *service, const gchar *account, GError **err) {
	return secret_password_lookup_sync(&keyring_schema, NULL, err,
		"service", service,
		"account", account,
		NULL);
}

// N.B., an existing value with the same service:account key is overwritten
// without error.
static bool gkr_save_password(const gchar *service, const gchar *account, const char *pw, GError **err) {
	return secret_password_store_sync(&keyring_schema, SECRET_COLLECTION_DEFAULT,
		"Azure Copy Secret", pw, NULL, err,
		"service", service,
		"account", account,
		NULL);
}

static bool gkr_remove_password(const gchar *service, const gchar *account, GError **err) {
	return secret_password_clear_sync(&keyring_schema, NULL, err,
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

func freeMaybeNullGError(err *C.GError) {
	if err != nil {
		C.g_error_free(err)
	}
}

func (p gnomeKeyring) Get(service, account string) (string, error) {
	var gErr *C.GError
	var pw *C.gchar

	cStrService := (*C.gchar)(C.CString(service))
	cStrAccount := (*C.gchar)(C.CString(account))
	defer C.free(unsafe.Pointer(cStrService))
	defer C.free(unsafe.Pointer(cStrAccount))

	pw = C.gkr_get_password(cStrService, cStrAccount, &gErr)
	defer freeMaybeNullGError(gErr)
	defer C.secret_password_free((*C.gchar)(pw))

	if pw == nil {
		return "", fmt.Errorf("GnomeKeyring failed to lookup: %+v", gErr)
	}
	return C.GoString((*C.char)(pw)), nil
}

func (p gnomeKeyring) Save(service, account string, secret string) error {
	var gErr *C.GError

	cStrService := (*C.gchar)(C.CString(service))
	cStrAccount := (*C.gchar)(C.CString(account))
	defer C.free(unsafe.Pointer(cStrService))
	defer C.free(unsafe.Pointer(cStrAccount))

	cStrSecret := (*C.gchar)(C.CString(secret))
	defer C.secret_password_free(cStrSecret)

	ok := C.gkr_save_password(cStrService, cStrAccount, cStrSecret, &gErr)
	defer freeMaybeNullGError(gErr)

	if !ok {
		return fmt.Errorf("GnomeKeyring failed to store: %+v", gErr)
	}
	return nil
}

func (p gnomeKeyring) Remove(service, account string) error {
	var gErr *C.GError

	cStrService := (*C.gchar)(C.CString(service))
	cStrAccount := (*C.gchar)(C.CString(account))
	defer C.free(unsafe.Pointer(cStrService))
	defer C.free(unsafe.Pointer(cStrAccount))

	ok := C.gkr_remove_password(cStrService, cStrAccount, &gErr)
	defer freeMaybeNullGError(gErr)

	if !ok {
		return fmt.Errorf("GnomeKeyring failed to remove: %+v", gErr)
	}
	return nil
}

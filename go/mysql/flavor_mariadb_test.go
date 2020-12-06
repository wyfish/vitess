/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mysql

import (
	"testing"
)

func TestMariadbSetMainCommands(t *testing.T) {
	params := &ConnParams{
		Uname: "username",
		Pass:  "password",
	}
	mainHost := "localhost"
	mainPort := 123
	mainConnectRetry := 1234
	want := `CHANGE MASTER TO
  MASTER_HOST = 'localhost',
  MASTER_PORT = 123,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 1234,
  MASTER_USE_GTID = current_pos`

	conn := &Conn{flavor: mariadbFlavor{}}
	got := conn.SetMainCommand(params, mainHost, mainPort, mainConnectRetry)
	if got != want {
		t.Errorf("mariadbFlavor.SetMainCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, mainHost, mainPort, mainConnectRetry, got, want)
	}
}

func TestMariadbSetMainCommandsSSL(t *testing.T) {
	params := &ConnParams{
		Uname:     "username",
		Pass:      "password",
		SslCa:     "ssl-ca",
		SslCaPath: "ssl-ca-path",
		SslCert:   "ssl-cert",
		SslKey:    "ssl-key",
	}
	params.EnableSSL()
	mainHost := "localhost"
	mainPort := 123
	mainConnectRetry := 1234
	want := `CHANGE MASTER TO
  MASTER_HOST = 'localhost',
  MASTER_PORT = 123,
  MASTER_USER = 'username',
  MASTER_PASSWORD = 'password',
  MASTER_CONNECT_RETRY = 1234,
  MASTER_SSL = 1,
  MASTER_SSL_CA = 'ssl-ca',
  MASTER_SSL_CAPATH = 'ssl-ca-path',
  MASTER_SSL_CERT = 'ssl-cert',
  MASTER_SSL_KEY = 'ssl-key',
  MASTER_USE_GTID = current_pos`

	conn := &Conn{flavor: mariadbFlavor{}}
	got := conn.SetMainCommand(params, mainHost, mainPort, mainConnectRetry)
	if got != want {
		t.Errorf("mariadbFlavor.SetMainCommands(%#v, %#v, %#v, %#v) = %#v, want %#v", params, mainHost, mainPort, mainConnectRetry, got, want)
	}
}

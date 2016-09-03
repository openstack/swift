//  Copyright (c) 2015 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package proxyserver

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/openstack/swift/go/hummingbird"
)

type testUser struct {
	Account  string
	Username string
	Password string
	Roles    []string
	Url      string
}

type TempAuth struct {
	mc        hummingbird.MemcacheRing
	testUsers []testUser
	next      http.Handler
}

func NewTempAuth(mc hummingbird.MemcacheRing, config hummingbird.Config) func(http.Handler) http.Handler {
	var users []testUser
	// Hardcoding "tempauth" is incorrect. The actual name of the section
	// is determined by the entry in the pipeline= in [pipeline:main]. XXX
	section := "filter:tempauth"
	if config.HasSection(section) {
		users = []testUser{}
		for key, val := range config.File[section] {
			keyparts := strings.Split(key, "_")
			valparts := strings.Fields(val)
			if len(keyparts) != 3 || keyparts[0] != "user" {
				// egg specification and other configuration
				continue
			}
			vallen := len(valparts)
			if vallen < 1 {
				continue
			}
			url := ""
			groups := []string{}
			if vallen > 1 {
				urlSpot := 0
				s := valparts[vallen-1]
				if strings.HasPrefix(s, "http://") || strings.HasPrefix(s, "https://") {
					urlSpot = 1
					url = s
				}
				for _, group := range valparts[1:vallen-urlSpot] {
					groups = append(groups, group)
				}
			}
			user := testUser{keyparts[1], keyparts[2], valparts[0], groups, url}
			users = append(users, user)
		}
	} else {
		// Swift would traceback in case of a missing section, but
		// traditional Hummingbird uses a hardcoded tempauth. Always.
		// [XXX isn't it a very bad idea in a public cloud?]
		users = []testUser{
			{"admin", "admin", "admin", []string{".admin", ".reseller_admin"}, ""},
			{"test", "tester", "testing", []string{".admin"}, ""},
			{"test2", "tester2", "testing2", []string{".admin"}, ""},
			{"test2", "tester2", "testing2", []string{".admin"}, ""},
			{"test", "tester3", "testing3", []string{}, ""},
		}
	}
	tempAuth := &TempAuth{
		mc: mc,
		testUsers: users,
	}
	return tempAuth.getMiddleware
}

func (ta *TempAuth) getMiddleware(next http.Handler) http.Handler {
	ta.next = next
	return ta
}

func (ta *TempAuth) login(account, user, key string) (string, string, error) {
	for _, tu := range ta.testUsers {
		if tu.Account == account && tu.Username == user && tu.Password == key {
			token := hummingbird.UUID()
			ta.mc.Set(token, true, 3600)
			return token, tu.Url, nil
		}
	}
	return "", "", errors.New("User not found.")
}

func (ta *TempAuth) createAccount(account string) bool {
	req, err := http.NewRequest("PUT", "/v1/AUTH_"+account, nil)
	if err == nil {
		writer := httptest.NewRecorder()
		ta.next.ServeHTTP(writer, req)
		return writer.Code/100 == 2
	}
	return false
}

func (ta *TempAuth) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	if request.URL.Path == "/auth/v1.0" {
		token := hummingbird.UUID()
		user := request.Header.Get("X-Auth-User")
		parts := strings.Split(user, ":")
		if len(parts) != 2 {
			hummingbird.StandardResponse(writer, 400)
			return
		}
		account := parts[0]
		user = parts[1]
		password := request.Header.Get("X-Auth-Key")
		token, url, err := ta.login(account, user, password)
		if err != nil {
			hummingbird.StandardResponse(writer, 401)
			return
		}
		writer.Header().Set("X-Storage-Token", token)
		writer.Header().Set("X-Auth-Token", token)
		if url != "" {
			writer.Header().Set("X-Storage-URL", url)
		} else {
			writer.Header().Set("X-Storage-URL", fmt.Sprintf("http://%s/v1/AUTH_%s", request.Host, account))
		}
		hummingbird.StandardResponse(writer, 200)
	} else {
		token := request.Header.Get("X-Auth-Token")
		valid, err := ta.mc.Get(token)
		if err != nil {
			hummingbird.StandardResponse(writer, 401)
			return
		}
		v, ok := valid.(bool)
		if !ok {
			v = false
		}
		ctx := GetProxyContext(request)
		if ctx != nil {
			ctx.Authorize = func(r *http.Request) bool {
				return v
			}
		}
		ta.next.ServeHTTP(writer, request)
	}
}

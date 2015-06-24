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

package hummingbird

import (
	"net/http"

	"github.com/gorilla/context"
)

type KeyType int

const logkey KeyType = iota

func GetLogger(r *http.Request) LoggingContext {
	if rv := context.Get(r, logkey); rv != nil {
		return rv.(LoggingContext)
	}
	return nil
}

func SetLogger(r *http.Request, val LoggingContext) {
	context.Set(r, logkey, val)
}

func GetVars(r *http.Request) map[string]string {
	if rv := context.Get(r, "vars"); rv != nil {
		return rv.(map[string]string)
	}
	return nil
}

func SetVars(r *http.Request, val map[string]string) {
	context.Set(r, "vars", val)
}

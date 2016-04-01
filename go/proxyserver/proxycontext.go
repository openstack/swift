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
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/gorilla/context"
	"github.com/openstack/swift/go/client"
	"github.com/openstack/swift/go/hummingbird"
)

type AccountInfo struct {
	ContainerCount int64
	ObjectCount    int64
	ObjectBytes    int64
	Metadata       map[string]string
	SysMetadata    map[string]string
}

type ContainerInfo struct {
	ObjectCount int64
	ObjectBytes int64
	Metadata    map[string]string
	SysMetadata map[string]string
}

type AuthorizeFunc func(r *http.Request) bool

type ProxyContextMiddleware struct {
	next http.Handler
	mc   hummingbird.MemcacheRing
	c    client.ProxyClient
}

type ProxyContext struct {
	*ProxyContextMiddleware
	Authorize          AuthorizeFunc
	containerInfoCache map[string]*ContainerInfo
	accountInfoCache   map[string]*AccountInfo
}

func (ctx *ProxyContext) GetContainerInfo(account, container string) *ContainerInfo {
	var err error
	key := fmt.Sprintf("container/%s", container)
	ci := ctx.containerInfoCache[key]
	if ci == nil {
		if err := ctx.mc.GetStructured(key, &ci); err != nil {
			ci = nil
		}
	}
	if ci == nil {
		headers, code := ctx.c.HeadContainer(account, container, nil)
		if code/100 != 2 {
			return nil
		}
		ci = &ContainerInfo{
			Metadata:    make(map[string]string),
			SysMetadata: make(map[string]string),
		}
		if ci.ObjectCount, err = strconv.ParseInt(headers.Get("X-Container-Object-Count"), 10, 64); err != nil {
			return nil
		}
		if ci.ObjectBytes, err = strconv.ParseInt(headers.Get("X-Container-Bytes-Used"), 10, 64); err != nil {
			return nil
		}
		for k := range headers {
			if strings.HasPrefix(k, "X-Container-Meta-") {
				ci.Metadata[k[17:]] = headers.Get(k)
			} else if strings.HasPrefix(k, "X-Container-Sysmeta-") {
				ci.SysMetadata[k[20:]] = headers.Get(k)
			}
		}
		ctx.mc.Set(key, ci, 30)
	}
	return ci
}

func (ctx *ProxyContext) GetAccountInfo(account string) *AccountInfo {
	var err error
	key := fmt.Sprintf("account/%s", account)
	ai := ctx.accountInfoCache[key]
	if ai == nil {
		if err := ctx.mc.GetStructured(key, &ai); err != nil {
			ai = nil
		}
	}
	if ai == nil {
		headers, code := ctx.c.HeadAccount(account, nil)
		if code/100 != 2 {
			return nil
		}
		ai = &AccountInfo{
			Metadata:    make(map[string]string),
			SysMetadata: make(map[string]string),
		}
		if ai.ContainerCount, err = strconv.ParseInt(headers.Get("X-Account-Container-Count"), 10, 64); err != nil {
			return nil
		}
		if ai.ObjectCount, err = strconv.ParseInt(headers.Get("X-Account-Object-Count"), 10, 64); err != nil {
			return nil
		}
		if ai.ObjectBytes, err = strconv.ParseInt(headers.Get("X-Account-Bytes-Used"), 10, 64); err != nil {
			return nil
		}
		for k := range headers {
			if strings.HasPrefix(k, "X-Account-Meta-") {
				ai.Metadata[k[15:]] = headers.Get(k)
			} else if strings.HasPrefix(k, "X-Account-Sysmeta-") {
				ai.SysMetadata[k[18:]] = headers.Get(k)
			}
		}
		ctx.mc.Set(key, ai, 30)
	}
	return ai
}

func (m *ProxyContextMiddleware) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := &ProxyContext{
		ProxyContextMiddleware: m,
		Authorize:              nil,
		containerInfoCache:     make(map[string]*ContainerInfo),
		accountInfoCache:       make(map[string]*AccountInfo),
	}
	wg := &sync.WaitGroup{}
	// we'll almost certainly need the AccountInfo and ContainerInfo for the current path, so pre-fetch them in parallel.
	account, container, _ := getPathParts(request)
	if account != "" {
		wg.Add(1)
		go func() {
			if ctx.GetAccountInfo(account) == nil {
				hdr := http.Header{
					"X-Timestamp": []string{hummingbird.GetTimestamp()},
				}
				m.c.PutAccount(account, hdr) // Auto-create the account if we can't get its info
			}
			wg.Done()
		}()
		if container != "" {
			wg.Add(1)
			go func() {
				ctx.GetContainerInfo(account, container)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	context.Set(request, "proxycontext", ctx)
	m.next.ServeHTTP(writer, request)
}

func (m *ProxyContextMiddleware) getMiddleware(next http.Handler) http.Handler {
	m.next = next
	return m
}

func NewProxyContextMiddleware(mc hummingbird.MemcacheRing, c client.ProxyClient) func(http.Handler) http.Handler {
	m := &ProxyContextMiddleware{
		mc: mc,
		c:  c,
	}
	return m.getMiddleware
}

func GetProxyContext(r *http.Request) *ProxyContext {
	if rv := context.Get(r, "proxycontext"); rv != nil {
		return rv.(*ProxyContext)
	}
	return nil
}

func getPathParts(request *http.Request) (string, string, string) {
	parts := strings.SplitN(request.URL.Path, "/", 5)
	if len(parts) == 5 {
		return parts[2], parts[3], parts[4]
	} else if len(parts) == 4 {
		return parts[2], parts[3], ""
	} else if len(parts) == 3 {
		return parts[2], "", ""
	}
	return "", "", ""
}

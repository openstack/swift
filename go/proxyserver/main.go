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
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/middleware"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/justinas/alice"
)

type ProxyServer struct {
	objectRing    hummingbird.Ring
	containerRing hummingbird.Ring
	accountRing   hummingbird.Ring
	client        *http.Client
	logger        hummingbird.SysLogLike
	mc            *memcache.Client
}

func (server *ProxyServer) LogRequest(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		requestLogger := &hummingbird.RequestLogger{Request: request, Logger: server.logger}
		newWriter := &hummingbird.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
		defer requestLogger.LogPanics(newWriter)
		start := time.Now()
		hummingbird.SetLogger(request, requestLogger)
		request.Header.Set("X-Trans-Id", hummingbird.GetTransactionId())
		request.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
		next.ServeHTTP(newWriter, request)
		server.logger.Info(fmt.Sprintf("%s - - [%s] \"%s %s\" %d %s \"%s\" \"%s\" \"%s\" %.4f \"%s\"",
			request.RemoteAddr,
			time.Now().Format("02/Jan/2006:15:04:05 -0700"),
			request.Method,
			hummingbird.Urlencode(request.URL.Path),
			newWriter.Status,
			hummingbird.GetDefault(newWriter.Header(), "Content-Length", "-"),
			hummingbird.GetDefault(request.Header, "Referer", "-"),
			hummingbird.GetDefault(request.Header, "X-Trans-Id", "-"),
			hummingbird.GetDefault(request.Header, "User-Agent", "-"),
			time.Since(start).Seconds(),
			"-")) // TODO: "additional info"?
	}
	return http.HandlerFunc(fn)
}

func (server *ProxyServer) GetHandler() http.Handler {
	commonHandlers := alice.New(middleware.ClearHandler, server.LogRequest, middleware.ValidateRequest)
	router := hummingbird.NewRouter()
	router.Get("/healthcheck", commonHandlers.ThenFunc(server.HealthcheckHandler))
	router.Get("/auth/v1.0", commonHandlers.ThenFunc(server.AuthHandler))
	router.Get("/v1/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjectGetHeadHandler))
	router.Head("/v1/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjectGetHeadHandler))
	router.Put("/v1/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjectPutHandler))
	router.Delete("/v1/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjectDeleteHandler))

	router.Get("/v1/:account/:container/", commonHandlers.ThenFunc(server.ContainerGetHeadHandler))
	router.Head("/v1/:account/:container/", commonHandlers.ThenFunc(server.ContainerGetHeadHandler))
	router.Put("/v1/:account/:container/", commonHandlers.ThenFunc(server.ContainerPutHandler))
	router.Delete("/v1/:account/:container/", commonHandlers.ThenFunc(server.ContainerDeleteHandler))
	router.Post("/v1/:account/:container/", commonHandlers.ThenFunc(server.ContainerPutHandler))

	router.Get("/v1/:account/:container", commonHandlers.ThenFunc(server.ContainerGetHeadHandler))
	router.Head("/v1/:account/:container", commonHandlers.ThenFunc(server.ContainerGetHeadHandler))
	router.Put("/v1/:account/:container", commonHandlers.ThenFunc(server.ContainerPutHandler))
	router.Delete("/v1/:account/:container", commonHandlers.ThenFunc(server.ContainerDeleteHandler))
	router.Post("/v1/:account/:container", commonHandlers.ThenFunc(server.ContainerPutHandler))

	router.Get("/v1/:account/", commonHandlers.ThenFunc(server.AccountGetHeadHandler))
	router.Head("/v1/:account/", commonHandlers.ThenFunc(server.AccountGetHeadHandler))
	router.Put("/v1/:account/", commonHandlers.ThenFunc(server.AccountPutHandler))
	router.Delete("/v1/:account/", commonHandlers.ThenFunc(server.AccountDeleteHandler))
	router.Post("/v1/:account/", commonHandlers.ThenFunc(server.AccountPutHandler))

	router.Get("/v1/:account", commonHandlers.ThenFunc(server.AccountGetHeadHandler))
	router.Head("/v1/:account", commonHandlers.ThenFunc(server.AccountGetHeadHandler))
	router.Put("/v1/:account", commonHandlers.ThenFunc(server.AccountPutHandler))
	router.Delete("/v1/:account", commonHandlers.ThenFunc(server.AccountDeleteHandler))
	router.Post("/v1/:account", commonHandlers.ThenFunc(server.AccountPutHandler))

	return router
}

func GetServer(conf string, flags *flag.FlagSet) (string, int, hummingbird.Server, hummingbird.SysLogLike, error) {
	server := &ProxyServer{}
	server.client = GetClient()
	server.mc = memcache.New("127.0.0.1:11211")
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return "", 0, nil, nil, err
	}
	serverconf, err := hummingbird.LoadIniFile(conf)
	if err != nil {
		return "", 0, nil, nil, errors.New(fmt.Sprintf("Unable to load %s", conf))
	}

	bindIP := serverconf.GetDefault("DEFAULT", "bind_ip", "0.0.0.0")
	bindPort := serverconf.GetInt("DEFAULT", "bind_port", 8080)
	server.logger = hummingbird.SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "proxy-server", "")

	//Getting all rings
	server.objectRing, err = hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix)
	server.containerRing, err = hummingbird.GetRing("container", hashPathPrefix, hashPathSuffix)
	server.accountRing, err = hummingbird.GetRing("account", hashPathPrefix, hashPathSuffix)
	if err != nil {
		return "", 0, nil, nil, err
	}

	return bindIP, int(bindPort), server, server.logger, nil
}

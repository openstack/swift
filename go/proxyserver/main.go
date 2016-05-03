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
	"flag"
	"fmt"
	"net/http"
	"time"

	"github.com/openstack/swift/go/client"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/middleware"

	"github.com/justinas/alice"
)

type ProxyServer struct {
	C      client.ProxyClient
	logger hummingbird.SysLogLike
	mc     hummingbird.MemcacheRing
}

func (server *ProxyServer) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
	return
}

func (server *ProxyServer) LogRequest(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		newWriter := &hummingbird.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
		requestLogger := &hummingbird.RequestLogger{Request: request, Logger: server.logger, W: newWriter}
		defer requestLogger.LogPanics("LOGGING REQUEST")
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

func (server *ProxyServer) GetHandler(config hummingbird.Config) http.Handler {
	router := hummingbird.NewRouter()
	router.Get("/healthcheck", http.HandlerFunc(server.HealthcheckHandler))

	router.Get("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectGetHandler))
	router.Head("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectHeadHandler))
	router.Put("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectPutHandler))
	router.Delete("/v1/:account/:container/*obj", http.HandlerFunc(server.ObjectDeleteHandler))

	router.Get("/v1/:account/:container", http.HandlerFunc(server.ContainerGetHandler))
	router.Get("/v1/:account/:container/", http.HandlerFunc(server.ContainerGetHandler))
	router.Head("/v1/:account/:container", http.HandlerFunc(server.ContainerHeadHandler))
	router.Head("/v1/:account/:container/", http.HandlerFunc(server.ContainerHeadHandler))
	router.Put("/v1/:account/:container", http.HandlerFunc(server.ContainerPutHandler))
	router.Put("/v1/:account/:container/", http.HandlerFunc(server.ContainerPutHandler))
	router.Delete("/v1/:account/:container", http.HandlerFunc(server.ContainerDeleteHandler))
	router.Delete("/v1/:account/:container/", http.HandlerFunc(server.ContainerDeleteHandler))
	router.Post("/v1/:account/:container", http.HandlerFunc(server.ContainerPutHandler))
	router.Post("/v1/:account/:container/", http.HandlerFunc(server.ContainerPutHandler))

	router.Get("/v1/:account", http.HandlerFunc(server.AccountGetHandler))
	router.Get("/v1/:account/", http.HandlerFunc(server.AccountGetHandler))
	router.Head("/v1/:account", http.HandlerFunc(server.AccountHeadHandler))
	router.Head("/v1/:account/", http.HandlerFunc(server.AccountHeadHandler))
	router.Put("/v1/:account", http.HandlerFunc(server.AccountPutHandler))
	router.Put("/v1/:account/", http.HandlerFunc(server.AccountPutHandler))
	router.Delete("/v1/:account", http.HandlerFunc(server.AccountDeleteHandler))
	router.Delete("/v1/:account/", http.HandlerFunc(server.AccountDeleteHandler))
	router.Post("/v1/:account", http.HandlerFunc(server.AccountPutHandler))
	router.Post("/v1/:account/", http.HandlerFunc(server.AccountPutHandler))

	return alice.New(
		middleware.ClearHandler,
		server.LogRequest,
		middleware.ValidateRequest,
		NewProxyContextMiddleware(server.mc, server.C),
		NewTempAuth(server.mc),
	).Then(router)
}

func GetServer(serverconf hummingbird.Config, flags *flag.FlagSet) (string, int, hummingbird.Server, hummingbird.SysLogLike, error) {
	var err error
	server := &ProxyServer{}
	server.C, err = client.NewProxyDirectClient()
	if err != nil {
		return "", 0, nil, nil, err
	}
	server.mc, err = hummingbird.NewMemcacheRingFromConfig(serverconf)
	if err != nil {
		return "", 0, nil, nil, err
	}

	bindIP := serverconf.GetDefault("DEFAULT", "bind_ip", "0.0.0.0")
	bindPort := serverconf.GetInt("DEFAULT", "bind_port", 8080)
	server.logger = hummingbird.SetupLogger(serverconf.GetDefault("DEFAULT", "log_facility", "LOG_LOCAL0"), "proxy-server", "")

	return bindIP, int(bindPort), server, server.logger, nil
}

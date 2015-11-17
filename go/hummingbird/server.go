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
	"bufio"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

var responseTemplate = "<html><h1>%s</h1><p>%s</p></html>"

var responseBodies = map[int]string{
	100: "",
	200: "",
	201: "",
	202: fmt.Sprintf(responseTemplate, "Accepted", "The request is accepted for processing."),
	204: "",
	206: "",
	301: fmt.Sprintf(responseTemplate, "Moved Permanently", "The resource has moved permanently."),
	302: fmt.Sprintf(responseTemplate, "Found", "The resource has moved temporarily."),
	303: fmt.Sprintf(responseTemplate, "See Other", "The response to the request can be found under a different URI."),
	304: "",
	307: fmt.Sprintf(responseTemplate, "Temporary Redirect", "The resource has moved temporarily."),
	400: fmt.Sprintf(responseTemplate, "Bad Request", "The server could not comply with the request since it is either malformed or otherwise incorrect."),
	401: fmt.Sprintf(responseTemplate, "Unauthorized", "This server could not verify that you are authorized to access the document you requested."),
	402: fmt.Sprintf(responseTemplate, "Payment Required", "Access was denied for financial reasons."),
	403: fmt.Sprintf(responseTemplate, "Forbidden", "Access was denied to this resource."),
	404: fmt.Sprintf(responseTemplate, "Not Found", "The resource could not be found."),
	405: fmt.Sprintf(responseTemplate, "Method Not Allowed", "The method is not allowed for this resource."),
	406: fmt.Sprintf(responseTemplate, "Not Acceptable", "The resource is not available in a format acceptable to your browser."),
	408: fmt.Sprintf(responseTemplate, "Request Timeout", "The server has waited too long for the request to be sent by the client."),
	409: fmt.Sprintf(responseTemplate, "Conflict", "There was a conflict when trying to complete your request."),
	410: fmt.Sprintf(responseTemplate, "Gone", "This resource is no longer available."),
	411: fmt.Sprintf(responseTemplate, "Length Required", "Content-Length header required."),
	412: "",
	413: fmt.Sprintf(responseTemplate, "Request Entity Too Large", "The body of your request was too large for this server."),
	414: fmt.Sprintf(responseTemplate, "Request URI Too Long", "The request URI was too long for this server."),
	415: fmt.Sprintf(responseTemplate, "Unsupported Media Type", "The request media type is not supported by this server."),
	416: fmt.Sprintf(responseTemplate, "Requested Range Not Satisfiable", "The Range requested is not available."),
	417: fmt.Sprintf(responseTemplate, "Expectation Failed", "Expectation failed."),
	422: fmt.Sprintf(responseTemplate, "Unprocessable Entity", "Unable to process the contained instructions"),
	499: fmt.Sprintf(responseTemplate, "Client Disconnect", "The client was disconnected during request."),
	500: fmt.Sprintf(responseTemplate, "Internal Error", "The server has either erred or is incapable of performing the requested operation."),
	501: fmt.Sprintf(responseTemplate, "Not Implemented", "The requested method is not implemented by this server."),
	502: fmt.Sprintf(responseTemplate, "Bad Gateway", "Bad gateway."),
	503: fmt.Sprintf(responseTemplate, "Service Unavailable", "The server is currently unavailable. Please try again at a later time."),
	504: fmt.Sprintf(responseTemplate, "Gateway Timeout", "A timeout has occurred speaking to a backend server."),
}

// ResponseWriter that saves its status - used for logging.

type WebWriter struct {
	http.ResponseWriter
	Status          int
	ResponseStarted bool
}

func (w *WebWriter) WriteHeader(status int) {
	w.ResponseWriter.WriteHeader(status)
	w.Status = status
	w.ResponseStarted = true
}

func (w WebWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return w.ResponseWriter.(http.Hijacker).Hijack()
}

func CopyResponseHeaders(w http.ResponseWriter, src *http.Response) {
	for key := range src.Header {
		w.Header().Set(key, src.Header.Get(key))
	}
}

func StandardResponse(w http.ResponseWriter, statusCode int) {
	body := responseBodies[statusCode]
	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(body)), 10))
	w.WriteHeader(statusCode)
	w.Write([]byte(body))
}

func CustomErrorResponse(w http.ResponseWriter, statusCode int, vars map[string]string) {
	body := ""
	switch statusCode {
	case 507:
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		if vars["Method"] != "HEAD" {
			body = fmt.Sprintf("<html><h1>Insufficient Storage</h1><p>There was not enough space to save the resource. Drive: %s</p></html>", vars["device"])
		}
	}
	w.Header().Set("Content-Length", strconv.FormatInt(int64(len(body)), 10))
	w.WriteHeader(statusCode)
	w.Write([]byte(body))
}

func CopyRequestHeaders(r *http.Request, dst *http.Request) {
	for key := range r.Header {
		dst.Header.Set(key, r.Header.Get(key))
	}
}

type RequestLogger struct {
	Request *http.Request
	Logger  SysLogLike
}

func (r RequestLogger) LogError(format string, args ...interface{}) {
	transactionId := r.Request.Header.Get("X-Trans-Id")
	r.Logger.Err(fmt.Sprintf(format, args...) + " (txn:" + transactionId + ")")
}

func (r RequestLogger) LogInfo(format string, args ...interface{}) {
	transactionId := r.Request.Header.Get("X-Trans-Id")
	r.Logger.Info(fmt.Sprintf(format, args...) + " (txn:" + transactionId + ")")
}

func (r RequestLogger) LogDebug(format string, args ...interface{}) {
	transactionId := r.Request.Header.Get("X-Trans-Id")
	r.Logger.Debug(fmt.Sprintf(format, args...) + " (txn:" + transactionId + ")")
}

func (r RequestLogger) LogPanics(w *WebWriter) {
	if e := recover(); e != nil {
		transactionId := r.Request.Header.Get("X-Trans-Id")
		r.Logger.Err(fmt.Sprintf("PANIC: %s: %s", e, debug.Stack()) + " (txn:" + transactionId + ")")
		// if we haven't set a status code yet, we can send a 500 response.
		if !w.ResponseStarted {
			StandardResponse(w, http.StatusInternalServerError)
		}
	}
}

func ValidateRequest(r *http.Request) bool {
	return utf8.ValidString(r.URL.Path) && utf8.ValidString(r.Header.Get("Content-Type"))
}

type LoggingContext interface {
	LogError(format string, args ...interface{})
	LogInfo(format string, args ...interface{})
	LogDebug(format string, args ...interface{})
}

type SysLogLike interface {
	Err(string) error
	Info(string) error
	Debug(string) error
}

/* http.Server that knows how to shut down gracefully */

type HummingbirdServer struct {
	http.Server
	Listener net.Listener
	wg       sync.WaitGroup
}

func (srv *HummingbirdServer) ConnStateChange(conn net.Conn, state http.ConnState) {
	if state == http.StateNew {
		srv.wg.Add(1)
	} else if state == http.StateClosed {
		srv.wg.Done()
	}
}

func (srv *HummingbirdServer) BeginShutdown() {
	srv.SetKeepAlivesEnabled(false)
	srv.Listener.Close()
}

func (srv *HummingbirdServer) Wait() {
	srv.wg.Wait()
}

func ShutdownStdio() {
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0600)
	if err != nil {
		panic("Error opening /dev/null")
	}
	syscall.Dup2(int(devnull.Fd()), int(os.Stdin.Fd()))
	syscall.Dup2(int(devnull.Fd()), int(os.Stdout.Fd()))
	syscall.Dup2(int(devnull.Fd()), int(os.Stderr.Fd()))
	devnull.Close()
}

func RetryListen(ip string, port int) (net.Listener, error) {
	address := fmt.Sprintf("%s:%d", ip, port)
	started := time.Now()
	for {
		if sock, err := net.Listen("tcp", address); err == nil {
			return sock, nil
		} else if time.Now().Sub(started) > 10*time.Second {
			return nil, errors.New(fmt.Sprintf("Failed to bind for 10 seconds (%v)", err))
		}
		time.Sleep(time.Second / 5)
	}
}

type Server interface {
	GetHandler() http.Handler
}

/*
	SIGHUP - graceful restart
	SIGINT - graceful shutdown
	SIGTERM, SIGQUIT - immediate shutdown

	Graceful shutdown/restart gives any open connections 5 minutes to complete, then exits.
*/
func RunServers(GetServer func(string, *flag.FlagSet) (string, int, Server, SysLogLike, error), flags *flag.FlagSet) {
	var servers []*HummingbirdServer

	if flags.NArg() != 0 {
		flags.Usage()
		return
	}
	configFile := flags.Lookup("c").Value.(flag.Getter).Get().(string)
	configFiles, err := filepath.Glob(filepath.Join(configFile, "*.conf"))
	if err != nil || len(configFiles) <= 0 {
		configFiles = []string{configFile}
	}
	for _, configFile := range configFiles {
		ip, port, server, logger, err := GetServer(configFile, flags)
		if err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		sock, err := RetryListen(ip, port)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error listening: %v\n", err)
			logger.Err(fmt.Sprintf("Error listening: %v", err))
			os.Exit(1)
		}
		srv := HummingbirdServer{
			Server: http.Server{
				Handler:      server.GetHandler(),
				ReadTimeout:  24 * time.Hour,
				WriteTimeout: 24 * time.Hour,
			},
			Listener: sock,
		}
		srv.Server.ConnState = srv.ConnStateChange
		go srv.Serve(sock)
		servers = append(servers, &srv)
		logger.Err(fmt.Sprintf("Server started on port %d", port))
		fmt.Printf("Server started on port %d\n", port)
	}

	if len(servers) > 0 {
		if flags.Lookup("d").Value.(flag.Getter).Get() == true {
			ShutdownStdio()
		}
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
		s := <-c
		if s == syscall.SIGINT {
			for _, srv := range servers {
				srv.BeginShutdown()
			}
			go func() {
				time.Sleep(time.Minute * 5)
				os.Exit(0)
			}()
			for _, srv := range servers {
				srv.Wait()
			}
			time.Sleep(time.Second * 5)
		}
	}
}

type Daemon interface {
	Run()
	RunForever()
	LogError(format string, args ...interface{})
}

func RunDaemon(GetDaemon func(string, *flag.FlagSet) (Daemon, error), flags *flag.FlagSet) {
	var daemons []Daemon

	if flags.NArg() != 0 {
		flags.Usage()
		return
	}

	configFile := flags.Lookup("c").Value.(flag.Getter).Get().(string)
	configFiles, err := filepath.Glob(filepath.Join(configFile, "*.conf"))
	if err != nil || len(configFiles) <= 0 {
		configFiles = []string{flags.Arg(0)}
	}

	once := flags.Lookup("once").Value.(flag.Getter).Get() == true

	for _, configFile := range configFiles {
		if daemon, err := GetDaemon(configFile, flags); err == nil {
			if once {
				daemon.Run()
				fmt.Fprintf(os.Stderr, "Daemon pass completed.\n")
				daemon.LogError("Daemon pass completed.")
			} else {
				daemons = append(daemons, daemon)
				go daemon.RunForever()
				fmt.Fprintf(os.Stderr, "Daemon started.\n")
				daemon.LogError("Daemon started.")
			}
		} else {
			fmt.Fprintf(os.Stderr, "Failed to create daemon: %v", err)
		}
	}

	if len(daemons) > 0 {
		if flags.Lookup("d").Value.(flag.Getter).Get() == true {
			ShutdownStdio()
		}
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGTERM, syscall.SIGINT, syscall.SIGQUIT)
		<-c
	}
}

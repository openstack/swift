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

package objectserver

import (
	"crypto/md5"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log/syslog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/textproto"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/justinas/alice"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/middleware"
)

type ObjectServer struct {
	driveRoot        string
	hashPathPrefix   string
	hashPathSuffix   string
	checkEtags       bool
	checkMounts      bool
	allowedHeaders   map[string]bool
	logger           *syslog.Writer
	logLevel         string
	diskInUse        *hummingbird.KeyedLimit
	accountDiskInUse *hummingbird.KeyedLimit
	expiringDivisor  int64
	updateClient     *http.Client
	objEngines       map[int]ObjectEngine
	updateTimeout    time.Duration
}

func (server *ObjectServer) newObject(req *http.Request, vars map[string]string, needData bool) (Object, error) {
	policy, err := strconv.Atoi(req.Header.Get("X-Backend-Storage-Policy-Index"))
	if err != nil {
		policy = 0
	}
	engine, ok := server.objEngines[policy]
	if !ok {
		return nil, fmt.Errorf("Engine for policy index %d not found.", policy)
	}
	return engine.New(vars, needData)
}

func (server *ObjectServer) ObjGetHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	headers := writer.Header()
	obj, err := server.newObject(request, vars, request.Method == "GET")
	if err != nil {
		hummingbird.GetLogger(request).LogError("Unable to open object: %v", err)
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	if !obj.Exists() {
		if im := request.Header.Get("If-Match"); im != "" && strings.Contains(im, "*") {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		} else {
			hummingbird.StandardResponse(writer, http.StatusNotFound)
			return
		}
	}

	metadata := obj.Metadata()

	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
	if deleteAt, ok := metadata["X-Delete-At"]; ok {
		if deleteTime, err := hummingbird.ParseDate(deleteAt); err == nil && deleteTime.Before(time.Now()) {
			hummingbird.StandardResponse(writer, http.StatusNotFound)
			return
		}
	}

	lastModified, err := hummingbird.ParseDate(metadata["X-Timestamp"])
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error getting timestamp from %s: %s", obj.Repr(), err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	lastModifiedHeader := lastModified
	if lastModified.Nanosecond() > 0 { // for some reason, Last-Modified is ceil(X-Timestamp)
		lastModifiedHeader = lastModified.Truncate(time.Second).Add(time.Second)
	}
	headers.Set("Last-Modified", lastModifiedHeader.Format(time.RFC1123))
	headers.Set("ETag", "\""+metadata["ETag"]+"\"")
	xTimestamp, err := hummingbird.GetEpochFromTimestamp(metadata["X-Timestamp"])
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error getting the epoch time from x-timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	headers.Set("X-Timestamp", xTimestamp)
	for key, value := range metadata {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) ||
			strings.HasPrefix(key, "X-Object-Meta-") ||
			strings.HasPrefix(key, "X-Object-Sysmeta-") {
			headers.Set(key, value)
		}
	}

	if im := request.Header.Get("If-Match"); im != "" && !strings.Contains(im, metadata["ETag"]) && !strings.Contains(im, "*") {
		hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
		return
	}

	if inm := request.Header.Get("If-None-Match"); inm != "" && (strings.Contains(inm, metadata["ETag"]) || strings.Contains(inm, "*")) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	if ius, err := hummingbird.ParseDate(request.Header.Get("If-Unmodified-Since")); err == nil && lastModified.After(ius) {
		hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
		return
	}

	if ims, err := hummingbird.ParseDate(request.Header.Get("If-Modified-Since")); err == nil && lastModified.Before(ims) {
		writer.WriteHeader(http.StatusNotModified)
		return
	}

	headers.Set("Accept-Ranges", "bytes")
	headers.Set("Content-Type", metadata["Content-Type"])
	headers.Set("Content-Length", metadata["Content-Length"])

	if rangeHeader := request.Header.Get("Range"); rangeHeader != "" {
		ranges, err := hummingbird.ParseRange(rangeHeader, obj.ContentLength())
		if err != nil {
			headers.Set("Content-Length", "0")
			writer.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		} else if ranges != nil && len(ranges) == 1 {
			headers.Set("Content-Length", strconv.FormatInt(int64(ranges[0].End-ranges[0].Start), 10))
			headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", ranges[0].Start, ranges[0].End-1, obj.ContentLength()))
			writer.WriteHeader(http.StatusPartialContent)
			obj.CopyRange(writer, ranges[0].Start, ranges[0].End)
			return
		} else if ranges != nil && len(ranges) > 1 {
			w := hummingbird.NewMultiWriter(writer)
			responseLength := int64(4 + len(w.Boundary()) + (len(w.Boundary())+len(metadata["Content-Type"])+47)*len(ranges))
			for _, rng := range ranges {
				responseLength += int64(len(fmt.Sprintf("%d-%d/%d", rng.Start, rng.End-1, obj.ContentLength()))) + rng.End - rng.Start
			}
			headers.Set("Content-Length", strconv.FormatInt(responseLength, 10))
			headers.Set("Content-Type", "multipart/byteranges;boundary="+w.Boundary())
			writer.WriteHeader(http.StatusPartialContent)
			for _, rng := range ranges {
				part, _ := w.CreatePart(textproto.MIMEHeader{"Content-Type": []string{metadata["Content-Type"]},
					"Content-Range": []string{fmt.Sprintf("bytes %d-%d/%d", rng.Start, rng.End-1, obj.ContentLength())}})
				obj.CopyRange(part, rng.Start, rng.End)
			}
			w.Close()
			return
		}
	}
	writer.WriteHeader(http.StatusOK)
	if request.Method == "GET" {
		if server.checkEtags {
			hash := md5.New()
			obj.Copy(writer, hash)
			if hex.EncodeToString(hash.Sum(nil)) != metadata["ETag"] {
				obj.Quarantine()
			}
		} else {
			obj.Copy(writer)
		}
	} else {
		writer.Write([]byte{})
	}
}

func (server *ObjectServer) ObjPutHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	outHeaders := writer.Header()

	requestTimestamp, err := hummingbird.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error standardizing request X-Timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	if vars["obj"] == "" {
		http.Error(writer, fmt.Sprintf("Invalid path: %s", request.URL.Path), http.StatusBadRequest)
		return
	}
	if request.Header.Get("Content-Type") == "" {
		http.Error(writer, "No content type", http.StatusBadRequest)
		return
	}
	if deleteAt := request.Header.Get("X-Delete-At"); deleteAt != "" {
		if deleteTime, err := hummingbird.ParseDate(deleteAt); err != nil || deleteTime.Before(time.Now()) {
			http.Error(writer, "X-Delete-At in past", 400)
			return
		}
	}

	obj, err := server.newObject(request, vars, false)
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error getting obj: %s", err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	if obj.Exists() {
		if inm := request.Header.Get("If-None-Match"); inm == "*" {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
		metadata := obj.Metadata()
		if requestTime, err := hummingbird.ParseDate(requestTimestamp); err == nil {
			if lastModified, err := hummingbird.ParseDate(metadata["X-Timestamp"]); err == nil && !requestTime.After(lastModified) {
				outHeaders.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
				hummingbird.StandardResponse(writer, http.StatusConflict)
				return
			}
		}
		if inm := request.Header.Get("If-None-Match"); inm != "*" && strings.Contains(inm, metadata["ETag"]) {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
	}

	tempFile, err := obj.SetData(request.ContentLength)
	if err == DriveFullError {
		hummingbird.GetLogger(request).LogDebug("Not enough space available")
		hummingbird.CustomErrorResponse(writer, 507, vars)
		return
	} else if err != nil {
		hummingbird.GetLogger(request).LogError("Error making new file: %s", err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}

	hash := md5.New()
	totalSize, err := hummingbird.Copy(request.Body, tempFile, hash)
	if err == io.ErrUnexpectedEOF {
		hummingbird.StandardResponse(writer, 499)
		return
	} else if err != nil {
		hummingbird.GetLogger(request).LogError("Error writing to file: %s", err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	metadata := map[string]string{
		"name":           "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"],
		"X-Timestamp":    requestTimestamp,
		"Content-Type":   request.Header.Get("Content-Type"),
		"Content-Length": strconv.FormatInt(totalSize, 10),
		"ETag":           hex.EncodeToString(hash.Sum(nil)),
	}
	for key := range request.Header {
		if allowed, ok := server.allowedHeaders[key]; (ok && allowed) ||
			strings.HasPrefix(key, "X-Object-Meta-") ||
			strings.HasPrefix(key, "X-Object-Sysmeta-") {
			metadata[key] = request.Header.Get(key)
		}
	}
	requestEtag := strings.ToLower(request.Header.Get("ETag"))
	if requestEtag != "" && requestEtag != metadata["ETag"] {
		http.Error(writer, "Unprocessable Entity", 422)
		return
	}
	outHeaders.Set("ETag", metadata["ETag"])

	if err := obj.Commit(metadata); err != nil {
		hummingbird.GetLogger(request).LogError("Error saving object: %v", err)
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	server.containerUpdates(request, metadata, request.Header.Get("X-Delete-At"), vars, hummingbird.GetLogger(request))
	hummingbird.StandardResponse(writer, http.StatusCreated)
}

func (server *ObjectServer) ObjDeleteHandler(writer http.ResponseWriter, request *http.Request) {
	vars := hummingbird.GetVars(request)
	headers := writer.Header()
	requestTimestamp, err := hummingbird.StandardizeTimestamp(request.Header.Get("X-Timestamp"))
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error standardizing request X-Timestamp: %s", err.Error())
		http.Error(writer, "Invalid X-Timestamp header", http.StatusBadRequest)
		return
	}
	responseStatus := http.StatusNotFound

	obj, err := server.newObject(request, vars, false)
	if err != nil {
		hummingbird.GetLogger(request).LogError("Error getting obj: %s", err.Error())
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	defer obj.Close()

	if ida := request.Header.Get("X-If-Delete-At"); ida != "" {
		_, err = strconv.ParseInt(ida, 10, 64)
		if err != nil {
			hummingbird.StandardResponse(writer, http.StatusBadRequest)
			return
		}
		if !obj.Exists() {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
		metadata := obj.Metadata()
		if _, ok := metadata["X-Delete-At"]; ok {
			if ida != metadata["X-Delete-At"] {
				hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
				return
			}
		} else {
			hummingbird.StandardResponse(writer, http.StatusPreconditionFailed)
			return
		}
	}

	deleteAt := ""
	if obj.Exists() {
		responseStatus = http.StatusNoContent
		metadata := obj.Metadata()
		if xda, ok := metadata["X-Delete-At"]; ok {
			deleteAt = xda
		}
		if origTimestamp, ok := metadata["X-Timestamp"]; ok && origTimestamp >= requestTimestamp {
			headers.Set("X-Backend-Timestamp", origTimestamp)
			hummingbird.StandardResponse(writer, http.StatusConflict)
			return
		}
	} else {
		responseStatus = http.StatusNotFound
	}

	metadata := map[string]string{
		"X-Timestamp": requestTimestamp,
		"name":        "/" + vars["account"] + "/" + vars["container"] + "/" + vars["obj"],
	}
	if err := obj.Delete(metadata); err == DriveFullError {
		hummingbird.GetLogger(request).LogDebug("Not enough space available")
		hummingbird.CustomErrorResponse(writer, 507, vars)
		return
	} else if err != nil {
		hummingbird.GetLogger(request).LogError("Error deleting object: %v", err)
		hummingbird.StandardResponse(writer, http.StatusInternalServerError)
		return
	}
	headers.Set("X-Backend-Timestamp", metadata["X-Timestamp"])
	server.containerUpdates(request, metadata, deleteAt, vars, hummingbird.GetLogger(request))
	hummingbird.StandardResponse(writer, responseStatus)
}

func (server *ObjectServer) HealthcheckHandler(writer http.ResponseWriter, request *http.Request) {
	writer.Header().Set("Content-Length", "2")
	writer.WriteHeader(http.StatusOK)
	writer.Write([]byte("OK"))
	return
}

func (server *ObjectServer) ReconHandler(writer http.ResponseWriter, request *http.Request) {
	hummingbird.ReconHandler(server.driveRoot, writer, request)
	return
}

func (server *ObjectServer) DiskUsageHandler(writer http.ResponseWriter, request *http.Request) {
	data, err := server.diskInUse.MarshalJSON()
	if err == nil {
		writer.WriteHeader(http.StatusOK)
		writer.Write(data)
	} else {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))
	}
	return
}

func (server *ObjectServer) LogRequest(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		newWriter := &hummingbird.WebWriter{ResponseWriter: writer, Status: 500, ResponseStarted: false}
		requestLogger := &hummingbird.RequestLogger{Request: request, Logger: server.logger, W: newWriter}
		defer requestLogger.LogPanics("LOGGING REQUEST")
		start := time.Now()
		hummingbird.SetLogger(request, requestLogger)
		next.ServeHTTP(newWriter, request)
		forceAcquire := request.Header.Get("X-Force-Acquire") == "true"

		extraInfo := "-"
		if forceAcquire {
			extraInfo = "FA"
		}
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
			extraInfo))

	}
	return http.HandlerFunc(fn)
}

func (server *ObjectServer) AcquireDevice(next http.Handler) http.Handler {
	fn := func(writer http.ResponseWriter, request *http.Request) {
		vars := hummingbird.GetVars(request)
		if device, ok := vars["device"]; ok && device != "" {
			devicePath := filepath.Join(server.driveRoot, device)
			if server.checkMounts {
				if mounted, err := hummingbird.IsMount(devicePath); err != nil || mounted != true {
					vars["Method"] = request.Method
					hummingbird.CustomErrorResponse(writer, 507, vars)
					return
				}
			}

			forceAcquire := request.Header.Get("X-Force-Acquire") == "true"
			if concRequests := server.diskInUse.Acquire(device, forceAcquire); concRequests != 0 {
				writer.Header().Set("X-Disk-Usage", strconv.FormatInt(concRequests, 10))
				hummingbird.StandardResponse(writer, 503)
				return
			}
			defer server.diskInUse.Release(device)

			if account, ok := vars["account"]; ok && account != "" {
				limitKey := fmt.Sprintf("%s/%s", device, account)
				if concRequests := server.accountDiskInUse.Acquire(limitKey, false); concRequests != 0 {
					hummingbird.StandardResponse(writer, 498)
					return
				}
				defer server.accountDiskInUse.Release(limitKey)
			}
		}
		next.ServeHTTP(writer, request)
	}
	return http.HandlerFunc(fn)
}

func (server *ObjectServer) updateDeviceLocks(seconds int64) {
	reloadTime := time.Duration(seconds) * time.Second
	for {
		time.Sleep(reloadTime)
		for _, key := range server.diskInUse.Keys() {
			lockPath := filepath.Join(server.driveRoot, key, "lock_device")
			if hummingbird.Exists(lockPath) {
				server.diskInUse.Lock(key)
			} else {
				server.diskInUse.Unlock(key)
			}
		}
	}
}

func (server *ObjectServer) GetHandler(config hummingbird.Config) http.Handler {
	commonHandlers := alice.New(middleware.ClearHandler, server.LogRequest, middleware.ValidateRequest, server.AcquireDevice)
	router := hummingbird.NewRouter()
	router.Get("/healthcheck", commonHandlers.ThenFunc(server.HealthcheckHandler))
	router.Get("/diskusage", commonHandlers.ThenFunc(server.DiskUsageHandler))
	router.Get("/recon/:method/:recon_type", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/recon/:method", commonHandlers.ThenFunc(server.ReconHandler))
	router.Get("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjGetHandler))
	router.Head("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjGetHandler))
	router.Put("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjPutHandler))
	router.Delete("/:device/:partition/:account/:container/*obj", commonHandlers.ThenFunc(server.ObjDeleteHandler))
	router.Get("/debug/pprof/:parm", http.DefaultServeMux)
	router.Post("/debug/pprof/:parm", http.DefaultServeMux)
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, fmt.Sprintf("Invalid path: %s", r.URL.Path), http.StatusBadRequest)
	})
	return alice.New(middleware.GrepObject).Then(router)
}

func GetServer(serverconf hummingbird.Config, flags *flag.FlagSet) (bindIP string, bindPort int, serv hummingbird.Server, logger hummingbird.SysLogLike, err error) {
	server := &ObjectServer{driveRoot: "/srv/node", hashPathPrefix: "", hashPathSuffix: "",
		allowedHeaders: map[string]bool{"Content-Disposition": true,
			"Content-Encoding":      true,
			"X-Delete-At":           true,
			"X-Object-Manifest":     true,
			"X-Static-Large-Object": true,
		},
	}
	server.hashPathPrefix, server.hashPathSuffix, err = hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return "", 0, nil, nil, err
	}
	server.objEngines = make(map[int]ObjectEngine)
	for _, policy := range hummingbird.LoadPolicies() {
		if newEngine, err := FindEngine(policy.Type); err != nil {
			return "", 0, nil, nil, fmt.Errorf("Unable to find object engine type %s: %v", policy.Type, err)
		} else {
			server.objEngines[policy.Index], err = newEngine(serverconf, policy, flags)
			if err != nil {
				return "", 0, nil, nil, fmt.Errorf("Error instantiating object engine type %s: %v", policy.Type, err)
			}
		}
	}

	server.driveRoot = serverconf.GetDefault("app:object-server", "devices", "/srv/node")
	server.checkMounts = serverconf.GetBool("app:object-server", "mount_check", true)
	server.checkEtags = serverconf.GetBool("app:object-server", "check_etags", false)
	server.logLevel = serverconf.GetDefault("app:object-server", "log_level", "INFO")
	server.diskInUse = hummingbird.NewKeyedLimit(serverconf.GetLimit("app:object-server", "disk_limit", 25, 0))
	server.accountDiskInUse = hummingbird.NewKeyedLimit(serverconf.GetLimit("app:object-server", "account_rate_limit", 20, 0))
	server.expiringDivisor = serverconf.GetInt("app:object-server", "expiring_objects_container_divisor", 86400)
	bindIP = serverconf.GetDefault("app:object-server", "bind_ip", "0.0.0.0")
	bindPort = int(serverconf.GetInt("app:object-server", "bind_port", 6000))
	if allowedHeaders, ok := serverconf.Get("app:object-server", "allowed_headers"); ok {
		headers := strings.Split(allowedHeaders, ",")
		for i := range headers {
			server.allowedHeaders[textproto.CanonicalMIMEHeaderKey(strings.TrimSpace(headers[i]))] = true
		}
	}
	server.logger = hummingbird.SetupLogger(serverconf.GetDefault("app:object-server", "log_facility", "LOG_LOCAL1"), "object-server", "")
	server.updateTimeout = time.Duration(serverconf.GetFloat("app:object-server", "container_update_timeout", 0.25) * float64(time.Second))
	connTimeout := time.Duration(serverconf.GetFloat("app:object-server", "conn_timeout", 1.0) * float64(time.Second))
	nodeTimeout := time.Duration(serverconf.GetFloat("app:object-server", "node_timeout", 10.0) * float64(time.Second))
	server.updateClient = &http.Client{
		Timeout:   nodeTimeout,
		Transport: &http.Transport{Dial: (&net.Dialer{Timeout: connTimeout}).Dial},
	}

	deviceLockUpdateSeconds := serverconf.GetInt("app:object-server", "device_lock_update_seconds", 0)
	if deviceLockUpdateSeconds > 0 {
		go server.updateDeviceLocks(deviceLockUpdateSeconds)
	}

	statsdHost := serverconf.GetDefault("app:object-server", "log_statsd_host", "")
	if statsdHost != "" {
		statsdPort := serverconf.GetInt("app:object-server", "log_statsd_port", 8125)
		// Go metrics collection pause interval in seconds
		statsdPause := serverconf.GetInt("app:object-server", "statsd_collection_pause", 10)
		basePrefix := serverconf.GetDefault("app:object-server", "log_statsd_metric_prefix", "")
		prefix := basePrefix + ".go.objectserver"
		go hummingbird.CollectRuntimeMetrics(statsdHost, statsdPort, statsdPause, prefix)
	}

	return bindIP, bindPort, server, server.logger, nil
}

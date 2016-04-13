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
	"time"
)

const (
	MAX_FILE_SIZE             = int64(5368709122)
	MAX_META_NAME_LENGTH      = 128
	MAX_META_VALUE_LENGTH     = 256
	MAX_META_COUNT            = 90
	MAX_META_OVERALL_SIZE     = 4096
	MAX_HEADER_SIZE           = 8192
	MAX_OBJECT_NAME_LENGTH    = 1024
	CONTAINER_LISTING_LIMIT   = 10000
	ACCOUNT_LISTING_LIMIT     = 10000
	MAX_ACCOUNT_NAME_LENGTH   = 256
	MAX_CONTAINER_NAME_LENGTH = 256
	EXTRA_HEADER_COUNT        = 0
)

func CheckMetadata(req *http.Request, targetType string) (int, string) {
	metaCount := 0
	metaSize := 0
	metaPrefix := fmt.Sprintf("X-%s-Meta", targetType)
	for key := range req.Header {
		if len(key) > MAX_HEADER_SIZE {
			return http.StatusBadRequest, fmt.Sprintf("Header value too long: %s", key[:MAX_META_NAME_LENGTH])
		}
		if !strings.HasPrefix(key, metaPrefix) {
			continue
		}
		value := req.Header.Get(key)
		key = key[len(metaPrefix):]
		metaCount += 1
		metaSize += len(key) + len(value)
		if key == "" {
			return http.StatusBadRequest, "Metadata name cannot be empty"
		}
		if len(key) > MAX_META_NAME_LENGTH {
			return http.StatusBadRequest, fmt.Sprintf("Metadata name too long: %s%s", metaPrefix, key)
		}
		if len(value) > MAX_META_VALUE_LENGTH {
			return http.StatusBadRequest, fmt.Sprintf("Metadata value longer than %d: %s%s", MAX_META_VALUE_LENGTH, metaPrefix, key)
		}
		if metaCount > MAX_META_COUNT {
			return http.StatusBadRequest, fmt.Sprintf("Too many metadata items; max %d", MAX_META_COUNT)
		}
		if metaSize > MAX_META_OVERALL_SIZE {
			return http.StatusBadRequest, fmt.Sprintf("Total metadata too large; max %d", MAX_META_OVERALL_SIZE)
		}
	}
	return http.StatusOK, ""
}

func CheckObjPut(req *http.Request, objectName string) (int, string) {
	if req.ContentLength >= 0 {
		if req.ContentLength > MAX_FILE_SIZE {
			return http.StatusRequestEntityTooLarge, "Your request is too large."
		}
	} else if req.Header.Get("Transfer-Encoding") != "chunked" {
		return http.StatusLengthRequired, "Missing Content-Length header."
	}
	if req.Header.Get("X-Copy-From") != "" && req.ContentLength != 0 {
		return http.StatusBadRequest, "Copy requests require a zero byte body"
	}
	if len(objectName) > MAX_OBJECT_NAME_LENGTH {
		return http.StatusBadRequest, fmt.Sprintf("Object name length of %d longer than %d", len(objectName), MAX_OBJECT_NAME_LENGTH)
	}
	if req.Header.Get("Content-Type") == "" {
		return http.StatusBadRequest, "No content type"
	}
	// check content-type is utf-8

	if xda := req.Header.Get("X-Delete-At"); xda != "" {
		if deleteAfter, err := strconv.ParseInt(xda, 10, 64); err != nil {
			return http.StatusBadRequest, "Non-integer X-Delete-At"
		} else if deleteAfter < time.Now().Unix() {
			return http.StatusBadRequest, "X-Delete-At in past"
		}
	} else if xda := req.Header.Get("X-Delete-After"); xda != "" {
		if deleteAfter, err := strconv.ParseInt(xda, 10, 64); err != nil {
			return http.StatusBadRequest, "Non-integer X-Delete-After"
		} else if deleteAfter < 0 {
			return http.StatusBadRequest, "X-Delete-After in past"
		} else {
			req.Header.Set("X-Delete-At", strconv.FormatInt(time.Now().Unix()+deleteAfter, 10))
		}
	}

	return CheckMetadata(req, "Object")
}

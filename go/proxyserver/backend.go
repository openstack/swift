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
	"net"
	"net/http"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

func GetClient() *http.Client {
	transport := http.Transport{
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 5 * time.Second,
		}).Dial,
	}
	return &http.Client{Transport: &transport, Timeout: 30 * time.Second}
}

type MultiClient struct {
	client       *http.Client
	requestCount int
	done         []chan int
}

func (mc *MultiClient) Do(log hummingbird.LoggingContext, req *http.Request) {
	donech := make(chan int)
	mc.done = append(mc.done, donech)
	go func(client *http.Client, req *http.Request, done chan int) {
		resp, err := client.Do(req)
		if err != nil {
			log.LogError("Error getting response for %s to %s: %s", req.Method, req.URL.Host, err.Error())
			done <- 500
		} else {
			resp.Body.Close()
			if resp.StatusCode/100 == 5 {
				log.LogError("5XX response code on %s to %s", req.Method, req.URL.Host)
			}
			done <- resp.StatusCode
		}
	}(mc.client, req, donech)
	mc.requestCount += 1
}

func (mc *MultiClient) BestResponse(writer http.ResponseWriter) {
	var responses []int
	quorum := (mc.requestCount / 2) + 1
	for chanIndex, done := range mc.done {
		responses = append(responses, <-done)
		for statusRange := 200; statusRange <= 400; statusRange += 100 {
			rangeCount := 0
			for _, response := range responses {
				if response >= statusRange && response < (statusRange+100) {
					rangeCount += 1
					if rangeCount >= quorum {
						http.Error(writer, http.StatusText(response), response)
						go func() { // wait for any remaining connections to finish
							for i := chanIndex + 1; i < len(mc.done); i++ {
								<-mc.done[i]
							}
						}()
						return
					}
				}
			}
		}
	}
	http.Error(writer, http.StatusText(500), 500)
}

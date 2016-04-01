package client

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/openstack/swift/go/hummingbird"
)

func map2Headers(m map[string]string) http.Header {
	if m == nil {
		return nil
	}
	headers := make(http.Header, len(m))
	for k, v := range m {
		headers.Set(k, v)
	}
	return headers
}

func headers2Map(headers http.Header) map[string]string {
	if headers == nil {
		return nil
	}
	m := make(map[string]string, len(headers))
	for k := range headers {
		m[k] = headers.Get(k)
	}
	return m
}

func mkquery(options map[string]string) string {
	query := ""
	for k, v := range options {
		if v != "" {
			query += url.QueryEscape(k) + "=" + url.QueryEscape(v) + "&"
		}
	}
	if query != "" {
		return "?" + strings.TrimRight(query, "&")
	}
	return ""
}

type ProxyDirectClient struct {
	client        *http.Client
	AccountRing   hummingbird.Ring
	ContainerRing hummingbird.Ring
	ObjectRing    hummingbird.Ring
}

func (c *ProxyDirectClient) quorumResponse(reqs ...*http.Request) int {
	var requestCount int
	var done []chan int
	for _, req := range reqs {
		donech := make(chan int)
		done = append(done, donech)
		go func(client *http.Client, req *http.Request, done chan int) {
			resp, err := client.Do(req)
			if err != nil {
				done <- 500
			} else {
				resp.Body.Close()
				done <- resp.StatusCode
			}
		}(c.client, req, donech)
		requestCount += 1
	}

	var responses []int
	quorum := (requestCount / 2) + 1
	for chanIndex, d := range done {
		responses = append(responses, <-d)
		for statusRange := 200; statusRange <= 400; statusRange += 100 {
			rangeCount := 0
			for _, response := range responses {
				if response >= statusRange && response < (statusRange+100) {
					rangeCount += 1
					if rangeCount >= quorum {
						go func() { // wait for any remaining connections to finish
							for i := chanIndex + 1; i < len(done); i++ {
								<-(done[i])
							}
						}()
						return response
					}
				}
			}
		}
	}
	return 500
}

func (c *ProxyDirectClient) firstResponse(reqs ...*http.Request) *http.Response {
	resps := make(chan *http.Response)
	ticker := time.NewTicker(time.Second)
	defer func() {
		close(resps)
		ticker.Stop()
	}()
	for _, req := range reqs {
		go func() {
			resp, err := c.client.Do(req)
			if err != nil {
				resp = nil
			} else if resp.StatusCode/100 != 2 {
				resp.Body.Close()
				resp = nil
			}
			select {
			case resps <- resp:
				return
			default:
				if resp != nil {
					resp.Body.Close()
				}
			}
		}()
		select {
		case <-ticker.C:
		case resp := <-resps:
			if resp != nil {
				return resp
			}
		}
	}
	return nil
}

var _ ProxyClient = &ProxyDirectClient{}

func (c *ProxyDirectClient) PutAccount(account string, headers http.Header) int {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(account))
		req, _ := http.NewRequest("PUT", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PostAccount(account string, headers http.Header) int {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(account))
		req, _ := http.NewRequest("POST", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) GetAccount(account string, options map[string]string, headers http.Header) (io.ReadCloser, http.Header, int) {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	query := mkquery(options)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), query)
		req, _ := http.NewRequest("GET", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, nil, 404
	}
	return resp.Body, resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) HeadAccount(account string, headers http.Header) (http.Header, int) {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			continue
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, 404
	}
	resp.Body.Close()
	return resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) DeleteAccount(account string, headers http.Header) int {
	partition := c.AccountRing.GetPartition(account, "", "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.AccountRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s", device.Ip, device.Port, device.Device, partition, hummingbird.Urlencode(account))
		req, _ := http.NewRequest("DELETE", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PutContainer(account string, container string, headers http.Header) int {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	reqs := make([]*http.Request, 0)
	for i, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container))
		req, _ := http.NewRequest("PUT", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		req.Header.Set("X-Account-Host", fmt.Sprintf("%s:%d", accountDevices[i].Ip, accountDevices[i].Port))
		req.Header.Set("X-Account-Device", accountDevices[i].Device)
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PostContainer(account string, container string, headers http.Header) int {
	partition := c.ContainerRing.GetPartition(account, container, "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container))
		req, _ := http.NewRequest("POST", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) GetContainer(account string, container string, options map[string]string, headers http.Header) (io.ReadCloser, http.Header, int) {
	partition := c.ContainerRing.GetPartition(account, container, "")
	reqs := make([]*http.Request, 0)
	query := mkquery(options)
	for _, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container), query)
		req, _ := http.NewRequest("GET", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, nil, 404
	}
	return resp.Body, resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) HeadContainer(account string, container string, headers http.Header) (http.Header, int) {
	partition := c.ObjectRing.GetPartition(account, container, "")
	reqs := make([]*http.Request, 0)
	for _, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			continue
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, 404
	}
	resp.Body.Close()
	return resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) DeleteContainer(account string, container string, headers http.Header) int {
	partition := c.ContainerRing.GetPartition(account, container, "")
	accountPartition := c.AccountRing.GetPartition(account, "", "")
	accountDevices := c.AccountRing.GetNodes(accountPartition)
	reqs := make([]*http.Request, 0)
	for i, device := range c.ContainerRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container))
		req, _ := http.NewRequest("DELETE", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("X-Account-Partition", strconv.FormatUint(accountPartition, 10))
		req.Header.Set("X-Account-Host", fmt.Sprintf("%s:%d", accountDevices[i].Ip, accountDevices[i].Port))
		req.Header.Set("X-Account-Device", accountDevices[i].Device)
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PutObject(account string, container string, obj string, headers http.Header, src io.Reader) int {
	partition := c.ObjectRing.GetPartition(account, container, obj)
	containerPartition := c.ContainerRing.GetPartition(account, container, "")
	containerDevices := c.ContainerRing.GetNodes(containerPartition)
	var writers []*io.PipeWriter
	reqs := make([]*http.Request, 0)
	for i, device := range c.ObjectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container), hummingbird.Urlencode(obj))
		rp, wp := io.Pipe()
		defer wp.Close()
		defer rp.Close()
		req, err := http.NewRequest("PUT", url, rp)
		if err != nil {
			continue
		}
		writers = append(writers, wp)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		// req.ContentLength = request.ContentLength // TODO
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		req.Header.Set("Expect", "100-Continue")
		reqs = append(reqs, req)
	}
	go func() {
		mw := io.MultiWriter(writers[0], writers[1], writers[2])
		io.Copy(mw, src)
		for _, writer := range writers {
			writer.Close()
		}
	}()
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) PostObject(account string, container string, obj string, headers http.Header) int {
	partition := c.ObjectRing.GetPartition(account, container, obj)
	containerPartition := c.ContainerRing.GetPartition(account, container, "")
	containerDevices := c.ContainerRing.GetNodes(containerPartition)
	reqs := make([]*http.Request, 0)
	for i, device := range c.ObjectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container), hummingbird.Urlencode(obj))
		req, _ := http.NewRequest("POST", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func (c *ProxyDirectClient) GetObject(account string, container string, obj string, headers http.Header) (io.ReadCloser, http.Header, int) {
	partition := c.ObjectRing.GetPartition(account, container, obj)
	nodes := c.ObjectRing.GetNodes(partition)
	reqs := make([]*http.Request, 0, len(nodes))
	for _, device := range nodes {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container), hummingbird.Urlencode(obj))
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			continue
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, nil, 404
	}
	return resp.Body, resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) GrepObject(account string, container string, obj string, search string) (io.ReadCloser, http.Header, int) {
	partition := c.ObjectRing.GetPartition(account, container, obj)
	nodes := c.ObjectRing.GetNodes(partition)
	reqs := make([]*http.Request, 0, len(nodes))
	for _, device := range nodes {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s?e=%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container), hummingbird.Urlencode(obj), hummingbird.Urlencode(search))
		req, err := http.NewRequest("GREP", url, nil)
		if err != nil {
			continue
		}
		reqs = append(reqs, req)
	}
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, nil, 404
	}
	return resp.Body, resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) HeadObject(account string, container string, obj string, headers http.Header) (http.Header, int) {
	partition := c.ObjectRing.GetPartition(account, container, obj)
	nodes := c.ObjectRing.GetNodes(partition)
	reqs := make([]*http.Request, 0, len(nodes))
	for _, device := range nodes {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container), hummingbird.Urlencode(obj))
		req, err := http.NewRequest("HEAD", url, nil)
		if err != nil {
			continue
		}
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		reqs = append(reqs, req)
	}
	resp := c.firstResponse(reqs...)
	if resp == nil {
		return nil, 404
	}
	resp.Body.Close()
	return resp.Header, resp.StatusCode
}

func (c *ProxyDirectClient) DeleteObject(account string, container string, obj string, headers http.Header) int {
	partition := c.ObjectRing.GetPartition(account, container, obj)
	containerPartition := c.ContainerRing.GetPartition(account, container, "")
	containerDevices := c.ContainerRing.GetNodes(containerPartition)
	reqs := make([]*http.Request, 0)
	for i, device := range c.ObjectRing.GetNodes(partition) {
		url := fmt.Sprintf("http://%s:%d/%s/%d/%s/%s/%s", device.Ip, device.Port, device.Device, partition,
			hummingbird.Urlencode(account), hummingbird.Urlencode(container), hummingbird.Urlencode(obj))
		req, _ := http.NewRequest("DELETE", url, nil)
		for key := range headers {
			req.Header.Set(key, headers.Get(key))
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("X-Container-Partition", strconv.FormatUint(containerPartition, 10))
		req.Header.Set("X-Container-Host", fmt.Sprintf("%s:%d", containerDevices[i].Ip, containerDevices[i].Port))
		req.Header.Set("X-Container-Device", containerDevices[i].Device)
		reqs = append(reqs, req)
	}
	return c.quorumResponse(reqs...)
}

func NewProxyDirectClient() (ProxyClient, error) {
	c := &ProxyDirectClient{}
	hashPathPrefix, hashPathSuffix, err := hummingbird.GetHashPrefixAndSuffix()
	if err != nil {
		return nil, err
	}
	c.ObjectRing, err = hummingbird.GetRing("object", hashPathPrefix, hashPathSuffix)
	if err != nil {
		return nil, err
	}
	c.ContainerRing, err = hummingbird.GetRing("container", hashPathPrefix, hashPathSuffix)
	if err != nil {
		return nil, err
	}
	c.AccountRing, err = hummingbird.GetRing("account", hashPathPrefix, hashPathSuffix)
	if err != nil {
		return nil, err
	}
	c.client = &http.Client{
		Transport: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 5 * time.Second,
			}).Dial,
		},
		Timeout: 120 * time.Minute,
	}
	return c, nil
}

type directClient struct {
	*ProxyDirectClient
	account string
}

var _ Client = &directClient{}

func (c *directClient) PutAccount(headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PutAccount(c.account, map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PostAccount(headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PostAccount(c.account, map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, headers map[string]string) ([]ContainerRecord, map[string]string, error) {
	options := map[string]string{
		"format":     "json",
		"marker":     marker,
		"end_marker": endMarker,
		"prefix":     prefix,
		"delimiter":  delimiter,
	}
	if limit != 0 {
		options["limit"] = strconv.Itoa(limit)
	}
	r, h, code := c.ProxyDirectClient.GetAccount(c.account, options, map2Headers(headers))
	if code != 200 {
		return nil, nil, HTTPError(code)
	}
	var accountListing []ContainerRecord
	decoder := json.NewDecoder(r)
	decoder.Decode(&accountListing)
	return accountListing, headers2Map(h), nil
}

func (c *directClient) HeadAccount(headers map[string]string) (map[string]string, error) {
	h, code := c.ProxyDirectClient.HeadAccount(c.account, map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return headers2Map(h), nil
}

func (c *directClient) DeleteAccount(headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.DeleteAccount(c.account, map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PutContainer(container string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PutContainer(c.account, container, map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PostContainer(container string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PostContainer(c.account, container, map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, headers map[string]string) ([]ObjectRecord, map[string]string, error) {
	options := map[string]string{
		"format":     "json",
		"marker":     marker,
		"end_marker": endMarker,
		"prefix":     prefix,
		"delimiter":  delimiter,
	}
	if limit != 0 {
		options["limit"] = strconv.Itoa(limit)
	}
	r, h, code := c.ProxyDirectClient.GetContainer(c.account, container, options, map2Headers(headers))
	if code != 200 {
		return nil, nil, HTTPError(code)
	}
	defer r.Close()
	var containerListing []ObjectRecord
	decoder := json.NewDecoder(r)
	decoder.Decode(&containerListing)
	return containerListing, headers2Map(h), nil
}

func (c *directClient) HeadContainer(container string, headers map[string]string) (map[string]string, error) {
	h, code := c.ProxyDirectClient.HeadContainer(c.account, container, map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return headers2Map(h), nil
}

func (c *directClient) DeleteContainer(container string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.DeleteContainer(c.account, container, map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PutObject(container string, obj string, headers map[string]string, src io.Reader) (err error) {
	if code := c.ProxyDirectClient.PutObject(c.account, container, obj, map2Headers(headers), src); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) PostObject(container string, obj string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.PostObject(c.account, container, obj, map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

func (c *directClient) GetObject(container string, obj string, headers map[string]string) (io.ReadCloser, map[string]string, error) {
	r, h, code := c.ProxyDirectClient.GetObject(c.account, container, obj, map2Headers(headers))
	if code/100 != 2 {
		if r != nil {
			r.Close()
		}
		return nil, nil, HTTPError(code)
	}
	return r, headers2Map(h), nil
}

func (c *directClient) HeadObject(container string, obj string, headers map[string]string) (map[string]string, error) {
	h, code := c.ProxyDirectClient.HeadObject(c.account, container, obj, map2Headers(headers))
	if code/100 != 2 {
		return nil, HTTPError(code)
	}
	return headers2Map(h), nil
}

func (c *directClient) DeleteObject(container string, obj string, headers map[string]string) (err error) {
	if code := c.ProxyDirectClient.DeleteObject(c.account, container, obj, map2Headers(headers)); code/100 != 2 {
		return HTTPError(code)
	}
	return nil
}

// NewDirectClient creates a new direct client with the given account name.
func NewDirectClient(account string) (Client, error) {
	rdc, err := NewProxyDirectClient()
	if err != nil {
		return nil, err
	}
	return &directClient{account: account, ProxyDirectClient: rdc.(*ProxyDirectClient)}, nil
}

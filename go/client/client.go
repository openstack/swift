package client

import (
	"fmt"
	"io"
	"net/http"
)

// HTTPError represents a non-200 HTTP response code.
type HTTPError int

func (e HTTPError) Error() string {
	return fmt.Sprintf("Bad HTTP status code: %d", e)
}

// ContainerRecord is an entry in an account listing.
type ContainerRecord struct {
	Count int64  `json:"count"`
	Bytes int64  `json:"bytes"`
	Name  string `json:"name"`
}

// ObjectRecord is an entry in a container listing.
type ObjectRecord struct {
	Hash         string `json:"hash"`
	LastModified string `json:"last_modified"`
	Bytes        int    `json:"bytes"`
	Name         string `json:"name"`
	ContentType  string `json:"content_type"`
}

// Client is an API interface to CloudFiles.
type Client interface {
	PutAccount(headers map[string]string) (err error)
	PostAccount(headers map[string]string) (err error)
	GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, headers map[string]string) ([]ContainerRecord, map[string]string, error)
	HeadAccount(headers map[string]string) (map[string]string, error)
	DeleteAccount(headers map[string]string) (err error)
	PutContainer(container string, headers map[string]string) (err error)
	PostContainer(container string, headers map[string]string) (err error)
	GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, headers map[string]string) ([]ObjectRecord, map[string]string, error)
	HeadContainer(container string, headers map[string]string) (map[string]string, error)
	DeleteContainer(container string, headers map[string]string) (err error)
	PutObject(container string, obj string, headers map[string]string, src io.Reader) (err error)
	PostObject(container string, obj string, headers map[string]string) (err error)
	GetObject(container string, obj string, headers map[string]string) (io.ReadCloser, map[string]string, error)
	HeadObject(container string, obj string, headers map[string]string) (map[string]string, error)
	DeleteObject(container string, obj string, headers map[string]string) (err error)
}

// ProxyClient is similar to Client except it also accepts an account parameter to its operations.  This is meant to be used by the proxy server.
type ProxyClient interface {
	PutAccount(account string, headers http.Header) int
	PostAccount(account string, headers http.Header) int
	GetAccount(account string, options map[string]string, headers http.Header) (io.ReadCloser, http.Header, int)
	HeadAccount(account string, headers http.Header) (http.Header, int)
	DeleteAccount(account string, headers http.Header) int
	PutContainer(account string, container string, headers http.Header) int
	PostContainer(account string, container string, headers http.Header) int
	GetContainer(account string, container string, options map[string]string, headers http.Header) (io.ReadCloser, http.Header, int)
	HeadContainer(account string, container string, headers http.Header) (http.Header, int)
	DeleteContainer(account string, container string, headers http.Header) int
	PutObject(account string, container string, obj string, headers http.Header, src io.Reader) int
	PostObject(account string, container string, obj string, headers http.Header) int
	GetObject(account string, container string, obj string, headers http.Header) (io.ReadCloser, http.Header, int)
	HeadObject(account string, container string, obj string, headers http.Header) (http.Header, int)
	DeleteObject(account string, container string, obj string, headers http.Header) int
}

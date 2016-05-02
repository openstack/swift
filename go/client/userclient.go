package client

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// userClient is a Client to be used by end-users.  It knows how to authenticate with auth v1 and v2.
type userClient struct {
	client                                              *http.Client
	ServiceURL                                          string
	AuthToken                                           string
	tenant, username, password, apikey, region, authurl string
	private                                             bool
}

var _ Client = &userClient{}

func (c *userClient) authedRequest(method string, path string, body io.Reader, headers map[string]string) (*http.Request, error) {
	req, err := http.NewRequest(method, c.ServiceURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Auth-Token", c.AuthToken)
	req.Header.Set("User-Agent", "Hummingbird Client")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (c *userClient) do(req *http.Request) (*http.Response, error) {
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == 401 {
		resp.Body.Close()
		if c.authenticate() != nil {
			return nil, errors.New("Authentication failed.")
		}
		resp, err = c.client.Do(req)
		if err != nil {
			return nil, err
		}
	}
	if resp.StatusCode/100 != 2 {
		resp.Body.Close()
		return nil, HTTPError(resp.StatusCode)
	}
	return resp, nil
}

func (c *userClient) doRequest(method string, path string, body io.Reader, headers map[string]string) error {
	req, err := c.authedRequest(method, path, body, headers)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

func (c *userClient) PutAccount(headers map[string]string) (err error) {
	return c.doRequest("PUT", "", nil, headers)
}

func (c *userClient) PostAccount(headers map[string]string) (err error) {
	return c.doRequest("POST", "", nil, headers)
}

func (c *userClient) GetAccount(marker string, endMarker string, limit int, prefix string, delimiter string, headers map[string]string) ([]ContainerRecord, map[string]string, error) {
	limitStr := ""
	if limit > 0 {
		limitStr = strconv.Itoa(limit)
	}
	path := mkquery(map[string]string{"marker": marker, "end_marker": endMarker, "prefix": prefix, "delimiter": delimiter, "limit": limitStr})
	req, err := c.authedRequest("GET", path, nil, headers)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	var accountListing []ContainerRecord
	if err := json.Unmarshal(body, &accountListing); err != nil {
		return nil, nil, err
	}
	return accountListing, headers2Map(resp.Header), nil
}

func (c *userClient) HeadAccount(headers map[string]string) (map[string]string, error) {
	req, err := c.authedRequest("HEAD", "", nil, headers)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return headers2Map(resp.Header), nil
}

func (c *userClient) DeleteAccount(headers map[string]string) (err error) {
	return c.doRequest("DELETE", "", nil, nil)
}

func (c *userClient) PutContainer(container string, headers map[string]string) (err error) {
	return c.doRequest("PUT", "/"+container, nil, headers)
}

func (c *userClient) PostContainer(container string, headers map[string]string) (err error) {
	return c.doRequest("POST", "/"+container, nil, headers)
}

func (c *userClient) GetContainer(container string, marker string, endMarker string, limit int, prefix string, delimiter string, headers map[string]string) ([]ObjectRecord, map[string]string, error) {
	limitStr := ""
	if limit > 0 {
		limitStr = strconv.Itoa(limit)
	}
	path := "/" + container + mkquery(map[string]string{"marker": marker, "end_marker": endMarker, "prefix": prefix, "delimiter": delimiter, "limit": limitStr})
	req, err := c.authedRequest("GET", path, nil, headers)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := c.do(req)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	var containerListing []ObjectRecord
	if err := json.Unmarshal(body, &containerListing); err != nil {
		return nil, nil, err
	}
	return containerListing, headers2Map(resp.Header), nil
}

func (c *userClient) HeadContainer(container string, headers map[string]string) (map[string]string, error) {
	req, err := c.authedRequest("HEAD", "/"+container, nil, headers)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return headers2Map(resp.Header), nil
}

func (c *userClient) DeleteContainer(container string, headers map[string]string) (err error) {
	return c.doRequest("DELETE", "/"+container, nil, headers)
}

func (c *userClient) PutObject(container string, obj string, headers map[string]string, src io.Reader) (err error) {
	return c.doRequest("PUT", "/"+container+"/"+obj, src, headers)
}

func (c *userClient) PostObject(container string, obj string, headers map[string]string) (err error) {
	return c.doRequest("POST", "/"+container+"/"+obj, nil, headers)
}

func (c *userClient) GetObject(container string, obj string, headers map[string]string) (io.ReadCloser, map[string]string, error) {
	req, err := c.authedRequest("GET", "/"+container+"/"+obj, nil, headers)
	if err != nil {
		return nil, nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, nil, err
	}
	return resp.Body, headers2Map(resp.Header), nil
}

func (c *userClient) HeadObject(container string, obj string, headers map[string]string) (map[string]string, error) {
	req, err := c.authedRequest("HEAD", "/"+container+"/"+obj, nil, headers)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	resp.Body.Close()
	return headers2Map(resp.Header), nil
}

func (c *userClient) DeleteObject(container string, obj string, headers map[string]string) (err error) {
	return c.doRequest("DELETE", "/"+container+"/"+obj, nil, headers)
}

func (c *userClient) authenticatev1() error {
	req, err := http.NewRequest("GET", c.authurl, nil)
	req.Header.Set("X-Auth-User", c.username)
	req.Header.Set("X-Auth-Key", c.apikey)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return HTTPError(resp.StatusCode)
	}
	c.ServiceURL = resp.Header.Get("X-Storage-Url")
	c.AuthToken = resp.Header.Get("X-Auth-Token")
	if c.ServiceURL != "" && c.AuthToken != "" {
		return nil
	}
	return errors.New("Failed to authenticate.")
}

type KeystoneRequestV2 struct {
	Auth interface{} `json:"auth"`
}

type KeystonePasswordAuthV2 struct {
	TenantName          string `json:"tenantName"`
	PasswordCredentials struct {
		Username string `json:"username"`
		Password string `json:"password"`
	} `json:"passwordCredentials"`
}

type RaxAPIKeyAuthV2 struct {
	APIKeyCredentials struct {
		Username string `json:"username"`
		APIKey   string `json:"apiKey"`
	} `json:"RAX-KSKEY:apiKeyCredentials"`
}

type KeystoneResponseV2 struct {
	Access struct {
		Token struct {
			ID     string `json:"id"`
			Tenant struct {
				Name string `json:"name"`
				ID   string `json:"id"`
			} `json:"tenant"`
		} `json:"token"`
		ServiceCatalog []struct {
			Endpoints []struct {
				PublicURL   string `json:"publicURL"`
				InternalURL string `json:"internalURL"`
				Region      string `json:"region"`
			} `json:"endpoints"`
			Type string `json:"type"`
		} `json:"serviceCatalog"`
		User struct {
			RaxDefaultRegion string `json:"RAX-AUTH:defaultRegion"`
		} `json:"user"`
	} `json:"access"`
}

func (c *userClient) authenticatev2() (err error) {
	if !strings.HasSuffix(c.authurl, "tokens") {
		if c.authurl[len(c.authurl)-1] == '/' {
			c.authurl = c.authurl + "tokens"
		} else {
			c.authurl = c.authurl + "/tokens"
		}
	}
	var authReq []byte
	if c.password != "" {
		creds := &KeystonePasswordAuthV2{TenantName: c.tenant}
		creds.PasswordCredentials.Username = c.username
		creds.PasswordCredentials.Password = c.password
		authReq, err = json.Marshal(&KeystoneRequestV2{Auth: creds})
	} else if c.apikey != "" {
		creds := &RaxAPIKeyAuthV2{}
		creds.APIKeyCredentials.Username = c.username
		creds.APIKeyCredentials.APIKey = c.apikey
		authReq, err = json.Marshal(&KeystoneRequestV2{Auth: creds})
	} else {
		return errors.New("Couldn't figure out what credentials to use.")
	}
	if err != nil {
		return err
	}
	resp, err := c.client.Post(c.authurl, "application/json", bytes.NewBuffer(authReq))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return HTTPError(resp.StatusCode)
	}
	var authResponse KeystoneResponseV2
	if body, err := ioutil.ReadAll(resp.Body); err != nil {
		return err
	} else if err = json.Unmarshal(body, &authResponse); err != nil {
		return err
	}
	c.AuthToken = authResponse.Access.Token.ID
	region := c.region
	if region == "" {
		region = authResponse.Access.User.RaxDefaultRegion
	}
	for _, s := range authResponse.Access.ServiceCatalog {
		if s.Type == "object-store" {
			for _, e := range s.Endpoints {
				if e.Region == region || region == "" || len(s.Endpoints) == 1 {
					if c.private {
						c.ServiceURL = e.InternalURL
					} else {
						c.ServiceURL = e.PublicURL
					}
					return nil
				}
			}
		}
	}
	return errors.New("Didn't find endpoint")
}

func (c *userClient) authenticate() error {
	if strings.Contains(c.authurl, "/v2") {
		return c.authenticatev2()
	} else {
		return c.authenticatev1()
	}
}

// NewClient creates a new end-user client.  It authenticates immediately, and returns an error if unable to.
func NewClient(tenant string, username string, password string, apikey string, region string, authurl string, private bool) (Client, error) {
	c := &userClient{
		client:   &http.Client{Timeout: 30 * time.Minute},
		tenant:   tenant,
		username: username,
		password: password,
		apikey:   apikey,
		region:   region,
		authurl:  authurl,
		private:  private,
	}
	if err := c.authenticate(); err != nil {
		return nil, err
	}
	return c, nil
}

// NewInsecureClient creates a new end-user client with SSL verification turned off.  It authenticates immediately, and returns an error if unable to.
func NewInsecureClient(tenant string, username string, password string, apikey string, region string, authurl string, private bool) (Client, error) {
	c := &userClient{
		client: &http.Client{
			Timeout: 30 * time.Minute,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		},
		tenant:   tenant,
		username: username,
		password: password,
		apikey:   apikey,
		region:   region,
		authurl:  authurl,
		private:  private,
	}
	if err := c.authenticate(); err != nil {
		return nil, err
	}
	return c, nil
}

package middleware

import (
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGrepObject(t *testing.T) {
	data := []byte("THIS IS LINE 1\nTHIS IS LINE 2\nTHIS IS LINE 3\nTHIS IS LINE 20\n")
	ts := httptest.NewServer(GrepObject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Hi", "There")
		w.WriteHeader(200)
		w.Write(data)
	})))
	defer ts.Close()

	req, _ := http.NewRequest("GREP", ts.URL+"?e=THIS", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	response, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, string(data), string(response))

	req, _ = http.NewRequest("GREP", ts.URL+"?e=2", nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	response, err = ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	require.Equal(t, "", res.Header.Get("X-Hi"))
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, "THIS IS LINE 2\nTHIS IS LINE 20\n", string(response))
}

func TestGrepPassNonGrep(t *testing.T) {
	data := []byte("THIS IS LINE 1\nTHIS IS LINE 2\nTHIS IS LINE 3\nTHIS IS LINE 20\n")
	ts := httptest.NewServer(GrepObject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Hi", "There")
		w.WriteHeader(200)
		w.Write(data)
	})))
	defer ts.Close()

	req, _ := http.NewRequest("GREP", ts.URL+"?e=THIS", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	response, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, string(data), string(response))

	req, _ = http.NewRequest("GET", ts.URL, nil)
	res, err = http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	response, err = ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	require.Equal(t, "There", res.Header.Get("X-Hi"))
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, "THIS IS LINE 1\nTHIS IS LINE 2\nTHIS IS LINE 3\nTHIS IS LINE 20\n", string(response))
}

func TestGrepObject404(t *testing.T) {
	ts := httptest.NewServer(GrepObject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})))
	defer ts.Close()
	req, _ := http.NewRequest("GREP", ts.URL+"?e=4", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	res.Body.Close()
	require.Equal(t, 404, res.StatusCode)
}

func TestGrepObjectGZ(t *testing.T) {
	data := []byte{'\x1f', '\x8b', '\x08', '\x08', '\x1f', '\x14', '\xde', 'U', '\x02', '\xff',
		'd', 'a', 't', 'a', '\x00', '\x0b', '\xf1', '\xf0', '\x0c', 'V', '\x00', '"', '\x1f',
		'O', '?', 'W', '\x05', 'C', '\xae', '\x10', 'd', '\xae', '\x11', '*', '\xd7', '\x98',
		'\x0b', '\x00', '\x97', 'V', '\x04', '\xc7', '-', '\x00', '\x00', '\x00'}
	ts := httptest.NewServer(GrepObject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(data)
	})))
	defer ts.Close()
	req, _ := http.NewRequest("GREP", ts.URL+"?e=2", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	response, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, "THIS IS LINE 2\n", string(response))
}

func TestGrepObjectBadGZ(t *testing.T) {
	data := []byte{'\x1f', '\x8b', 'X', 'X', 'X'}
	ts := httptest.NewServer(GrepObject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(data)
	})))
	defer ts.Close()
	req, _ := http.NewRequest("GREP", ts.URL+"?e=2", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	res.Body.Close()
	require.Equal(t, 500, res.StatusCode)
}

func TestGrepObjectBZ2(t *testing.T) {
	data := []byte{'B', 'Z', 'h', '9', '1', 'A', 'Y', '&', 'S', 'Y', '\x0c', '\x0e', '\x0b', '\x95',
		'\x00', '\x00', '\x15', '^', '\x00', '\x00', '\x10', '@', '\x00', '8', '\x00', '\x02', 'e',
		'\x0c', '\x00', ' ', '\x00', '!', '\xb5', 'C', '@', '\xf2', '\x10', '4', '\r', '\n', '\xd5',
		'l', 'h', '\xe0', '\xc8', 'A', '\x94', '\x88', '%', '\x0b', '\x17', 'r', 'E', '8', 'P', '\x90',
		'\x0c', '\x0e', '\x0b', '\x95'}
	ts := httptest.NewServer(GrepObject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.Write(data)
	})))
	defer ts.Close()
	req, _ := http.NewRequest("GREP", ts.URL+"?e=2", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	response, err := ioutil.ReadAll(res.Body)
	res.Body.Close()
	if err != nil {
		log.Fatal(err)
	}
	require.Equal(t, 200, res.StatusCode)
	require.Equal(t, "THIS IS LINE 2\n", string(response))
}

func TestGrepObjectInvalidRegex(t *testing.T) {
	ts := httptest.NewServer(GrepObject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})))
	defer ts.Close()
	req, _ := http.NewRequest("GREP", ts.URL+"?e=([|", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	res.Body.Close()
	require.Equal(t, 400, res.StatusCode)
}

func TestGrepObjectNoQuery(t *testing.T) {
	ts := httptest.NewServer(GrepObject(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
	})))
	defer ts.Close()
	req, _ := http.NewRequest("GREP", ts.URL, nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	res.Body.Close()
	require.Equal(t, 400, res.StatusCode)
}

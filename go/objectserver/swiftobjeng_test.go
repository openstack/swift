package objectserver

import (
	"bytes"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/stretchr/testify/require"
)

func TestSwiftObjectRoundtrip(t *testing.T) {
	driveRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(driveRoot)

	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "1"}
	swcon := &SwiftObjectFactory{driveRoot: driveRoot, hashPathPrefix: "prefix", hashPathSuffix: "suffix"}
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	require.Nil(t, err)
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	swo.Commit(map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"})

	swo, err = swcon.New(vars, true)
	require.Nil(t, err)
	defer swo.Close()
	metadata := swo.Metadata()
	require.Equal(t, map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"}, metadata)
	buf := &bytes.Buffer{}
	_, err = swo.Copy(buf)
	require.Nil(t, err)
	require.Equal(t, "!", buf.String())
}

func TestSwiftObjectFailAuditContentLengthWrong(t *testing.T) {
	driveRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(driveRoot)

	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "1"}
	swcon := &SwiftObjectFactory{driveRoot: driveRoot, hashPathPrefix: "prefix", hashPathSuffix: "suffix"}
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	swo.Commit(map[string]string{"Content-Length": "0", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"})

	swo, err = swcon.New(vars, false)
	require.NotNil(t, err)
}

func TestSwiftObjectFailAuditBadContentLength(t *testing.T) {
	driveRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(driveRoot)

	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "1"}
	swcon := &SwiftObjectFactory{driveRoot: driveRoot, hashPathPrefix: "prefix", hashPathSuffix: "suffix"}
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	swo.Commit(map[string]string{"Content-Length": "X", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"})

	swo, err = swcon.New(vars, false)
	require.NotNil(t, err)
}

func TestSwiftObjectQuarantine(t *testing.T) {
	driveRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(driveRoot)

	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "3"}
	swcon := &SwiftObjectFactory{driveRoot: driveRoot, hashPathPrefix: "prefix", hashPathSuffix: "suffix"}
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	swo.Commit(map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"})
	swo.Quarantine()
	require.True(t, hummingbird.Exists(filepath.Join(driveRoot, "sda", "quarantined")))
}

func TestSwiftObjectMultiCopy(t *testing.T) {
	driveRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(driveRoot)

	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "1"}
	swcon := &SwiftObjectFactory{driveRoot: driveRoot, hashPathPrefix: "prefix", hashPathSuffix: "suffix"}
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	swo.Commit(map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"})

	swo, err = swcon.New(vars, true)
	require.Nil(t, err)
	defer swo.Close()
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}
	_, err = swo.Copy(buf1, buf2)
	require.Nil(t, err)
	require.Equal(t, "!", buf1.String())
	require.Equal(t, "!", buf2.String())
}

func TestSwiftObjectDelete(t *testing.T) {
	driveRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(driveRoot)

	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "1"}
	swcon := &SwiftObjectFactory{driveRoot: driveRoot, hashPathPrefix: "prefix", hashPathSuffix: "suffix"}
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	swo.Commit(map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"})

	swo, err = swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	require.True(t, swo.Exists())
	err = swo.Delete(map[string]string{"X-Timestamp": "1234567891.123456"})
	require.Nil(t, err)

	swo, err = swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	require.False(t, swo.Exists())
}

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
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"os/user"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParseRange(t *testing.T) {
	//Setting up individual test data
	tests := []struct {
		rangeHeader string
		exError     string
		exRanges    []httpRange
	}{
		{"bytes=-0", "Zero end with no begin", nil},
		{"bytes=12346-123457", "Begin bigger than file", nil},
		{"bytes=12346-", "Begin bigger than file", nil},
		{" ", "", []httpRange{}},
		{"nonbytes=1-2", "", []httpRange{}},
		{"bytes=", "", []httpRange{}},
		{"bytes=-", "", []httpRange{}},
		{"bytes=-cv", "", []httpRange{}},
		{"bytes=cv-", "", []httpRange{}},
		{"bytes=-12346", "", []httpRange{httpRange{0, 12345}}},
		{"bytes=-12344", "", []httpRange{httpRange{1, 12345}}},
		{"bytes=12344-", "", []httpRange{httpRange{12344, 12345}}},
		{"bytes=12344-cv", "", []httpRange{}},
		{"bytes=13-12", "", []httpRange{}},
		{"bytes=cv-12345", "", []httpRange{}},
		{"bytes=12342-12343", "", []httpRange{httpRange{12342, 12344}}},
		{"bytes=12342-12344", "", []httpRange{httpRange{12342, 12345}}},
		{"bytes=0-1,2-3", "", []httpRange{httpRange{0, 2}, httpRange{2, 4}}},
		{"bytes=0-1,x-x", "", []httpRange{httpRange{0, 2}}},
		{"bytes=0-1,x-x,2-3", "", []httpRange{httpRange{0, 2}, httpRange{2, 4}}},
		{"bytes=0-1,x-", "", []httpRange{httpRange{0, 2}}},
		{"bytes=0-1,2-3,4-5", "", []httpRange{httpRange{0, 2}, httpRange{2, 4}, httpRange{4, 6}}},
	}

	//Run tests with data from above
	for _, test := range tests {
		result, err := ParseRange(test.rangeHeader, 12345)
		if test.rangeHeader == " " {
			assert.Nil(t, result)
			assert.Nil(t, err)
			continue
		}
		if test.exError == "" {
			assert.Nil(t, err)
		} else {
			assert.Equal(t, err.Error(), test.exError)
		}
		assert.Equal(t, len(result), len(test.exRanges))
		for i, _ := range result {
			assert.Equal(t, test.exRanges[i], result[i])
		}
	}
}

func TestParseDate(t *testing.T) {
	//Setup tests with individual data
	tests := []string{
		"Mon, 02 Jan 2006 15:04:05 MST",
		"Mon, 02 Jan 2006 15:04:05 -0700",
		"Mon Jan 02 15:04:05 2006",
		"Monday, 02-Jan-06 15:04:05 MST",
		"1136214245",
		"1136214245.1234",
		"2006-01-02 15:04:05",
	}

	//Run Tests from above
	for _, timestamp := range tests {
		timeResult, err := ParseDate(timestamp)
		if err == nil {
			assert.Equal(t, timeResult.Day(), 2)
			assert.Equal(t, timeResult.Month(), time.Month(1))
			assert.Equal(t, timeResult.Year(), 2006)
			assert.Equal(t, timeResult.Hour(), 15)
			assert.Equal(t, timeResult.Minute(), 4)
			assert.Equal(t, timeResult.Second(), 5)
		} else {
			assert.Equal(t, err.Error(), "invalid time")
		}
	}

}

func TestStandardizeTimestamp(t *testing.T) {
	//Setup tests with individual data
	tests := []struct {
		timestamp      string
		expectedResult string
	}{
		{"12345.12345", "0000012345.12345"},
		{"12345.1234", "0000012345.12340"},
		{"12345.1234_123455", "0000012345.12340_0000000000123455"},
		{"12345.12343_12345a", "0000012345.12343_000000000012345a"},
	}

	//Run Tests from above
	for _, test := range tests {
		result, _ := StandardizeTimestamp(test.timestamp)
		assert.Equal(t, test.expectedResult, result)
	}

}

func TestStandardizeTimestamp_invalidTimestamp(t *testing.T) {
	//Setup test data
	tests := []struct {
		timestamp string
		errorMsg  string
	}{
		{"invalidTimestamp", "Could not parse float from 'invalidTimestamp'."},
		{"1234.1234_invalidOffset", "Could not parse int from 'invalidOffset'."},
	}
	for _, test := range tests {
		_, err := StandardizeTimestamp(test.timestamp)
		assert.Equal(t, err.Error(), test.errorMsg)
	}
}

func TestGetEpochFromTimestamp(t *testing.T) {
	//Setup tests with individual data
	tests := []struct {
		timestamp      string
		expectedResult string
	}{
		{"12345.12345", "0000012345.12345"},
		{"12345.1234", "0000012345.12340"},
		{"12345.1234_123455", "0000012345.12340"},
		{"12345.12343_12345a", "0000012345.12343"},
	}

	//Run Tests from above
	for _, test := range tests {
		result, _ := GetEpochFromTimestamp(test.timestamp)
		assert.Equal(t, test.expectedResult, result)
	}
}

func TestGetEpochFromTimestamp_invalidTimestamp(t *testing.T) {
	_, err := GetEpochFromTimestamp("invalidTimestamp")
	assert.Equal(t, err.Error(), "Could not parse float from 'invalidTimestamp'.")
}

func TestParseTimestamp(t *testing.T) {
	tests := []string{
		"2006-01-02 15:04:05",
		"Mon, 02 Jan 2006 15:04:05 MST",
	}

	for _, timestamp := range tests {
		timeResult, err := FormatTimestamp(timestamp)
		if err != nil {
			assert.Equal(t, err.Error(), "invalid time")
			assert.Empty(t, timeResult)
		} else {
			assert.Equal(t, "2006-01-02T15:04:05", timeResult)
		}
	}
}

func TestLooksTrue(t *testing.T) {
	tests := []string{
		"true ", "true", "t", "yes", "y", "1", "on",
	}
	for _, test := range tests {
		assert.True(t, LooksTrue(test))
	}
}

func TestValidTimestamp(t *testing.T) {
	assert.True(t, ValidTimestamp("12345.12345"))
	assert.False(t, ValidTimestamp("12345"))
	assert.False(t, ValidTimestamp("your.face"))
}

func TestUrlencode(t *testing.T) {
	assert.True(t, Urlencode("HELLO%2FTHERE") == "HELLO%252FTHERE")
	assert.True(t, Urlencode("HELLOTHERE") == "HELLOTHERE")
	assert.True(t, Urlencode("HELLO THERE, YOU TWO//\x00\xFF") == "HELLO%20THERE%2C%20YOU%20TWO//%00%FF")
	assert.True(t, Urlencode("鐋댋") == "%E9%90%8B%EB%8C%8B")
}

func TestIniFile(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "INI")
	assert.Nil(t, err)
	defer os.RemoveAll(tempFile.Name())
	tempFile.WriteString("[stuff]\ntruevalue=true\nfalsevalue=false\nintvalue=3\nset log_facility = LOG_LOCAL1\n")
	iniFile, err := LoadIniFile(tempFile.Name())
	assert.Equal(t, true, iniFile.GetBool("stuff", "truevalue", false))
	assert.Equal(t, false, iniFile.GetBool("stuff", "falsevalue", true))
	assert.Equal(t, true, iniFile.GetBool("stuff", "defaultvalue", true))
	assert.Equal(t, int64(3), iniFile.GetInt("stuff", "intvalue", 2))
	assert.Equal(t, int64(2), iniFile.GetInt("stuff", "missingvalue", 2))
	assert.Equal(t, "false", iniFile.GetDefault("stuff", "falsevalue", "true"))
	assert.Equal(t, "true", iniFile.GetDefault("stuff", "missingvalue", "true"))
	assert.Equal(t, "LOG_LOCAL1", iniFile.GetDefault("stuff", "log_facility", "LOG_LOCAL0"))
}

func TestIsMount(t *testing.T) {
	isMount, err := IsMount("/proc")
	assert.Nil(t, err)
	assert.True(t, isMount)
	isMount, err = IsMount(".")
	assert.Nil(t, err)
	assert.False(t, isMount)
	isMount, err = IsMount("/slartibartfast")
	assert.NotNil(t, err)
}

func TestIsNotDir(t *testing.T) {
	tempFile, _ := ioutil.TempFile("", "INI")
	defer os.RemoveAll(tempFile.Name())
	_, err := ioutil.ReadDir(tempFile.Name())
	assert.True(t, IsNotDir(err))
	_, err = ioutil.ReadDir("/aseagullstolemysailorhat")
	assert.True(t, IsNotDir(err))
}

func TestCopy(t *testing.T) {
	src := bytes.NewBuffer([]byte("WELL HELLO THERE"))
	dst1 := &bytes.Buffer{}
	dst2 := &bytes.Buffer{}
	Copy(src, dst1, dst2)
	assert.Equal(t, []byte("WELL HELLO THERE"), dst1.Bytes())
	assert.Equal(t, []byte("WELL HELLO THERE"), dst2.Bytes())
}

func TestCopyN(t *testing.T) {
	src := bytes.NewBuffer([]byte("WELL HELLO THERE"))
	dst1 := &bytes.Buffer{}
	dst2 := &bytes.Buffer{}
	CopyN(src, 10, dst1, dst2)
	assert.Equal(t, []byte("WELL HELLO"), dst1.Bytes())
	assert.Equal(t, []byte("WELL HELLO"), dst2.Bytes())
}

func TestWriteFileAtomic(t *testing.T) {
	tempFile, _ := ioutil.TempFile("", "INI")
	defer os.RemoveAll(tempFile.Name())
	WriteFileAtomic(tempFile.Name(), []byte("HI THERE"), 0600)
	fi, err := os.Stat(tempFile.Name())
	assert.Nil(t, err)
	assert.Equal(t, 8, int(fi.Size()))
}

func TestReadDirNames(t *testing.T) {
	tempDir, _ := ioutil.TempDir("", "RDN")
	defer os.RemoveAll(tempDir)
	ioutil.WriteFile(tempDir+"/Z", []byte{}, 0666)
	ioutil.WriteFile(tempDir+"/X", []byte{}, 0666)
	ioutil.WriteFile(tempDir+"/Y", []byte{}, 0666)
	fileNames, err := ReadDirNames(tempDir)
	assert.Nil(t, err)
	assert.Equal(t, fileNames, []string{"X", "Y", "Z"})
}

func TestGetHashPrefixAndSuffix(t *testing.T) {
	_, suffix, err := GetHashPrefixAndSuffix()
	assert.Nil(t, err, "Error getting hash path prefix or suffix")

	if suffix == "" {
		t.Error("Error prefix and suffix not being set")
	}
}

func TestUidFromConf(t *testing.T) {
	usr, err := user.Current()
	assert.Nil(t, err)
	tempFile, err := ioutil.TempFile("", "INI")
	assert.Nil(t, err)
	defer os.RemoveAll(tempFile.Name())
	defer tempFile.Close()
	fmt.Fprintf(tempFile, "[DEFAULT]\nuser=%s\n", usr.Username)

	currentUid, err := strconv.ParseUint(usr.Uid, 10, 32)
	assert.Nil(t, err)
	currentGid, err := strconv.ParseUint(usr.Gid, 10, 32)
	assert.Nil(t, err)
	uid, gid, err := UidFromConf(tempFile.Name())
	assert.Nil(t, err)
	assert.Equal(t, uint32(currentUid), uint32(uid))
	assert.Equal(t, uint32(currentGid), uint32(gid))
}

func TestUidFromConfFailure(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "INI")
	assert.Nil(t, err)
	defer os.RemoveAll(tempFile.Name())
	defer tempFile.Close()
	fmt.Fprintf(tempFile, "[DEFAULT]\nuser=SomeUserWhoShouldntExist\n")
	_, _, err = UidFromConf(tempFile.Name())
	assert.NotNil(t, err)
}

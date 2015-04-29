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
	"testing"

	"github.com/stretchr/testify/assert"
)

func BenchmarkPickle(b *testing.B) {
	d := map[string]string{
		"Content-Length": "65536", "Content-Type": "application/octet-stream", "ETag": "fcd6bcb56c1689fcef28b57c22475bad",
		"X-Timestamp": "1422766779.57463", "name": "/someaccountname/somecontainername/5821142269423797100"}
	for i := 0; i < b.N; i++ {
		PickleDumps(d)
	}
}

func BenchmarkUnpickle(b *testing.B) {
	pickled := PickleDumps(map[string]string{
		"Content-Length": "65536", "Content-Type": "application/octet-stream", "ETag": "fcd6bcb56c1689fcef28b57c22475bad",
		"X-Timestamp": "1422766779.57463", "name": "/someaccountname/somecontainername/5821142269423797100"})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		PickleLoads(pickled)
	}
}

func BenchmarkUnpicklePythoned(b *testing.B) {
	// This is what cPickle makes of the data structure in the other tests.
	// It's not a very efficient pickle, but all of our existing data looks like this.
	pythoned := []byte("\x80\x02}q\x01(U\x0bX-TimestampU\x101422766779.57463U\x0eContent-LengthU\x0565536U\x04ETag" +
		"U fcd6bcb56c1689fcef28b57c22475badU\x0cContent-TypeU\x18application/octet-streamU\x04nameq\x02" +
		"U6/someaccountname/somecontainername/5821142269423797100u.")
	for i := 0; i < b.N; i++ {
		PickleLoads(pythoned)
	}
}

func TestUnpicklingVersion1Map(t *testing.T) {
	data, err := PickleLoads([]byte("(dp1\nS'hi'\np2\nS'there'\np3\ns."))
	assert.Nil(t, err)
	dataVal, ok := data.(map[interface{}]interface{})
	assert.True(t, ok)
	assert.Equal(t, "there", dataVal["hi"])
}

func TestUnpicklingVersion2Map(t *testing.T) {
	data, err := PickleLoads([]byte("\x80\x02}q\x01U\x02hiq\x02U\x05thereq\x03s."))
	assert.Nil(t, err)
	dataVal, ok := data.(map[interface{}]interface{})
	assert.True(t, ok)
	assert.Equal(t, "there", dataVal["hi"])
}

func PickleRoundTrip(t *testing.T, v interface{}) interface{} {
	ret, err := PickleLoads(PickleDumps(v))
	assert.Nil(t, err)
	return ret
}

func TestPickleInt(t *testing.T) {
	testCases := []int64{2, 1 << 10, 1 << 40, -1, 0 - (1 << 40)}
	for _, testCase := range testCases {
		dataVal, ok := PickleRoundTrip(t, testCase).(int64)
		assert.True(t, ok)
		assert.Equal(t, testCase, dataVal)
	}
}

func TestPickleIntTypes(t *testing.T) {
	testCases := []interface{}{uint(8), int16(8), uint16(8), int32(8), uint32(8), int64(8), uint64(8)}
	for _, testCase := range testCases {
		dataVal, ok := PickleRoundTrip(t, testCase).(int64)
		assert.True(t, ok)
		assert.Equal(t, int64(8), dataVal)
	}
}

func TestPickleFloat32(t *testing.T) {
	dataVal, ok := PickleRoundTrip(t, float32(3.14159)).(float64)
	assert.True(t, ok)
	assert.Equal(t, int64(31415), int64(dataVal*10000))
}

func TestPickleFloat64(t *testing.T) {
	dataVal, ok := PickleRoundTrip(t, 3.14159).(float64)
	assert.True(t, ok)
	assert.Equal(t, int64(31415), int64(dataVal*10000))
}

func TestPickleString(t *testing.T) {
	dataVal, ok := PickleRoundTrip(t, "hi").(string)
	assert.True(t, ok)
	assert.Equal(t, "hi", dataVal)
}

func TestPickleLongString(t *testing.T) {
	longString := string(make([]byte, 1024))
	dataVal, ok := PickleRoundTrip(t, longString).(string)
	assert.True(t, ok)
	assert.Equal(t, longString, dataVal)
}

func TestPickleBool(t *testing.T) {
	dataVal, ok := PickleRoundTrip(t, true).(bool)
	assert.True(t, ok)
	assert.Equal(t, true, dataVal)

	dataVal, ok = PickleRoundTrip(t, false).(bool)
	assert.True(t, ok)
	assert.Equal(t, false, dataVal)
}

func TestPickleMapStringString(t *testing.T) {
	data := map[string]string{"1": "test1", "2": "test2"}
	dataVal, ok := PickleRoundTrip(t, data).(map[interface{}]interface{})
	assert.True(t, ok)
	assert.Equal(t, "test1", dataVal["1"])
	assert.Equal(t, "test2", dataVal["2"])
}

func TestPickleMapInterfaceInterface(t *testing.T) {
	data := map[interface{}]interface{}{"1": "test1", "2": "test2"}
	dataVal, ok := PickleRoundTrip(t, data).(map[interface{}]interface{})
	assert.True(t, ok)
	assert.Equal(t, "test1", dataVal["1"])
	assert.Equal(t, "test2", dataVal["2"])
}

func TestPickleMapStringInterface(t *testing.T) {
	data := map[string]interface{}{"1": "test1", "2": "test2"}
	dataVal, ok := PickleRoundTrip(t, data).(map[interface{}]interface{})
	assert.True(t, ok)
	assert.Equal(t, "test1", dataVal["1"])
	assert.Equal(t, "test2", dataVal["2"])
}

func TestPickleMapIntString(t *testing.T) {
	data := map[int]string{1: "test1", 2: "test2"}
	dataVal, ok := PickleRoundTrip(t, data).(map[interface{}]interface{})
	assert.True(t, ok)
	assert.Equal(t, "test1", dataVal[int64(1)])
	assert.Equal(t, "test2", dataVal[int64(2)])
}

func TestPickleSliceInterface(t *testing.T) {
	data := []interface{}{1, 2, 3}
	dataVal, ok := PickleRoundTrip(t, data).([]interface{})
	assert.True(t, ok)
	assert.Equal(t, int64(1), dataVal[0])
	assert.Equal(t, int64(2), dataVal[1])
	assert.Equal(t, int64(3), dataVal[2])
}

func TestPickleNil(t *testing.T) {
	assert.Nil(t, PickleRoundTrip(t, nil))
}

func TestPythonString(t *testing.T) {
	testCases := []struct {
		src, expect string
	}{
		{"\"hi there\"", "hi there"},
		{"\"hi \\' there\"", "hi ' there"},
		{"\"hi \\\" there\"", "hi \" there"},
		{"'hi \" there'", "hi \" there"},
		{"'hi \\\" there'", "hi \" there"},
		{"'hi \\\\\" there'", "hi \\\" there"},
		{"'hi \\' there'", "hi ' there"},
		{"'hi \\\\\\' there'", "hi \\' there"},
		{"'hi there\\\\'", "hi there\\"},
	}
	for _, testCase := range testCases {
		str, err := pythonString(testCase.src)
		assert.Nil(t, err)
		assert.Equal(t, testCase.expect, str)
	}

	failCases := []string{
		"",
		"hi",
		"'hi \\\\' there'",
		"'hi \\\\\\\\' there'",
	}
	for _, testCase := range failCases {
		_, err := pythonString(testCase)
		assert.NotNil(t, err)
	}
}

func TestUnpickleBigPickle(t *testing.T) {
	// just to grow the stack beyond its default
	v, err := PickleLoads([]byte("\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88\x88."))
	assert.Nil(t, err)
	assert.Equal(t, true, v.(bool))
}

func TestUnpickleBool(t *testing.T) {
	v, err := PickleLoads([]byte("\x88."))
	assert.Nil(t, err)
	assert.Equal(t, true, v.(bool))
	v, err = PickleLoads([]byte("\x89."))
	assert.Nil(t, err)
	assert.Equal(t, false, v.(bool))
}

func TestUnpickleTuple1(t *testing.T) {
	v, err := PickleLoads([]byte("\x88\x85."))
	tuple := v.([]interface{})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(tuple))
	assert.Equal(t, true, tuple[0])
}

func TestUnpickleTuple2(t *testing.T) {
	v, err := PickleLoads([]byte("\x88\x89\x86."))
	tuple := v.([]interface{})
	assert.Nil(t, err)
	assert.Equal(t, 2, len(tuple))
	assert.Equal(t, true, tuple[0])
	assert.Equal(t, false, tuple[1])
}

func TestUnpickleTuple3(t *testing.T) {
	v, err := PickleLoads([]byte("K\x00K\x01K\x02\x87."))
	tuple := v.([]interface{})
	assert.Nil(t, err)
	assert.Equal(t, 3, len(tuple))
	assert.Equal(t, int64(0), tuple[0])
	assert.Equal(t, int64(1), tuple[1])
	assert.Equal(t, int64(2), tuple[2])
}

func TestUnpickleDup(t *testing.T) {
	v, err := PickleLoads([]byte("K\x002\x86."))
	tuple := v.([]interface{})
	if err != nil || len(tuple) != 2 || tuple[0].(int64) != 0 || tuple[1].(int64) != 0 {
		t.Fatal("Return data not correct.")
	}
}

func TestUnpicklePop(t *testing.T) {
	v, err := PickleLoads([]byte("K\x00K\x010."))
	assert.Nil(t, err)
	assert.Equal(t, int64(0), v)
}

func TestUnpickleOldInt(t *testing.T) {
	v, err := PickleLoads([]byte("I12345\n."))
	assert.Nil(t, err)
	assert.Equal(t, int64(12345), v)

	v, err = PickleLoads([]byte("L12345\n."))
	assert.Nil(t, err)
	assert.Equal(t, int64(12345), v)
}

func TestUnpickleOldFloat(t *testing.T) {
	v, err := PickleLoads([]byte("F3.14159\n."))
	assert.Nil(t, err)
	assert.Equal(t, int64(31415), int64(v.(float64)*10000))
}

func TestUnpickleOldStupidMap(t *testing.T) {
	v, _ := PickleLoads([]byte("}(K\x00K\x01K\x02K\x03u."))
	m := v.(map[interface{}]interface{})
	assert.Equal(t, 2, len(m))
	assert.Equal(t, int64(1), m[int64(0)])
	assert.Equal(t, int64(3), m[int64(2)])
}

func TestUnpickleOldStupidList(t *testing.T) {
	v, _ := PickleLoads([]byte("]K\xffa."))
	l := v.([]interface{})
	assert.Equal(t, 1, len(l))
	assert.Equal(t, int64(255), l[0])
}

func TestUnpickleListAppends(t *testing.T) {
	v, _ := PickleLoads([]byte("](K\x01K\x02K\x03e."))
	l := v.([]interface{})
	assert.Equal(t, 3, len(l))
	assert.Equal(t, int64(1), l[0])
	assert.Equal(t, int64(2), l[1])
	assert.Equal(t, int64(3), l[2])
}

func TestUnpicklePopMark(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFF(K\x01K\x02K\x031."))
	assert.Equal(t, int64(255), v)
}

func TestGetPutMemo(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFFp5\nK\x00g5\n."))
	assert.Equal(t, int64(255), v)
}

func TestBinGetPutMemo(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFFq\x05K\x00h\x05."))
	assert.Equal(t, int64(255), v)
}

func TestLongbinGetPutMemo(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFFr1234K\x00j1234."))
	assert.Equal(t, int64(255), v)
}

func TestMixedGetPutMemo(t *testing.T) {
	v, _ := PickleLoads([]byte("K\xFFq\x05K\x00g5\n.")) // binary put non-binary get
	assert.Equal(t, int64(255), v)

	v, _ = PickleLoads([]byte("K\xFFp5\nK\x00h\x05.")) // non-binary put binary get
	assert.Equal(t, int64(255), v)

	v, _ = PickleLoads([]byte("K\xFFr\x05\x00\x00\x00K\x00g5\n.")) // longbin put non-binary get
	assert.Equal(t, int64(255), v)

	v, _ = PickleLoads([]byte("K\xFFp5\nK\x00j\x05\x00\x00\x00.")) // non-binary put longbin get
	assert.Equal(t, int64(255), v)
}

func TestCustomTypes(t *testing.T) {
	type MyString string
	sDataVal := PickleRoundTrip(t, MyString("hi"))
	assert.Equal(t, "hi", sDataVal)

	type MyInt int
	iDataVal := PickleRoundTrip(t, MyInt(3))
	assert.Equal(t, int64(3), iDataVal)
}

func TestShortPickles(t *testing.T) {
	// short pickles that are invalid for various reasons, hits a bunch of error cases.
	// Mostly opcodes without data, or asking to pop an empty stack.
	tests := []string{"", "S", "U", "U1", "T", "T1234", "I", "Iabc\n", "F", "Fabc\n", "K", "0", ".", "2",
		"u", "a", "e", "\x87", "\x85", "\x86", "M", "J", "\x8a", "G", "p", "px\n", "g", "gx\n", "h", "j",
		"r", "q", "?", "SX\n", "\x8a\x01", "s", "Ns", "r....", "q.", "p1\n", "NNs", "(", "!"}
	for _, test := range tests {
		_, err := PickleLoads([]byte(test))
		assert.NotNil(t, err)
	}
}

func TestPickleUnpicklelablePanics(t *testing.T) {
	catchFunc := func() {
		e := recover()
		assert.NotNil(t, e)
	}
	defer catchFunc()
	PickleDumps(catchFunc) // can't pickle a function.
	t.Fatal("I shouldn't make it here.")
}

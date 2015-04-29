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
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
)

func picklestring(val string, buf *bytes.Buffer, scratch []byte) {
	length := len(val)
	if length < 256 {
		buf.WriteByte('U') // SHORT_BINSTRING
		buf.WriteByte(byte(length))
		buf.WriteString(val)
	} else {
		scratch[0] = 'T' // BINSTRING
		binary.LittleEndian.PutUint32(scratch[1:5], uint32(length))
		buf.Write(scratch[0:5])
		buf.WriteString(val)
	}
}

func pickleint(val int64, buf *bytes.Buffer, scratch []byte) {
	if int64(uint8(val)) == val {
		buf.WriteByte('K') // BININT1
		buf.WriteByte(byte(val))
	} else if int64(uint16(val)) == val {
		scratch[0] = 'M' // BININT2
		binary.LittleEndian.PutUint16(scratch[1:3], uint16(val))
		buf.Write(scratch[0:3])
	} else if int64(int32(val)) == val {
		scratch[0] = 'J' // BININT
		binary.LittleEndian.PutUint32(scratch[1:5], uint32(val))
		buf.Write(scratch[0:5])
	} else {
		scratch[0] = '\x8a' // LONG1
		scratch[1] = 8      // 8 bytes
		binary.LittleEndian.PutUint64(scratch[2:10], uint64(val))
		buf.Write(scratch[0:10])
	}
}

func pickleobj(o interface{}, buf *bytes.Buffer, scratch []byte) {
	v := reflect.ValueOf(o)
	switch v.Kind() {
	case reflect.Invalid:
		buf.WriteByte('N') // NONE
	case reflect.Bool:
		if v.Bool() {
			buf.WriteByte('\x88') // NEWTRUE
		} else {
			buf.WriteByte('\x89') // NEWFALSE
		}
	case reflect.String:
		picklestring(v.String(), buf, scratch)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		pickleint(v.Int(), buf, scratch)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		pickleint(int64(v.Uint()), buf, scratch)
	case reflect.Float32, reflect.Float64:
		scratch[0] = 'G' // BINFLOAT
		bits := math.Float64bits(v.Float())
		binary.LittleEndian.PutUint64(scratch[1:9], bits)
		buf.Write(scratch[0:9])
	case reflect.Slice, reflect.Array:
		buf.WriteByte('(') // MARK
		for i := 0; i < v.Len(); i++ {
			pickleobj(v.Index(i).Interface(), buf, scratch)
		}
		buf.WriteByte('l') // LIST
	case reflect.Map:
		buf.WriteByte('(') // MARK
		// Using "range" is way faster than iterating over maps with reflection,
		// but it's verbose and has to be a type we know ahead of time.
		switch o := o.(type) {
		case map[string]string:
			for k, v := range o {
				picklestring(k, buf, scratch)
				picklestring(v, buf, scratch)
			}
		case map[string]interface{}:
			for k, v := range o {
				picklestring(k, buf, scratch)
				pickleobj(v, buf, scratch)
			}
		case map[interface{}]interface{}:
			for k, v := range o {
				pickleobj(k, buf, scratch)
				pickleobj(v, buf, scratch)
			}
		default:
			for _, k := range v.MapKeys() {
				pickleobj(k.Interface(), buf, scratch)
				pickleobj(v.MapIndex(k).Interface(), buf, scratch)
			}
		}
		buf.WriteByte('d') // DICT
	default:
		panic(fmt.Sprintf("Unknown object type in pickle: %v", v))
	}
}

func PickleDumps(o interface{}) []byte {
	buf := &bytes.Buffer{}
	buf.WriteByte('\x80') // PROTO
	buf.WriteByte(2)      // Protocol 2
	scratch := make([]byte, 10)
	pickleobj(o, buf, scratch)
	buf.WriteByte('.')
	return buf.Bytes()
}

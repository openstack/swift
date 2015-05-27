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
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

var markster = "HI, I'M MARK!"
var mark = interface{}(&markster)

type unpickleState struct {
	stack      []interface{}
	top        int
	data       []byte
	dataOffset int
	memoKey    []int
	memoVal    []interface{}
}

func (s *unpickleState) push(item interface{}) {
	if len(s.stack) < s.top+1 {
		s.stack = append(s.stack, item)
	} else {
		s.stack[s.top] = item
	}
	s.top++
}

func (s *unpickleState) pop() (interface{}, error) {
	if s.top <= 0 {
		return nil, errors.New("empty stack")
	}
	s.top -= 1
	return s.stack[s.top], nil
}

func (s *unpickleState) peek() (interface{}, error) {
	if s.top <= 0 {
		return nil, errors.New("empty stack")
	}
	return s.stack[s.top-1], nil
}

func (s *unpickleState) setMark() {
	s.push(mark)
}

func (s *unpickleState) mark() []interface{} {
	start := s.top
	for s.top > 0 && s.stack[s.top-1] != mark {
		s.top--
	}
	s.top--
	return s.stack[s.top+1 : start]
}

func (s *unpickleState) readByte() (byte, error) {
	if s.dataOffset >= len(s.data) {
		return 0, io.EOF
	}
	c := s.data[s.dataOffset]
	s.dataOffset++
	return c, nil
}

func (s *unpickleState) readBytes(length int) ([]byte, error) {
	if len(s.data)-s.dataOffset < length {
		return nil, io.EOF
	}
	retval := s.data[s.dataOffset : s.dataOffset+length]
	s.dataOffset += length
	return retval, nil
}

func (s *unpickleState) readString(delim byte) (string, error) {
	offset := bytes.IndexByte(s.data[s.dataOffset:], delim)
	if offset == -1 {
		return "", io.EOF
	}
	retval := string(s.data[s.dataOffset : s.dataOffset+offset])
	s.dataOffset += offset + 1
	return retval, nil
}

func (s *unpickleState) readUint64() (uint64, error) {
	if len(s.data)-s.dataOffset < 8 {
		return 0, io.EOF
	}
	r := uint64(s.data[s.dataOffset]) | uint64(s.data[s.dataOffset+1])<<8 | uint64(s.data[s.dataOffset+2])<<16 | uint64(s.data[s.dataOffset+3])<<24 |
		uint64(s.data[s.dataOffset+4])<<32 | uint64(s.data[s.dataOffset+5])<<40 | uint64(s.data[s.dataOffset+6])<<48 | uint64(s.data[s.dataOffset+7])<<56
	s.dataOffset += 8
	return r, nil
}

func (s *unpickleState) readUint32() (uint32, error) {
	if len(s.data)-s.dataOffset < 4 {
		return 0, io.EOF
	}
	r := uint32(s.data[s.dataOffset]) | uint32(s.data[s.dataOffset+1])<<8 | uint32(s.data[s.dataOffset+2])<<16 | uint32(s.data[s.dataOffset+3])<<24
	s.dataOffset += 4
	return r, nil
}

func (s *unpickleState) readUint16() (uint16, error) {
	if len(s.data)-s.dataOffset < 2 {
		return 0, io.EOF
	}
	r := uint16(s.data[s.dataOffset]) | uint16(s.data[s.dataOffset+1])<<8
	s.dataOffset += 2
	return r, nil
}

func (s *unpickleState) getMemo(m int) interface{} {
	for i, key := range s.memoKey {
		if key == m {
			return s.memoVal[i]
		}
	}
	return nil
}

func (s *unpickleState) setMemo(i int, v interface{}) {
	s.memoKey = append(s.memoKey, i)
	s.memoVal = append(s.memoVal, v)
}

func newState(initialSize int, data []byte) *unpickleState {
	return &unpickleState{stack: make([]interface{}, initialSize), top: 0, data: data, dataOffset: 0}
}

// attempt to convert python string representations to golang string
// basically this should return the same thing as eval(src) in python.
func pythonString(src string) (string, error) {
	if len(src) < 2 || src[0] != src[len(src)-1] || (src[0] != '\'' && src[0] != '"') {
		return "", errors.New("invalid syntax")
	}
	quote := src[0]
	src = src[1 : len(src)-1]
	dst := []byte{'"'}
	backslashes := 0
	for _, c := range []byte(src) {
		switch c {
		case '\\':
			backslashes++
			continue
		case '"':
			if quote == '\'' && backslashes%2 == 0 {
				backslashes++
			}
		case '\'':
			if backslashes%2 == 1 {
				backslashes--
			} else if quote == '\'' {
				return "", errors.New("invalid syntax")
			}
		}
		for ; backslashes > 0; backslashes-- {
			dst = append(dst, '\\')
		}
		dst = append(dst, c)
	}
	for ; backslashes > 0; backslashes-- {
		dst = append(dst, '\\')
	}
	dst = append(dst, '"')
	return strconv.Unquote(string(dst))
}

func PickleLoads(data []byte) (interface{}, error) {
	state := newState(16, data)
	for op, err := state.readByte(); err == nil; op, err = state.readByte() {
		switch op {
		case '\x80': // PROTO
			state.readByte()
		case '(': // MARK
			state.setMark()
		case '.': // STOP
			top, err := state.pop()
			if err != nil {
				return nil, errors.New("Incomplete pickle (STOP): " + err.Error())
			}
			return top, nil
		case '0': // POP
			if _, err := state.pop(); err != nil {
				return nil, errors.New("Incomplete pickle (POP): " + err.Error())
			}
		case '1': // POP_MARK
			state.mark()
		case '2': // DUP
			top, err := state.peek()
			if err != nil {
				return nil, errors.New("Incomplete pickle (POP): " + err.Error())
			}
			state.push(top)
		case '\x88': // NEWTRUE
			state.push(true)
		case '\x89': // NEWFALSE
			state.push(false)
		case 'N': // NONE
			state.push(nil)

		case 'S', 'V': // STRING, UNICODE
			val, err := state.readString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (STRING): " + err.Error())
			}
			str, err := pythonString(val)
			if err != nil {
				return nil, errors.New("Unable to interpret Python string (STRING): " + err.Error())
			}
			state.push(str)
		case 'U': //SHORT_BINSTRING
			length, err := state.readByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (SHORT_BINSTRING): " + err.Error())
			}
			str, err := state.readBytes(int(length))
			if err != nil {
				return nil, errors.New("Incomplete pickle (SHORT_BINSTRING): " + err.Error())
			}
			state.push(string(str))
		case 'T', 'X': // BINUNICODE, BINSTRING
			length, err := state.readUint32()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINSTRING): " + err.Error())
			}
			str, err := state.readBytes(int(length))
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINSTRING): " + err.Error())
			}
			state.push(string(str))

		case 's': // SETITEM
			val, err1 := state.pop()
			key, err2 := state.pop()
			if err1 != nil || err2 != nil {
				return nil, errors.New("Incomplete pickle (SETITEM): stack empty")
			}
			top, err := state.peek()
			if err != nil {
				return nil, errors.New("Invalid pickle (SETITEM): stack empty")
			}
			d, ok := top.(map[interface{}]interface{})
			if !ok {
				return nil, errors.New("Incomplete pickle (SETITEM): stack top isn't a map")
			}
			d[key] = val
		case 'u': // SETITEMS
			vals := state.mark()
			top, err := state.peek()
			if err != nil {
				return nil, errors.New("Invalid pickle (SETITEMS): stack empty")
			}
			dict, ok := top.(map[interface{}]interface{})
			if !ok {
				return nil, errors.New("Incomplete pickle (SETITEMS): stack top isn't a map")
			}
			for j := 0; j < len(vals); j += 2 {
				dict[vals[j]] = vals[j+1]
			}

		case '}': // EMPTY_DICT
			state.push(make(map[interface{}]interface{}, 5))
		case 'd': // DICT
			vals := state.mark()
			dict := make(map[interface{}]interface{}, len(vals)/2)
			for j := 0; j < len(vals); j += 2 {
				dict[vals[j]] = vals[j+1]
			}
			state.push(dict)
		case ']', ')': // EMPTY_LIST, EMPTY_TUPLE
			state.push(make([]interface{}, 0))
		case 'l', 't': // LIST, TUPLE
			state.push(state.mark())
		case 'a': // APPEND
			value, err1 := state.pop()
			list, err2 := state.pop()
			if err1 != nil || err2 != nil {
				return nil, errors.New("Incomplete pickle (APPEND): stack empty")
			}
			state.push(append(list.([]interface{}), value))
		case 'e': // APPENDS
			items := state.mark()
			top, err := state.pop()
			if err != nil {
				return nil, errors.New("Invalid pickle (APPENDS): stack empty")
			}
			l, ok := top.([]interface{})
			if !ok {
				return nil, errors.New("Incomplete pickle (APPENDS): stack top isn't a list")
			}
			state.push(append(l, items...))
		case '\x85': // TUPLE1
			top, err := state.pop()
			if err != nil {
				return nil, errors.New("Incomplete pickle (TUPLE1): stack empty")
			}
			state.push([]interface{}{top})
		case '\x86': // TUPLE2
			a, err1 := state.pop()
			b, err2 := state.pop()
			if err1 != nil || err2 != nil {
				return nil, errors.New("Incomplete pickle (TUPLE2): stack empty")
			}
			state.push([]interface{}{b, a})
		case '\x87': // TUPLE3
			a, err1 := state.pop()
			b, err2 := state.pop()
			c, err3 := state.pop()
			if err1 != nil || err2 != nil || err3 != nil {
				return nil, errors.New("Incomplete pickle (TUPLE3): stack empty")
			}
			state.push([]interface{}{c, b, a})

		case 'I', 'L': // INT, LONG
			line, err := state.readString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (INT): " + err.Error())
			}
			val, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return nil, errors.New("Invalid pickle (INT): " + err.Error())
			}
			state.push(val)
		case 'F': // FLOAT
			line, err := state.readString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (FLOAT): " + err.Error())
			}
			val, err := strconv.ParseFloat(line, 64)
			if err != nil {
				return nil, errors.New("Invalid pickle (FLOAT): " + err.Error())
			}
			state.push(val)
		case 'K': // BININT1
			val, err := state.readByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BININT1): " + err.Error())
			}
			state.push(int64(val))
		case 'M': // BININT2
			val, err := state.readUint16()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BININT2): " + err.Error())
			}
			state.push(int64(val))
		case 'J': // BININT
			val, err := state.readUint32()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BININT): " + err.Error())
			}
			state.push(int64(int32(val)))
		case '\x8a': // LONG1
			length, err := state.readByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (LONG1): " + err.Error())
			}
			val := int64(0)
			if length > 0 {
				valb, err := state.readBytes(int(length))
				if err != nil {
					return nil, errors.New("Incomplete pickle (LONG1): " + err.Error())
				}
				for i, d := range valb {
					val |= (int64(d) << uint64(i*8))
				}
				if valb[len(valb)-1] >= '\x80' {
					val -= int64(1) << uint64(length*8)
				}
			}
			state.push(val)
		case 'G': // BINFLOAT
			val, err := state.readUint64()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINFLOAT): " + err.Error())
			}
			state.push(math.Float64frombits(val))

		case 'p': // PUT
			line, err := state.readString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (PUT): " + err.Error())
			}
			id, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return nil, errors.New("Invalid pickle (PUT): " + err.Error())
			}
			top, err := state.peek()
			if err != nil {
				return nil, errors.New("Invalid pickle (PUT): " + err.Error())
			}
			state.setMemo(int(id), top)
		case 'g': // GET
			line, err := state.readString('\n')
			if err != nil {
				return nil, errors.New("Incomplete pickle (GET): " + err.Error())
			}
			id, err := strconv.ParseInt(line, 10, 64)
			if err != nil {
				return nil, errors.New("Invalid pickle (GET): " + err.Error())
			}
			state.push(state.getMemo(int(id)))
		case 'q': // BINPUT
			id, err := state.readByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINPUT): " + err.Error())
			}
			top, err := state.peek()
			if err != nil {
				return nil, errors.New("Invalid pickle (PUT): " + err.Error())
			}
			state.setMemo(int(id), top)
		case 'h': // BINGET
			id, err := state.readByte()
			if err != nil {
				return nil, errors.New("Incomplete pickle (BINGET): " + err.Error())
			}
			state.push(state.getMemo(int(id)))
		case 'j': // LONG_BINGET
			id, err := state.readUint32()
			if err != nil {
				return nil, errors.New("Incomplete pickle (LONG_BINGET): " + err.Error())
			}
			state.push(state.getMemo(int(id)))
		case 'r': // LONG_BINPUT
			id, err := state.readUint32()
			if err != nil {
				return nil, errors.New("Incomplete pickle (LONG_BINPUT): " + err.Error())
			}
			top, err := state.peek()
			if err != nil {
				return nil, errors.New("Invalid pickle (PUT): " + err.Error())
			}
			state.setMemo(int(id), top)
		default:
			return nil, errors.New(fmt.Sprintf("Unknown pickle opcode: %c (%x)\n", op, op))
		}
	}
	return nil, errors.New("Incomplete pickle: fell out of loop")
}

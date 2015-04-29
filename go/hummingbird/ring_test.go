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

func TestGetRing(t *testing.T) {
	tests := []struct {
		prefix    string
		suffix    string
		ring_type string
	}{
		{"t", "t", "test"},
		{"t", "t", "object"},
		{"t", "t", "account"},
		{"t", "t", "container"},
	}

	for _, test := range tests {
		ring, err := GetRing(test.ring_type, test.prefix, test.suffix)
		if test.ring_type == "test" {
			assert.Equal(t, "Error loading test ring", err.Error())
		} else {
			assert.NotNil(t, ring)
		}
	}

}

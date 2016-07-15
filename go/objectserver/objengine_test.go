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

package objectserver

import (
	"errors"
	"flag"
	"testing"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/stretchr/testify/require"
)

func TestObjectEngineRegistry(t *testing.T) {
	testErr := errors.New("Not implemented")
	constructor := func(hummingbird.Config, *hummingbird.Policy, *flag.FlagSet) (ObjectEngine, error) {
		return nil, testErr
	}

	RegisterObjectEngine("test", constructor)

	fconstructor, err := FindEngine("test")
	require.Nil(t, err)
	eng, err := fconstructor(hummingbird.Config{}, nil, nil)
	require.Nil(t, eng)
	require.Equal(t, err, testErr)

	fconstructor, err = FindEngine("hopefullynotfound")
	require.Nil(t, fconstructor)
	require.NotNil(t, err)
}

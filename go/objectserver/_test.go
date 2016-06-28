//  Copyright (c) 2016 Red Hat, Inc.

package objectserver

import (
	"os"

	"github.com/openstack/swift/go/hummingbird"
)

func testGetHashPrefixAndSuffix() (pfx string, sfx string, err error) {
	return "", "983abc1de3ff4258", nil
}

func TestMain(m *testing.M) {
	hummingbird.GetHashPrefixAndSuffix = testGetHashPrefixAndSuffix
	os.Exit(m.Run())
}

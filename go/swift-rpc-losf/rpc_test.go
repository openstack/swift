// Copyright (c) 2010-2012 OpenStack Foundation
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bytes"
	pb "github.com/openstack/swift-rpc-losf/proto"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"github.com/alecuyer/statsd"
	"io"
	"io/ioutil"
	"net"
	"os"
	"path"
	"strings"
	"testing"
	"time"
)

func runTestServer(kv KV, diskPath string, addr string) (err error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return
	}

	s := grpc.NewServer()
	_, diskName := path.Split(path.Clean(diskPath))
	fs := &server{kv: kv, diskPath: diskPath, diskName: diskName, isClean: true}

	statsdPrefix := "kv"
	fs.statsd_c, err = statsd.New(statsd.Prefix(statsdPrefix))
	if err != nil {
		return
	}

	pb.RegisterFileMgrServer(s, fs)
	s.Serve(lis)
	return
}

func teardown(tempdir string) {
	if strings.HasPrefix(tempdir, "/tmp/") {
		os.RemoveAll(tempdir)
	}
}

var client pb.FileMgrClient

func populateKV() (err error) {
	volumes := []pb.NewVolumeInfo{
		{Partition: 9, Type: 0, VolumeIndex: 20, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 10, Type: 0, VolumeIndex: 35, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 40, Type: 0, VolumeIndex: 24, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 63, Type: 0, VolumeIndex: 27, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 65, Type: 0, VolumeIndex: 33, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 71, Type: 0, VolumeIndex: 19, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 111, Type: 0, VolumeIndex: 47, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 127, Type: 0, VolumeIndex: 43, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 139, Type: 0, VolumeIndex: 50, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 171, Type: 0, VolumeIndex: 49, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 195, Type: 0, VolumeIndex: 12, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 211, Type: 0, VolumeIndex: 16, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 213, Type: 0, VolumeIndex: 14, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 243, Type: 0, VolumeIndex: 17, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 271, Type: 0, VolumeIndex: 8, Offset: 24576, State: 0, RepairTool: false},
		{Partition: 295, Type: 0, VolumeIndex: 28, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 327, Type: 0, VolumeIndex: 48, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 360, Type: 0, VolumeIndex: 15, Offset: 12288, State: 0, RepairTool: false},
		{Partition: 379, Type: 0, VolumeIndex: 25, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 417, Type: 0, VolumeIndex: 22, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 420, Type: 0, VolumeIndex: 32, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 421, Type: 0, VolumeIndex: 46, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 428, Type: 0, VolumeIndex: 21, Offset: 12288, State: 0, RepairTool: false},
		{Partition: 439, Type: 0, VolumeIndex: 38, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 453, Type: 0, VolumeIndex: 44, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 466, Type: 0, VolumeIndex: 40, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 500, Type: 0, VolumeIndex: 39, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 513, Type: 0, VolumeIndex: 26, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 530, Type: 0, VolumeIndex: 4, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 530, Type: 1, VolumeIndex: 5, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 535, Type: 0, VolumeIndex: 1, Offset: 20480, State: 0, RepairTool: false},
		{Partition: 535, Type: 0, VolumeIndex: 2, Offset: 4096, State: 0, RepairTool: false},
		{Partition: 535, Type: 1, VolumeIndex: 3, Offset: 12288, State: 0, RepairTool: false},
		{Partition: 559, Type: 0, VolumeIndex: 30, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 602, Type: 0, VolumeIndex: 41, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 604, Type: 0, VolumeIndex: 29, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 673, Type: 0, VolumeIndex: 11, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 675, Type: 0, VolumeIndex: 42, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 710, Type: 0, VolumeIndex: 37, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 765, Type: 0, VolumeIndex: 36, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 766, Type: 0, VolumeIndex: 45, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 786, Type: 0, VolumeIndex: 23, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 809, Type: 0, VolumeIndex: 31, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 810, Type: 0, VolumeIndex: 13, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 855, Type: 0, VolumeIndex: 18, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 974, Type: 0, VolumeIndex: 9, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 977, Type: 0, VolumeIndex: 6, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 977, Type: 1, VolumeIndex: 7, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 1009, Type: 0, VolumeIndex: 34, Offset: 8192, State: 0, RepairTool: false},
		{Partition: 1019, Type: 0, VolumeIndex: 10, Offset: 8192, State: 0, RepairTool: false},
	}

	objects := []pb.NewObjectInfo{
		{Name: []byte("85fd12f8961e33cbf7229a94118524fa1515589781.45671.ts"), VolumeIndex: 3, Offset: 8192, NextOffset: 12288, RepairTool: false},
		{Name: []byte("84afc1659c7e8271951fe370d6eee0f81515590332.51834.ts"), VolumeIndex: 5, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("f45bf9000f39092b9de5a74256e3eebe1515590648.06511.ts"), VolumeIndex: 7, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("43c8adc53dbb40d27add4f614fc49e5e1515595691.35618#0#d.data"), VolumeIndex: 8, Offset: 20480, NextOffset: 24576, RepairTool: false},
		{Name: []byte("f3804523d91d294dab1500145b43395b1515596136.42189#4#d.data"), VolumeIndex: 9, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("fefe1ba1120cd6cd501927401d6b2ecc1515750800.13517#2#d.data"), VolumeIndex: 10, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("a8766d2608b77dc6cb0bfe3fe6782c731515750800.18975#0#d.data"), VolumeIndex: 11, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("30f12368ca25d11fb1a80d10e64b15431515750800.19224#4#d.data"), VolumeIndex: 12, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("ca9576ada218f74cb8f11648ecec439c1515750800.21553#2#d.data"), VolumeIndex: 13, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("3549df7ef11006af6852587bf16d82971515750800.22096#2#d.data"), VolumeIndex: 14, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("5a0a70e36a057a9982d1dc9188069b511515750803.50544#0#d.data"), VolumeIndex: 15, Offset: 8192, NextOffset: 12288, RepairTool: false},
		{Name: []byte("5a1801fea97614f8c5f58511905773d01515750800.40035#0#d.data"), VolumeIndex: 15, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("34c46ce96897a24374d126d7d7eab2fb1515750800.42545#0#d.data"), VolumeIndex: 16, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("3cf60143ea488c84da9e1603158203a11515750800.93160#0#d.data"), VolumeIndex: 17, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("d5c64e9cb0b093441fb6b500141aa0531515750800.94069#2#d.data"), VolumeIndex: 18, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("11f5db768b6f9a37cf894af99b15c0d11515750801.05135#4#d.data"), VolumeIndex: 19, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("02573d31b770cda8e0effd7762e8a0751515750801.09785#2#d.data"), VolumeIndex: 20, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("6b08eabf5667557c72dc6570aa1fb8451515750801.08639#4#d.data"), VolumeIndex: 21, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("6b08eabf5667557c72dc6570aa1fb8451515750856.77219.meta"), VolumeIndex: 21, Offset: 8192, NextOffset: 12288, RepairTool: false},
		{Name: []byte("6b08eabf5667557c72dc6570abcfb8451515643210.72429#4#d.data"), VolumeIndex: 22, Offset: 8192, NextOffset: 12288, RepairTool: false},
		{Name: []byte("687ba0410f4323c66397a85292077b101515750801.10244#0#d.data"), VolumeIndex: 22, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("c4aaea9b28c425f45eb64d4d5b0b3f621515750801.19478#2#d.data"), VolumeIndex: 23, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("0a0898eb861579d1240adbb1c9f0c92b1515750801.20636#2#d.data"), VolumeIndex: 24, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("5efd43142db5913180ba865ef529eccd1515750801.64704#4#d.data"), VolumeIndex: 25, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("806a35f1e974f93161b2da51760f22701515750801.68309#2#d.data"), VolumeIndex: 26, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("0fdceb7af49cdd0cb1262acbdc88ae881515750801.93565#0#d.data"), VolumeIndex: 27, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("49d4fa294d2c97f08596148bf4615bfa1515750801.93739#4#d.data"), VolumeIndex: 28, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("971b4d05733f475d447d7f8b050bb0071515750802.09721#2#d.data"), VolumeIndex: 29, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("8bc66b3ae033db15ceb3729d89a07ece1515750802.51062#0#d.data"), VolumeIndex: 30, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("ca53beae1aeb4deacd17409e32305a2c1515750802.63996#2#d.data"), VolumeIndex: 31, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("69375433763d9d511114e8ac869c916c1515750802.63846#0#d.data"), VolumeIndex: 32, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("105de5f388ab4b72e56bc93f36ad388a1515750802.73393#2#d.data"), VolumeIndex: 33, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("105de5f388ab4b72e56bc93f36ad388a1515873948.27383#2#d.meta"), VolumeIndex: 33, Offset: 8192, NextOffset: 12288, RepairTool: false},
		{Name: []byte("fc6916fd1e6a0267afac88c395b876ac1515750802.83459#2#d.data"), VolumeIndex: 34, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("02b10d6bfb205fe0f34f9bd82336dc711515750802.93662#2#d.data"), VolumeIndex: 35, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("bf43763a98208f15da803e76bf52e7d11515750803.01357#0#d.data"), VolumeIndex: 36, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("b1abadfed91b1cb4392dd2ec29e171ac1515750803.07767#4#d.data"), VolumeIndex: 37, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("6de30d74634d088f1f5923336af2b3ae1515750803.36199#4#d.data"), VolumeIndex: 38, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("7d234bbd1137d509105245ac78427b9f1515750803.49022#4#d.data"), VolumeIndex: 39, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("749057975c1bac830360530bdcd741591515750803.49647#0#d.data"), VolumeIndex: 40, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("9692991e77c9742cbc24469391d499981515750803.56295#0#d.data"), VolumeIndex: 41, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("a8dbd473e360787caff0b97aca33373f1515750803.68428#2#d.data"), VolumeIndex: 42, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("1ff88cb2b6b64f1fd3b6097f20203ee01515750803.73746#4#d.data"), VolumeIndex: 43, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("71572f46094d7ac440f5e2a3c72da17b1515750803.75628#2#d.data"), VolumeIndex: 44, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("bf8e83d954478d66ac1dba7eaa832c721515750803.81141#4#d.data"), VolumeIndex: 45, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("69724f682fe12b4a4306bceeb75825431515750804.10112#2#d.data"), VolumeIndex: 46, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("1bf38645ccc5f158c96480f1e0861a141515750804.31472#0#d.data"), VolumeIndex: 47, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("51fecf0e0bb30920fd0d83ee8fba29f71515750804.32492#2#d.data"), VolumeIndex: 48, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("2acbf85061e46b3bb3adb8930cb7414d1515750804.46622#2#d.data"), VolumeIndex: 49, Offset: 4096, NextOffset: 8192, RepairTool: false},
		{Name: []byte("22e4a97f1d4f2b6d4150bb9b481e4c971515750804.51987#0#d.data"), VolumeIndex: 50, Offset: 4096, NextOffset: 8192, RepairTool: false},
	}

	// Register volumes
	for _, df := range volumes {
		_, err = client.RegisterVolume(context.Background(), &df)
		if err != nil {
			return
		}
	}

	// Register objects
	for _, obj := range objects {
		_, err = client.RegisterObject(context.Background(), &obj)
		if err != nil {
			return
		}
	}
	return
}

func TestMain(m *testing.M) {
	log.Info("RPC test setup")
	log.SetLevel(logrus.ErrorLevel)
	diskPath, err := ioutil.TempDir("/tmp", "losf-test")
	if err != nil {
		log.Fatal(err)
	}
	rootDir := path.Join(diskPath, "losf")
	dbDir := path.Join(rootDir, "db")

	err = os.MkdirAll(rootDir, 0700)
	if err != nil {
		log.Fatal(err)
	}

	kv, err := openLevigoDB(dbDir)
	if err != nil {
		log.Fatal("failed to create leveldb")
	}
	addr := "127.0.0.1:22345"
	go runTestServer(kv, diskPath, addr)

	// test client
	opts := []grpc.DialOption{grpc.WithBlock(), grpc.WithTimeout(time.Second), grpc.WithInsecure()}
	conn, err := grpc.Dial(addr, opts...)
	if err != nil {
		log.Fatal(err)
	}

	client = pb.NewFileMgrClient(conn)

	err = populateKV()
	if err != nil {
		log.Error(err)
		log.Fatal("failed to populate test KV")
	}

	ret := m.Run()

	teardown(diskPath)

	os.Exit(ret)
}

// TODO, add more tests:
//   - prefix with no objects
//   - single object
//   - first and last elements of the KV
func TestLoadObjectsByPrefix(t *testing.T) {
	prefix := &pb.ObjectPrefix{Prefix: []byte("105de5f388ab4b72e56bc93f36ad388a")}

	expectedObjects := []pb.Object{
		{Name: []byte("105de5f388ab4b72e56bc93f36ad388a1515750802.73393#2#d.data"), VolumeIndex: 33, Offset: 4096},
		{Name: []byte("105de5f388ab4b72e56bc93f36ad388a1515873948.27383#2#d.meta"), VolumeIndex: 33, Offset: 8192},
	}

	r, err := client.LoadObjectsByPrefix(context.Background(), prefix)
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	for i, obj := range r.Objects {
		expected := expectedObjects[i]
		if !bytes.Equal(obj.Name, expected.Name) {
			t.Errorf("\ngot     : %s\nexpected: %s", string(obj.Name), string(expected.Name))
		}
	}
}

func TestListPartitions(t *testing.T) {
	partPower := uint32(10)

	expectedPartitions := []string{"9", "10", "40", "63", "65", "71", "111", "127", "139", "171", "195", "211", "213", "243", "271", "295", "327", "360", "379", "417", "420", "421", "428", "439", "453", "466", "500", "513", "530", "535", "559", "602", "604", "673", "675", "710", "765", "766", "786", "809", "810", "855", "974", "977", "1009", "1019"}

	r, err := client.ListPartitions(context.Background(), &pb.ListPartitionsInfo{PartitionBits: partPower})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	if len(r.Entry) != len(expectedPartitions) {
		t.Fatalf("\ngot: %v\nwant: %v", r.Entry, expectedPartitions)
	}

	for i, obj := range r.Entry {
		if obj != expectedPartitions[i] {
			t.Fatalf("checking individual elements\ngot: %v\nwant: %v", r.Entry, expectedPartitions)
		}
	}
}

func TestListPartitionRecursive(t *testing.T) {
	partition := uint32(428)
	partPower := uint32(10)

	expEntries := []pb.FullPathEntry{
		{Suffix: []byte("845"), Ohash: []byte("6b08eabf5667557c72dc6570aa1fb845"), Filename: []byte("1515750801.08639#4#d.data")},
		{Suffix: []byte("845"), Ohash: []byte("6b08eabf5667557c72dc6570aa1fb845"), Filename: []byte("1515750856.77219.meta")},
		{Suffix: []byte("845"), Ohash: []byte("6b08eabf5667557c72dc6570abcfb845"), Filename: []byte("1515643210.72429#4#d.data")},
	}

	r, err := client.ListPartitionRecursive(context.Background(), &pb.ListPartitionInfo{Partition: partition, PartitionBits: partPower})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	if len(r.FileEntries) != len(expEntries) {
		t.Fatalf("\ngot: %v\nwant: %v", r.FileEntries, expEntries)
	}

	for i, e := range r.FileEntries {
		if !bytes.Equal(e.Suffix, expEntries[i].Suffix) || !bytes.Equal(e.Ohash, expEntries[i].Ohash) || !bytes.Equal(e.Filename, expEntries[i].Filename) {
			t.Fatalf("checking individual elements\ngot: %v\nwant: %v", r.FileEntries, expEntries)
		}
	}
}

// TODO: add more tests, have a suffix with multiple entries
func TestListSuffix(t *testing.T) {
	partition := uint32(428)
	partPower := uint32(10)
	suffix := []byte("845")

	expectedHashes := []string{"6b08eabf5667557c72dc6570aa1fb845", "6b08eabf5667557c72dc6570abcfb845"}

	r, err := client.ListSuffix(context.Background(), &pb.ListSuffixInfo{Partition: partition, Suffix: suffix, PartitionBits: partPower})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}

	if len(r.Entry) != len(expectedHashes) {
		t.Fatalf("\ngot: %v\nwant: %v", r.Entry, expectedHashes)
	}

	for i, obj := range r.Entry {
		if obj != expectedHashes[i] {
			t.Fatalf("checking individual elements\ngot: %v\nwant: %v", r.Entry, expectedHashes)
		}
	}
}

func TestState(t *testing.T) {
	// Mark dirty and check
	_, err := client.SetKvState(context.Background(), &pb.KvState{IsClean: false})
	if err != nil {
		t.Fatalf("Failed to change KV state")
	}
	resp, err := client.GetKvState(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}
	if resp.IsClean != false {
		t.Fatal("isClean true, should be false")
	}

	// Mark clean and check
	_, err = client.SetKvState(context.Background(), &pb.KvState{IsClean: true})
	if err != nil {
		t.Fatalf("Failed to change KV state")
	}
	resp, err = client.GetKvState(context.Background(), &pb.Empty{})
	if err != nil {
		t.Fatalf("RPC call failed: %v", err)
	}
	if resp.IsClean != true {
		t.Fatal("isClean false, should be true")
	}

}

func TestRegisterObject(t *testing.T) {
    // Register new non-existing object
	name := []byte("33dea50d391ee52a8ead7cb562a9b4e2/1539791765.84449#5#d.data")
    obj := &pb.NewObjectInfo{Name: name, VolumeIndex: 1, Offset: 4096, NextOffset: 8192, RepairTool: false}
    _, err := client.RegisterObject(context.Background(), obj)
    if err != nil {
        t.Fatalf("failed to register object: %s", err)
    }

    objInfo := &pb.LoadObjectInfo{Name: name, IsQuarantined: false, RepairTool: false}
    r, err := client.LoadObject(context.Background(), objInfo)
    if err != nil {
        t.Fatalf("error getting registered object: %s", err)
    }
    if !bytes.Equal(r.Name, name) || r.VolumeIndex != 1 || r.Offset != 4096 {
        t.Fatalf("object found but name, volume index, or offset, is wrong: %v", r)
    }

    // Register existing object, which should fail
    obj = &pb.NewObjectInfo{Name: name, VolumeIndex: 1, Offset: 4096, NextOffset: 8192, RepairTool: false}
    _, err = client.RegisterObject(context.Background(), obj)
	if err != nil {
		grpcStatus, ok := status.FromError(err)
		if !ok {
			t.Fatal("failed to convert error to grpc status")
		}
		if grpcStatus.Code() != codes.AlreadyExists {
			t.Fatalf("registering existing object, expected AlreadyExists, got: %s", err)
		}
	} else {
		t.Fatal("was able to register an existing object")
	}

    // Remove object
    unregInfo := &pb.ObjectName{Name: name}
    _, err = client.UnregisterObject(context.Background(), unregInfo)
    if err != nil {
        t.Fatalf("failed to unregister object: %s", err)
    }

    // Attempt to remove again, should fail
    unregInfo = &pb.ObjectName{Name: name}
    _, err = client.UnregisterObject(context.Background(), unregInfo)
	if err != nil {
		grpcStatus, ok := status.FromError(err)
		if !ok {
			t.Fatal("failed to convert error to grpc status")
		}
		if grpcStatus.Code() != codes.NotFound {
			t.Fatalf("unregistering non-existent object, expected NotFound, got: %s", err)
		}
	} else {
		t.Fatal("was able to unregister a non-existent object")
	}
}

func TestQuarantineObject(t *testing.T) {
	// Quarantine an existing object
	name := []byte("bf43763a98208f15da803e76bf52e7d11515750803.01357#0#d.data")
	objName := &pb.ObjectName{Name: name, RepairTool: false}
	_, err := client.QuarantineObject(context.Background(), objName)
	if err != nil {
		t.Fatal("Failed to quarantine object")
	}
	// We shouldn't be able to find it
	objInfo := &pb.LoadObjectInfo{Name: name, IsQuarantined: false, RepairTool: false}
	_, err = client.LoadObject(context.Background(), objInfo)
	if err != nil {
		grpcStatus, ok := status.FromError(err)
		if !ok {
			t.Fatal("failed to convert error to grpc status")
		}
		if grpcStatus.Code() != codes.NotFound {
			t.Fatalf("Getting quarantined object, expected NotFound, got: %s", err)
		}
	} else {
		t.Fatal("Quarantined object can still be found")
	}

	// TODO, need to test that the quarantined object exists
	// then try to quarantine non existent object
}

func TestUnquarantineObject(t *testing.T) {
	// Unuarantine an existing quarantined object (check that)
	name := []byte("bf43763a98208f15da803e76bf52e7d11515750803.01357#0#d.data")
	objName := &pb.ObjectName{Name: name, RepairTool: false}
	_, err := client.UnquarantineObject(context.Background(), objName)
	if err != nil {
		t.Fatal("Failed to quarantine object")
	}
	// We should be able to find it
	objInfo := &pb.LoadObjectInfo{Name: name, IsQuarantined: false, RepairTool: false}
	_, err = client.LoadObject(context.Background(), objInfo)
	if err != nil {
		t.Fatal("cannot find unquarantined object")
	}

	// TODO, need to test that the quarantined object exists
	// then try to quarantine non existent object
}

// This test modifies the DB
func TestListQuarantinedOHashes(t *testing.T) {
	empty := &pb.Empty{}
	stream, err := client.ListQuarantinedOHashes(context.Background(), empty)
	if err != nil {
		t.Fatalf("failed to initialize stream for ListQuarantinedOHashes")
	}

	for {
		_, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal("error listing quarantined objects")
		}
		t.Fatal("expected no quarantined objects")
	}

	objectsToQuarantine := []pb.ObjectName{
		{Name: []byte("02573d31b770cda8e0effd7762e8a0751515750801.09785#2#d.data"), RepairTool: false},
		{Name: []byte("6b08eabf5667557c72dc6570aa1fb8451515750801.08639#4#d.data"), RepairTool: false},
		{Name: []byte("6b08eabf5667557c72dc6570aa1fb8451515750856.77219.meta"), RepairTool: false},
		{Name: []byte("6b08eabf5667557c72dc6570abcfb8451515643210.72429#4#d.data"), RepairTool: false},
		{Name: []byte("687ba0410f4323c66397a85292077b101515750801.10244#0#d.data"), RepairTool: false},
		{Name: []byte("c4aaea9b28c425f45eb64d4d5b0b3f621515750801.19478#2#d.data"), RepairTool: false},
		{Name: []byte("0a0898eb861579d1240adbb1c9f0c92b1515750801.20636#2#d.data"), RepairTool: false},
	}

	expectedOhashes := [][]byte{
		[]byte("02573d31b770cda8e0effd7762e8a075"),
		[]byte("0a0898eb861579d1240adbb1c9f0c92b"),
		[]byte("687ba0410f4323c66397a85292077b10"),
		[]byte("6b08eabf5667557c72dc6570aa1fb845"),
		[]byte("6b08eabf5667557c72dc6570abcfb845"),
		[]byte("c4aaea9b28c425f45eb64d4d5b0b3f62"),
	}

	for _, qObj := range objectsToQuarantine {
		if _, err := client.QuarantineObject(context.Background(), &qObj); err != nil {
			t.Fatalf("failed to quarantine object: %s", err)
		}
	}

	receivedOhashes := [][]byte{}

	stream, err = client.ListQuarantinedOHashes(context.Background(), empty)
	if err != nil {
		t.Fatalf("failed to initialize stream for ListQuarantinedOHashes")
	}

	for {
		obj, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal("error listing quarantined objects")
		}
		receivedOhashes = append(receivedOhashes, obj.Name)
	}

	if !testEqSliceBytes(receivedOhashes, expectedOhashes) {
		t.Fatalf("\nexpected %v\ngot      %v", expectedOhashes, receivedOhashes)
	}

}

func TestListQuarantinedOHash(t *testing.T) {
	ohash := pb.ObjectPrefix{Prefix: []byte("6b08eabf5667557c72dc6570aa1fb845"), RepairTool: false}
	qList, err := client.ListQuarantinedOHash(context.Background(), &ohash)
	if err != nil {
		t.Fatalf("error listing quarantined object files: %s", err)
	}

	expectedFiles := [][]byte{
		[]byte("6b08eabf5667557c72dc6570aa1fb8451515750801.08639#4#d.data"),
		[]byte("6b08eabf5667557c72dc6570aa1fb8451515750856.77219.meta"),
	}

	if len(qList.Objects) != len(expectedFiles) {
		t.Fatalf("got %d objects, expected %d", len(qList.Objects), len(expectedFiles))
	}

	receivedFiles := make([][]byte, len(qList.Objects))
	for i, obj := range qList.Objects {
		receivedFiles[i] = obj.Name
	}

	if !testEqSliceBytes(receivedFiles, expectedFiles) {
		t.Fatalf("\nexpected %v\ngot      %v", expectedFiles, receivedFiles)
	}

	// Add test, non existent ohash
	// Add test, unquarantine one file, list again
}

func testEqSliceBytes(a, b [][]byte) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// func (s * server) ListQuarantinedOHashes(ctx context.Context, in *pb.ListQuarantinedOHashesInfo) (*pb.QuarantinedObjects, error) {

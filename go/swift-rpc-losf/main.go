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

// KV store for LOFS
package main

import (
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"
)

// Name of the base losf root directory, relative to the swift disk
const rootDirBase = "losf"

// Will run checks and create rootDir if needed
// Returns the path to rootDir
func diskSetup(diskPath string, policyIdx int, waitForMount bool) (string, error) {

	if waitForMount {
		log.Debugf("waitForMount is set, if %s is not mounted, will wait until it is", diskPath)
	}
	for waitForMount {
		nowMounted, err := isMounted(diskPath)
		if err != nil {
			return "", err
		}
		if nowMounted {
			break
		}
		time.Sleep(time.Second / 10)
	}

	// OVH patch to match a similar mechanism in the python code.
	// If a ".offline" file exists in srv node, do not start (/srv/node/disk-XX.offline)
	offlineFile := fmt.Sprintf("%s%s", diskPath, ".offline")
	offlineFileExists := false
	if _, err := os.Stat(offlineFile); err == nil {
		offlineFileExists = true
		log.Debugf("offline file exists: %s", offlineFile)
	}
	for offlineFileExists {
		if _, err := os.Stat(offlineFile); os.IsNotExist(err) {
			offlineFileExists = false
		}
		time.Sleep(time.Second * 10)
	}

	rootDir := path.Join(diskPath, getBaseDirName(rootDirBase, policyIdx))
	log.Debug(rootDir)

	rootDirExists, err := dirExists(rootDir)
	if err != nil {
		return "", err
	}
	if !rootDirExists {
		err := os.Mkdir(rootDir, os.FileMode(0700))
		if err != nil {
			return "", err
		}
	}
	return rootDir, nil
}

// Parse options and starts the rpc server.
func main() {
	var dbDir, socketPath string
	var kv KV

	setupLogging()

	debugLevels := map[string]logrus.Level{
		"panic": logrus.PanicLevel,
		"fatal": logrus.FatalLevel,
		"error": logrus.ErrorLevel,
		"warn":  logrus.WarnLevel,
		"info":  logrus.InfoLevel,
		"debug": logrus.DebugLevel,
	}

	var diskPath = flag.String("diskPath", "", "Swift disk path (/srv/node/disk-xyz)")
	var policyIdx = flag.Int("policyIdx", 0, "Policy index")
	var waitForMount = flag.Bool("waitForMount", true, "Wait for diskPath to be mounted. If diskPath exists but is not a mount, it will wait")
	var profilerAddr = flag.String("profilerAddr", "", "Start profiler and make it available at this address (127.0.0.1:8081)")
	var debugLevel = flag.String("debug", "info", "Debug level (error, warn, info, debug)")
	var allowRoot = flag.Bool("allowRoot", false, "Allow process to run as root")
	var useGoLevelDB = flag.Bool("useGoLevelDB", false, "Use native golang levelDB package")

	flag.Parse()

	log.SetLevel(debugLevels[*debugLevel])

	if !*allowRoot && os.Getuid() == 0 {
		log.Fatal("running as root, and allowRoot is false")
	}

	if *diskPath == "" {
		log.Fatal("diskPath not specified")
	}

	rootDir, err := diskSetup(*diskPath, *policyIdx, *waitForMount)
	if err != nil {
		panic(err)
	}

	dbDir = path.Join(rootDir, "db")
	socketPath = path.Join(rootDir, "rpc.socket")
	rlog := log.WithFields(logrus.Fields{"socket": socketPath})

	// install signal handler
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	// start http server for profiling
	if *profilerAddr != "" {
		go func() {
			rlog.Debug(http.ListenAndServe(*profilerAddr, nil))
		}()
	}

	// Acquire lock to protect socket
	rlog.Debug("Locking socket")
	err = lockSocket(socketPath)
	if err != nil {
		rlog.Fatalf("Failed to lock RPC socket: %s", err)
	}
	os.Remove(socketPath)

	// Open the database
	if *useGoLevelDB {
		kv, err = openGoLevelDb(dbDir)
	} else {
		kv, err = openLevigoDB(dbDir)
	}

	if err != nil {
		rlog.Fatal(err)
	}

	// Check the kv was stopped properly
	isClean, err := setKvState(kv)
	if err != nil {
		rlog.Fatal(err)
	}
	log.Infof("kv is clean: %v", isClean)

	// Start the RPC server
	rlog.Info("Starting RPC server")
	err = runServer(kv, *diskPath, socketPath, stopChan, isClean)
	if err != nil {
		rlog.Fatal(err)
	}

	return
}

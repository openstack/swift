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

package main

import (
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/openstack/swift/go/bench"
	"github.com/openstack/swift/go/hummingbird"
	"github.com/openstack/swift/go/objectserver"
	"github.com/openstack/swift/go/proxyserver"
)

var Version = "0.1"

func WritePid(name string, pid int) error {
	file, err := os.Create(fmt.Sprintf("/var/run/hummingbird/%s.pid", name))
	if err != nil {
		return err
	}
	fmt.Fprintf(file, "%d", pid)
	file.Close()
	return nil
}

func RemovePid(name string) error {
	return os.RemoveAll(fmt.Sprintf("/var/run/hummingbird/%s.pid", name))
}

func GetProcess(name string) (*os.Process, error) {
	var pid int
	file, err := os.Open(fmt.Sprintf("/var/run/hummingbird/%s.pid", name))
	if err != nil {
		return nil, err
	}
	_, err = fmt.Fscanf(file, "%d", &pid)
	if err != nil {
		return nil, err
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return nil, err
	}
	err = process.Signal(syscall.Signal(0))
	if err != nil {
		return nil, err
	}
	return process, nil
}

func findConfig(name string) string {
	configName := strings.Split(name, "-")[0]
	configSearch := []string{
		fmt.Sprintf("/etc/hummingbird/%s-server.conf", configName),
		fmt.Sprintf("/etc/hummingbird/%s-server.conf.d", configName),
		fmt.Sprintf("/etc/hummingbird/%s-server", configName),
		fmt.Sprintf("/etc/swift/%s-server.conf", configName),
		fmt.Sprintf("/etc/swift/%s-server.conf.d", configName),
		fmt.Sprintf("/etc/swift/%s-server", configName),
	}
	for _, config := range configSearch {
		if hummingbird.Exists(config) {
			return config
		}
	}
	return ""
}

func StartServer(name string, args ...string) {
	_, err := GetProcess(name)
	if err == nil {
		fmt.Println("Found already running", name, "server")
		return
	}

	serverConf := findConfig(name)
	if serverConf == "" {
		fmt.Println("Unable to find config file")
		return
	}

	serverExecutable, err := exec.LookPath(os.Args[0])
	if err != nil {
		fmt.Println("Unable to find hummingbird executable in path.")
		return
	}

	uid, gid, err := hummingbird.UidFromConf(serverConf)
	if err != nil {
		fmt.Println("Unable to find uid to execute process:", err)
		return
	}

	cmd := exec.Command(serverExecutable, append([]string{name, "-d", "-c", serverConf}, args...)...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if uint32(os.Getuid()) != uid { // This is goofy.
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uid, Gid: gid}
	}
	rdp, err := cmd.StdoutPipe()
	cmd.Stderr = cmd.Stdout
	if err != nil {
		fmt.Println("Error creating stdout pipe:", err)
		return
	}

	syscall.Umask(022)
	err = cmd.Start()
	if err != nil {
		fmt.Println("Error starting server:", err)
		return
	}
	io.Copy(os.Stdout, rdp)
	WritePid(name, cmd.Process.Pid)
	fmt.Println(strings.Title(name), "server started.")
}

func StopServer(name string, args ...string) {
	process, err := GetProcess(name)
	if err != nil {
		fmt.Println("Error finding", name, "server process:", err)
		return
	}
	process.Signal(syscall.SIGTERM)
	process.Wait()
	RemovePid(name)
	fmt.Println(strings.Title(name), "server stopped.")
}

func RestartServer(name string, args ...string) {
	process, err := GetProcess(name)
	if err == nil {
		process.Signal(syscall.SIGTERM)
		process.Wait()
		fmt.Println(strings.Title(name), "server stopped.")
	} else {
		fmt.Println(strings.Title(name), "server not found.")
	}
	RemovePid(name)
	StartServer(name, args...)
}

func GracefulRestartServer(name string, args ...string) {
	process, err := GetProcess(name)
	if err == nil {
		process.Signal(syscall.SIGINT)
		time.Sleep(time.Second)
		fmt.Println(strings.Title(name), "server graceful shutdown began.")
	} else {
		fmt.Println(strings.Title(name), "server not found.")
	}
	RemovePid(name)
	StartServer(name, args...)
}

func GracefulShutdownServer(name string, args ...string) {
	process, err := GetProcess(name)
	if err != nil {
		fmt.Println("Error finding", name, "server process:", err)
		return
	}
	process.Signal(syscall.SIGINT)
	RemovePid(name)
	fmt.Println(strings.Title(name), "server graceful shutdown began.")
}

func ProcessControlCommand(serverCommand func(name string, args ...string)) {
	if !hummingbird.Exists("/var/run/hummingbird") {
		err := os.MkdirAll("/var/run/hummingbird", 0600)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Unable to create /var/run/hummingbird\n")
			fmt.Fprintf(os.Stderr, "You should create it, writable by the user you wish to launch servers with.\n")
			os.Exit(1)
		}
	}

	if flag.NArg() < 2 {
		flag.Usage()
		return
	}

	switch flag.Arg(1) {
	case "proxy", "object", "object-replicator", "object-auditor":
		serverCommand(flag.Arg(1), flag.Args()[2:]...)
	case "all":
		for _, server := range []string{"proxy", "object", "object-replicator", "object-auditor"} {
			serverCommand(server)
		}
	default:
		flag.Usage()
	}
}

func main() {
	hummingbird.UseMaxProcs()
	hummingbird.SetRlimits()
	rand.Seed(time.Now().Unix())

	/* sub-command flag parsers */

	proxyFlags := flag.NewFlagSet("proxy server", flag.ExitOnError)
	proxyFlags.Bool("d", false, "Close stdio once the server is running")
	proxyFlags.Bool("v", false, "Send all log messages to the console (if -d is not specified)")
	proxyFlags.String("c", findConfig("proxy"), "Config file/directory to use")
	proxyFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird proxy [ARGS]\n")
		fmt.Fprintf(os.Stderr, "  Run proxy server\n")
		proxyFlags.PrintDefaults()
	}

	objectFlags := flag.NewFlagSet("object server", flag.ExitOnError)
	objectFlags.Bool("d", false, "Close stdio once the server is running")
	objectFlags.Bool("v", false, "Send all log messages to the console (if -d is not specified)")
	objectFlags.String("c", findConfig("object"), "Config file/directory to use")
	objectFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird object [ARGS]\n")
		fmt.Fprintf(os.Stderr, "  Run object server\n")
		objectFlags.PrintDefaults()
	}

	objectReplicatorFlags := flag.NewFlagSet("object replicator", flag.ExitOnError)
	objectReplicatorFlags.Bool("q", false, "Quorum Delete. Will delete handoff node if pushed to #replicas/2 + 1 nodes.")
	objectReplicatorFlags.Bool("d", false, "Close stdio once the daemon is running")
	objectReplicatorFlags.Bool("v", false, "Send all log messages to the console (if -d is not specified)")
	objectReplicatorFlags.String("c", findConfig("object"), "Config file/directory to use")
	objectReplicatorFlags.Bool("once", false, "Run one pass of the replicator")
	objectReplicatorFlags.String("devices", "", "Replicate only given devices. Comma-separated list.")
	objectReplicatorFlags.String("partitions", "", "Replicate only given partitions. Comma-separated list.")
	objectReplicatorFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird object-replicator [ARGS]\n")
		fmt.Fprintf(os.Stderr, "  Run object replicator\n")
		objectReplicatorFlags.PrintDefaults()
	}

	objectAuditorFlags := flag.NewFlagSet("object auditor", flag.ExitOnError)
	objectAuditorFlags.Bool("d", false, "Close stdio once the daemon is running")
	objectAuditorFlags.Bool("v", false, "Send all log messages to the console (if -d is not specified)")
	objectAuditorFlags.String("c", findConfig("object"), "Config file/directory to use")
	objectAuditorFlags.Bool("once", false, "Run one pass of the auditor")
	objectAuditorFlags.Usage = func() {
		fmt.Fprintf(os.Stderr, "hummingbird object-auditor [ARGS]\n")
		fmt.Fprintf(os.Stderr, "  Run object auditor\n")
		objectAuditorFlags.PrintDefaults()
	}

	/* main flag parser, which doesn't do much */

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Hummingbird Usage\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "hummingbird [DAEMON COMMAND] [DAEMON NAME]\n")
		fmt.Fprintf(os.Stderr, "  Process control for daemons.  The commands are:\n")
		fmt.Fprintf(os.Stderr, "     start: start a server\n")
		fmt.Fprintf(os.Stderr, "     shutdown: gracefully stop a server\n")
		fmt.Fprintf(os.Stderr, "     stop: stop a server immediately\n")
		fmt.Fprintf(os.Stderr, "     reload: alias for graceful-restart\n")
		fmt.Fprintf(os.Stderr, "     restart: stop then restart a server\n")
		fmt.Fprintf(os.Stderr, "  The daemons are: object, proxy, object-replicator, object-auditor, all\n")
		fmt.Fprintf(os.Stderr, "\n")
		objectFlags.Usage()
		fmt.Fprintf(os.Stderr, "\n")
		objectReplicatorFlags.Usage()
		fmt.Fprintf(os.Stderr, "\n")
		objectAuditorFlags.Usage()
		fmt.Fprintf(os.Stderr, "\n")
		proxyFlags.Usage()
		fmt.Fprintf(os.Stderr, "\n")
		fmt.Fprintf(os.Stderr, "hummingbird moveparts [old ring.gz]\n")
		fmt.Fprintf(os.Stderr, "  Prioritize replication for moving partitions after a ring change\n\n")
		fmt.Fprintf(os.Stderr, "hummingbird restoredevice [ip] [device-name]\n")
		fmt.Fprintf(os.Stderr, "  Reconstruct a device from its peers\n\n")
		fmt.Fprintf(os.Stderr, "hummingbird rescueparts [partnum1,partnum2,...]\n")
		fmt.Fprintf(os.Stderr, "  Will send requests to all the object nodes to try to fully replicate given partitions if they have them.\n\n")
		fmt.Fprintf(os.Stderr, "hummingbird bench CONFIG\n")
		fmt.Fprintf(os.Stderr, "  Run bench tool\n\n")
		fmt.Fprintf(os.Stderr, "hummingbird dbench CONFIG\n")
		fmt.Fprintf(os.Stderr, "  Run direct to object server bench tool\n\n")
		fmt.Fprintf(os.Stderr, "hummingbird thrash CONFIG\n")
		fmt.Fprintf(os.Stderr, "  Run thrash bench tool\n")
		fmt.Fprintf(os.Stderr, "hummingbird grep [ACCOUNT/CONTAINER/PREFIX] [SEARCH-STRING]\n")
		fmt.Fprintf(os.Stderr, "  Run grep on the edge\n")
	}

	flag.Parse()

	if flag.NArg() < 1 {
		flag.Usage()
		return
	}

	switch flag.Arg(0) {
	case "version":
		fmt.Println(Version)
	case "start":
		ProcessControlCommand(StartServer)
	case "stop":
		ProcessControlCommand(StopServer)
	case "restart":
		ProcessControlCommand(RestartServer)
	case "reload", "graceful-restart":
		ProcessControlCommand(GracefulRestartServer)
	case "shutdown", "graceful-shutdown":
		ProcessControlCommand(GracefulShutdownServer)
	case "proxy":
		proxyFlags.Parse(flag.Args()[1:])
		hummingbird.RunServers(proxyserver.GetServer, proxyFlags)
	case "object":
		objectFlags.Parse(flag.Args()[1:])
		hummingbird.RunServers(objectserver.GetServer, objectFlags)
	case "object-replicator":
		objectReplicatorFlags.Parse(flag.Args()[1:])
		hummingbird.RunDaemon(objectserver.NewReplicator, objectReplicatorFlags)
	case "object-auditor":
		objectAuditorFlags.Parse(flag.Args()[1:])
		hummingbird.RunDaemon(objectserver.NewAuditor, objectAuditorFlags)
	case "bench":
		bench.RunBench(flag.Args()[1:])
	case "dbench":
		bench.RunDBench(flag.Args()[1:])
	case "thrash":
		bench.RunThrash(flag.Args()[1:])
	case "moveparts":
		objectserver.MoveParts(flag.Args()[1:])
	case "restoredevice":
		objectserver.RestoreDevice(flag.Args()[1:])
	case "rescueparts":
		objectserver.RescueParts(flag.Args()[1:])
	default:
		flag.Usage()
	}
}

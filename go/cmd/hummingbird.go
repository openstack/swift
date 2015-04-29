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

func StartServer(name string) {
	_, err := GetProcess(name)
	if err == nil {
		fmt.Println("Found already running", name, "server")
		return
	}
	serverConf := ""
	configName := strings.Split(name, "-")[0]
	configSearch := []string{
		fmt.Sprintf("/etc/hummingbird/%s-server.conf", configName),
		fmt.Sprintf("/etc/hummingbird/%s-server", configName),
		fmt.Sprintf("/etc/swift/%s-server.conf", configName),
		fmt.Sprintf("/etc/swift/%s-server", configName),
	}
	for _, config := range configSearch {
		if hummingbird.Exists(config) {
			serverConf = config
			break
		}
	}
	if serverConf == "" {
		fmt.Println("Unable to find", configName, "configuration file.")
		return
	}
	serverExecutable, err := exec.LookPath(os.Args[0])
	if err != nil {
		fmt.Println("Unable to find hummingbird executable in path.")
		return
	}
	cmd := exec.Command(serverExecutable, "run", name, serverConf)

	uid, gid, err := hummingbird.UidFromConf(serverConf)
	if err != nil {
		fmt.Println("Unable to find uid to execute process:", err)
		return
	}
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

func StopServer(name string) {
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

func RestartServer(name string) {
	process, err := GetProcess(name)
	if err == nil {
		process.Signal(syscall.SIGTERM)
		process.Wait()
		fmt.Println(strings.Title(name), "server stopped.")
	} else {
		fmt.Println(strings.Title(name), "server not found.")
	}
	RemovePid(name)
	StartServer(name)
}

func GracefulRestartServer(name string) {
	process, err := GetProcess(name)
	if err == nil {
		process.Signal(syscall.SIGINT)
		time.Sleep(time.Second)
		fmt.Println(strings.Title(name), "server graceful shutdown began.")
	} else {
		fmt.Println(strings.Title(name), "server not found.")
	}
	RemovePid(name)
	StartServer(name)
}

func GracefulShutdownServer(name string) {
	process, err := GetProcess(name)
	if err != nil {
		fmt.Println("Error finding", name, "server process:", err)
		return
	}
	process.Signal(syscall.SIGINT)
	RemovePid(name)
	fmt.Println(strings.Title(name), "server graceful shutdown began.")
}

func RunServer(name string) {
	switch name {
	case "object":
		hummingbird.RunServers(os.Args[3], objectserver.GetServer)
	case "proxy":
		hummingbird.RunServers(os.Args[3], proxyserver.GetServer)
	case "object-replicator":
		hummingbird.RunDaemon(os.Args[3], objectserver.NewReplicator)
	case "object-auditor":
		hummingbird.RunDaemon(os.Args[3], objectserver.NewAuditor)
	}
}

func RunCommand(cmd string, args ...string) {
	executable, err := exec.LookPath(cmd)
	if err != nil {
		fmt.Println("Unable to find executable", cmd)
		return
	}
	processArgs := append([]string{executable}, args...)
	err = syscall.Exec(executable, processArgs, nil)
	fmt.Println("Failed to execute", executable)
}

func FakeSwiftObject(configFile string) {
	devnull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0600)
	if err != nil {
		fmt.Println("Error opening devnull")
		return
	}
	syscall.Dup2(int(devnull.Fd()), int(os.Stdin.Fd()))
	syscall.Dup2(int(devnull.Fd()), int(os.Stdout.Fd()))
	syscall.Dup2(int(devnull.Fd()), int(os.Stderr.Fd()))
	devnull.Close()
	hummingbird.RunServers(configFile, objectserver.GetServer)
}

func main() {
	hummingbird.UseMaxProcs()
	hummingbird.SetRlimits()
	rand.Seed(time.Now().Unix())

	var serverList []string
	var serverCommand func(name string)

	if len(os.Args) < 2 {
		goto USAGE
	}

	if strings.HasSuffix(os.Args[0], "swift-object-server") && strings.HasPrefix(os.Args[1], "/etc/swift/object-server/") {
		FakeSwiftObject(os.Args[1])
		return
	}

	switch strings.ToLower(os.Args[1]) {
	case "version":
		fmt.Println(Version)
		os.Exit(0)
	case "run":
		if len(os.Args) < 4 {
			goto USAGE
		}
		serverCommand = RunServer
	case "start":
		serverCommand = StartServer
	case "stop":
		serverCommand = StopServer
	case "restart":
		serverCommand = RestartServer
	case "reload", "graceful-restart":
		serverCommand = GracefulRestartServer
	case "shutdown", "graceful-shutdown":
		serverCommand = GracefulShutdownServer
	case "bench":
		bench.RunBench(os.Args[2:])
		return
	case "dbench":
		bench.RunDBench(os.Args[2:])
		return
	case "thrash":
		bench.RunThrash(os.Args[2:])
		return
	default:
		goto USAGE
	}

	if len(os.Args) < 3 {
		goto USAGE
	}

	if !hummingbird.Exists("/var/run/hummingbird") {
		err := os.MkdirAll("/var/run/hummingbird", 0600)
		if err != nil {
			fmt.Println("Unable to create /var/run/hummingbird")
			fmt.Println("You should create it, writable by the user you wish to launch servers with.")
			os.Exit(1)
		}
	}

	switch strings.ToLower(os.Args[2]) {
	case "proxy", "object", "object-replicator", "object-auditor":
		serverList = []string{strings.ToLower(os.Args[2])}
	case "all":
		serverList = []string{"proxy", "object", "object-replicator", "object-auditor"}
	default:
		goto USAGE
	}

	for _, server := range serverList {
		serverCommand(server)
	}
	return

USAGE:
	fmt.Println("Usage: hummingbird [command] [args...]")
	fmt.Println("")
	fmt.Println("Process control: args=[object,proxy,all]")
	fmt.Println("              run: run a server (attached)")
	fmt.Println("            start: start a server (detached)")
	fmt.Println("             stop: stop a server immediately")
	fmt.Println("          restart: stop then restart a server")
	fmt.Println("         shutdown: gracefully stop a server")
	fmt.Println("           reload: alias for graceful-restart")
}

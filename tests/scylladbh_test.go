package scylladbh_test

import (
	"encoding/json"
	"net"
	"os/exec"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	"github.com/uol/gofiles"
	"github.com/uol/scylladbh"
)

//
// Tests for the library.
// author: rnojiri
//

func testConfWithError(t *testing.T, instance interface{}, err error, expectedError error) bool {

	if !assert.Nil(t, instance, "instance must be null") {
		return false
	}

	if !assert.Error(t, err, "expecting an error") {
		return false
	}

	return assert.Equalf(t, expectedError, err, "expecting the %s error", expectedError)
}

// TestNullConf - tests a null configuration
func TestNullConf(t *testing.T) {

	s, err := scylladbh.NewSession(nil)
	if !testConfWithError(t, s, err, scylladbh.ErrNullConf) {
		return
	}

	sx, err := scylladbh.NewSessionX(nil)
	if !testConfWithError(t, sx, err, scylladbh.ErrNullConf) {
		return
	}
}

// TestNoNodesConf - test no nodes configured configuration
func TestNoNodesConf(t *testing.T) {

	conf := &scylladbh.Configuration{}

	s, err := scylladbh.NewSession(conf)
	if !testConfWithError(t, s, err, scylladbh.ErrNoNodes) {
		return
	}

	sx, err := scylladbh.NewSessionX(conf)
	if !testConfWithError(t, sx, err, scylladbh.ErrNoNodes) {
		return
	}
}

var expectedConf scylladbh.Configuration = scylladbh.Configuration{
	Nodes:             []string{"node1", "node2", "node3"},
	Keyspace:          "test",
	NumConnections:    5,
	Password:          "c4554ndr4",
	Port:              19042,
	ReconnectInterval: *funks.ForceNewStringDuration("10s"),
	Username:          "cassandra",
	ProtoVersion:      2,
}

// TestLoadingFromToml - test loading configuration from a toml file
func TestLoadingFromToml(t *testing.T) {

	conf := scylladbh.Configuration{}

	_, err := toml.DecodeFile("./config.toml", &conf)
	if !assert.NoError(t, err, "no error expected reading the toml") {
		return
	}

	jsonStr, err := json.Marshal(&conf)
	if err != nil {
		panic(err)
	}

	assert.True(t, reflect.DeepEqual(expectedConf, conf), "expected same configuration: %s", jsonStr)
}

// TestLoadingFromJSON - test loading configuration from a json file
func TestLoadingFromJSON(t *testing.T) {

	jsonBytes, err := gofiles.ReadFileBytes("./config.json")
	if !assert.NoError(t, err, "expected no error loading json file") {
		return
	}

	conf := scylladbh.Configuration{}

	err = json.Unmarshal(jsonBytes, &conf)
	if !assert.NoError(t, err, "expected no error unmarshalling json") {
		return
	}

	assert.True(t, reflect.DeepEqual(expectedConf, conf), "expected same configuration: %s", string(jsonBytes))
}

func removeScyllaDocker(pod string) {

	exec.Command("/bin/sh", "-c", "docker rm -f "+pod).Run()
}

func startScyllaDocker(t *testing.T, pod string) bool {

	output, err := exec.Command("/bin/sh", "-c", "docker run --name "+pod+" -p 9042:9042 -d scylladb/scylla").Output()

	if !assert.NoError(t, err, "error executing run command, output: %s", string(output)) {
		return false
	}

	if !assert.Regexp(t, regexp.MustCompile("[a-f0-9]{64}"), string(output), "pod was not created") {
		return false
	}

	output, err = exec.Command("docker", "inspect", "--format='{{ .NetworkSettings.Networks.bridge.IPAddress }}'", pod).Output()
	if !assert.NoError(t, err, "not expecting error checking pod ip") {
		return false
	}

	lines := strings.Split(string(output), "\n")
	host := strings.Trim(lines[0], "'")
	start := time.Now()
	connected := false

	// wait scylla to start listening on 9042
	for {
		if time.Now().Sub(start).Seconds() > 60 {
			break
		}

		<-time.After(1 * time.Second)
		conn, err := net.DialTimeout("tcp", net.JoinHostPort(host, "9042"), 1*time.Second)
		if err != nil {
			continue
		}
		if conn != nil {
			defer conn.Close()
			connected = true
			break
		}
	}

	return assert.True(t, connected, "expected connection to the scylla node")
}

// TestDockerAutoConnection - test the docker automatic connection
func TestDockerAutoConnection(t *testing.T) {

	pod := "test-scylla"

	removeScyllaDocker(pod)

	if !startScyllaDocker(t, pod) {
		return
	}

	defer removeScyllaDocker(pod)

	conf := &scylladbh.Configuration{
		Nodes: []string{pod},
	}

	s, err := scylladbh.NewDockerSession(conf, "")
	if !assert.NoError(t, err, "error not expected on library connection") {
		return
	}

	text := ""

	err = s.Query("select keyspace_name from system_schema.keyspaces limit 1").Scan(&text)
	if !assert.NoError(t, err, "expected no error running keyspaces query") {
		return
	}

	defer s.Close()

	assert.Regexp(t, regexp.MustCompile("[a-zA-Z_]+"), text, "expecting a valid keyspace name: %s", text)
}

// TestDockerAutoConnectionX - test the docker automatic connection x
func TestDockerAutoConnectionX(t *testing.T) {

	pod := "test-scylla"

	removeScyllaDocker(pod)

	if !startScyllaDocker(t, pod) {
		return
	}

	defer removeScyllaDocker(pod)

	conf := &scylladbh.Configuration{
		Nodes: []string{pod},
	}

	sx, err := scylladbh.NewDockerSessionX(conf, "")
	if !assert.NoError(t, err, "error not expected on library x connection") {
		return
	}

	defer sx.Close()

	text := ""

	err = sx.Query("select table_name from system_schema.tables limit 1").Scan(&text)
	if !assert.NoError(t, err, "expected no error running tables query") {
		return
	}

	assert.Regexp(t, regexp.MustCompile("[a-zA-Z_]+"), text, "expecting a valid table name: %s", text)
}

package scylladbh_test

import (
	"encoding/json"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	"github.com/uol/gofiles"
	docker "github.com/uol/gotest/docker"
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
	Timeout:           *funks.ForceNewStringDuration("31s"),
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

// TestSuiteDocker - tests a suite of tests using a pod
func TestSuiteDocker(t *testing.T) {

	pod := "test-scylla"

	docker.Remove(pod)

	_, err := docker.StartScylla(pod, "", "", 60*time.Second)
	if !assert.NoError(t, err, "error not expected creating scylla pod") {
		return
	}

	defer docker.Remove(pod)

	suite := []struct {
		title string
		run   func(t *testing.T, pod string)
	}{
		{"normal gocql session", testDockerAutoConnection},
		{"gocql x session", testDockerAutoConnectionX},
	}

	for _, test := range suite {
		t.Run(test.title, func(t *testing.T) {
			test.run(t, pod)
		})
	}
}

// testDockerAutoConnection - test the docker automatic connection
func testDockerAutoConnection(t *testing.T, pod string) {

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

// testDockerAutoConnectionX - test the docker automatic connection x
func testDockerAutoConnectionX(t *testing.T, pod string) {

	conf := &scylladbh.Configuration{
		Nodes: []string{pod},
	}

	sx, err := scylladbh.NewDockerSessionX(conf, "")
	if !assert.NoError(t, err, "error not expected on library x connection") {
		return
	}

	defer sx.Close()

	text := ""

	err = sx.Session.Query("select table_name from system_schema.tables limit 1").Scan(&text)
	if !assert.NoError(t, err, "expected no error running tables query") {
		return
	}

	assert.Regexp(t, regexp.MustCompile("[a-zA-Z_]+"), text, "expecting a valid table name: %s", text)
}

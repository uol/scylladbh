package scylladbh_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/stretchr/testify/assert"
	"github.com/uol/funks"
	"github.com/uol/gofiles"
	"github.com/uol/scylladbh"
)

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

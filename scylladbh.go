package scylladbh

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/uol/funks"
	"github.com/uol/gotest/docker"
)

//
// Helps to configure gocql and gocqlx.
// author: rnojiri
//

const defaultProtoVersion int = 3

// Configuration - the connection configuration
type Configuration struct {
	Nodes             []string       `json:"nodes,omitempty"`
	Port              int            `json:"port,omitempty"`
	NumConnections    int            `json:"numConnections,omitempty"`
	ReconnectInterval funks.Duration `json:"reconnectInterval,omitempty"`
	Keyspace          string         `json:"keyspace,omitempty"`
	Username          string         `json:"username,omitempty"`
	Password          string         `json:"password,omitempty"`
	ProtoVersion      int            `json:"protoVersion,omitempty"`
	Timeout           funks.Duration `json:"timeout,omitempty"`
}

var (
	// ErrNullConf - raised when the configuration is null
	ErrNullConf error = fmt.Errorf("configuration is null")

	// ErrNoNodes - raised when there are no nodes configured
	ErrNoNodes error = fmt.Errorf("no nodes configured")
)

// newSession - generic new session
func newSession(configuration *Configuration, isDocker bool, dockerInspectIPPath string) (*gocql.Session, error) {

	if configuration == nil {
		return nil, ErrNullConf
	}

	if len(configuration.Nodes) == 0 {
		return nil, ErrNoNodes
	}

	if isDocker {

		ips, err := docker.GetIPs(dockerInspectIPPath, configuration.Nodes...)
		if err != nil {
			return nil, err
		}

		configuration.Nodes = ips
	}

	cluster := gocql.NewCluster(configuration.Nodes...)

	if configuration.Port != 0 {
		cluster.Port = configuration.Port
	}

	cluster.Keyspace = configuration.Keyspace

	if len(configuration.Username) > 0 {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: configuration.Username,
			Password: configuration.Password,
		}
	}

	if configuration.NumConnections != 0 {
		cluster.NumConns = configuration.NumConnections
	}

	if configuration.ReconnectInterval.Duration != 0 {
		cluster.ReconnectInterval = configuration.ReconnectInterval.Duration
	}

	if configuration.ProtoVersion != 0 {
		cluster.ProtoVersion = configuration.ProtoVersion
	} else {
		cluster.ProtoVersion = defaultProtoVersion
	}

	if configuration.Timeout.Duration != 0 {
		cluster.Timeout = configuration.Timeout.Duration
	}

	return cluster.CreateSession()
}

// NewSession - creates a new session using gocql
func NewSession(configuration *Configuration) (*gocql.Session, error) {

	return newSession(configuration, false, "")
}

// NewSessionX - creates a new session using gocqlx
func NewSessionX(configuration *Configuration) (*gocqlx.Session, error) {

	session, err := gocqlx.WrapSession(NewSession(configuration))
	if err != nil {
		return nil, err
	}

	return &session, nil
}

// NewDockerSession - connects to a scylla cluster in docker (use nodes parameter to name the pods)
func NewDockerSession(configuration *Configuration, dockerInspectIPPath string) (*gocql.Session, error) {

	return newSession(configuration, true, dockerInspectIPPath)
}

// NewDockerSessionX - connects to a scylla cluster in docker (use nodes parameter to name the pods)
func NewDockerSessionX(configuration *Configuration, dockerInspectIPPath string) (*gocql.Session, error) {

	return NewDockerSession(configuration, dockerInspectIPPath)
}

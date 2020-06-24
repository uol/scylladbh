package scylladbh

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/uol/funks"
)

// Configuration - the connection configuration
type Configuration struct {
	Nodes             []string       `json:"nodes,omitempty"`
	Port              int            `json:"port,omitempty"`
	NumConnections    int            `json:"numConnections,omitempty"`
	ReconnectInterval funks.Duration `json:"reconnectInterval,omitempty"`
	Keyspace          string         `json:"keyspace,omitempty"`
	Username          string         `json:"username,omitempty"`
	Password          string         `json:"password,omitempty"`
}

var (
	// ErrNullConf - raised when the configuration is null
	ErrNullConf error = fmt.Errorf("configuration is null")

	// ErrNoNodes - raised when there are no nodes configured
	ErrNoNodes error = fmt.Errorf("no nodes configured")
)

// NewSession - creates a new session using gocql
func NewSession(configuration *Configuration) (*gocql.Session, error) {

	if configuration == nil {
		return nil, ErrNullConf
	}

	if len(configuration.Nodes) == 0 {
		return nil, ErrNoNodes
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

	return cluster.CreateSession()
}

// NewSessionX - creates a new session using gocqlx
func NewSessionX(configuration *Configuration) (*gocqlx.Session, error) {

	session, err := gocqlx.WrapSession(NewSession(configuration))
	if err != nil {
		return nil, err
	}

	return &session, nil
}

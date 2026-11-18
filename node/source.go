package node

import (
	"github.com/digitalocean/firebolt"
	"github.com/digitalocean/firebolt/ack"
	"github.com/digitalocean/firebolt/fbcontext"
)

// Source is a root node that generates messages to be processed or receives them from an external system.
type Source interface {
	Init(id string, ctx fbcontext.FBContext)
	AcceptsMessage(messageType string) bool
	Setup(config map[string]string, eventchan chan firebolt.Event) error
	Start() error
	Shutdown() error
	Receive(msg fbcontext.Message) error
}

// AckerSource is implemented by sources that support manual acknowledgement of source records.
// When a source implements it, the executor opens an acknowledgement branch per root node and only
// the source itself decides when an offset (or equivalent position) may be committed, based on the
// completion tracking driven by the node hierarchy.  Sources that do not implement it keep the
// framework's default behavior of relying on the source system's automatic offset handling.
type AckerSource interface {
	Acker() *ack.Acker
}

// SourceConfig is the set of configuration values for this Source
type SourceConfig struct {
	Name   string            `yaml:"name"`
	ID     string            `yaml:"id"`
	Params map[string]string `yaml:"params"`
}

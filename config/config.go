package config

import (
	"log"
	"time"
)

// RegisterConfig is used to pass multiple configuration options to the register's constructors.
// Watch out the Immutable set, allow override your KV, shall use default value false.
type RegisterConfig struct {
	Name       string // the name for store the instance, unique. If empty will use Key replace.
	Key        string // register key, unique. The format maybe like: /{scheme}/{service}/{endPoint}.
	Val        string
	TTL        time.Duration
	MaxLoopTry uint64 // default 64, if error and try max times, register effect only KeepAlive.Mode=1.

	MutableVal bool // If true you can override the 'Val', default false. Pls watch out other items shall not change, so dangerous.

	KeepAlive struct {
		Interval time.Duration // default 1s, at least >=100ms, better < TTL, invalid will set TTL-1
		Mode     uint8         // 0=ticker(default, KeepAliveOnce), 1=KeepAlive(not support val update)
	}

	CommonConfig
}

// DiscoverConfig is used to pass multiple configuration options to the discovery's constructors.
type DiscoverConfig struct {
	Name          string // the name for store the instance, unique. If empty will use Scheme, Service replace.
	Scheme        string // register resolver with name scheme, like: services
	Service       string // service name, like: test/v1.0/grpc
	ReturnResolve bool   // if true, will output Resolve info to messages.

	AddressesParser func(string, []byte) (string, error) // parse address, k string, val []byte, return address string.

	CommonConfig
}

// CommonConfig common config used for register and discovery
type CommonConfig struct {
	ChannelBufferSize int // default 256, for errors or messages channel

	// Return specifies what will be populated. If they are set to true,
	// you must read from them to prevent deadlock.
	Return struct {
		// If enabled, any errors that occurred while consuming are returned on
		// the Errors channel (default disabled).
		Errors bool
		// If enabled, any messages that occurred while consuming are returned on
		// the Messages channel (default disabled).
		Messages bool
	}

	ErrorsHandler   func(err <-chan error)     // consume errors, if not set drop.
	MessagesHandler func(string <-chan string) // consume messages expect errors, if not set drop.
	Logger          *log.Logger                // shall not set, use for debug
}

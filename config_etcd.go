package rd

import (
	"log"
	"time"

	disETCD "github.com/v8fg/rd/internal/discovering/etcd"
	regETCD "github.com/v8fg/rd/internal/registering/etcd"
)

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

type RegisterConfig struct {
	Name string // the name for store the instance, unique. If empty will use Key replace.
	Key  string // register key, unique. The format maybe like: /{scheme}/{service}/{endPoint}.
	Val  string
	TTL  time.Duration

	MutableVal bool // If true you can override the 'Val', default false. Pls watch out other items shall not change, so dangerous.

	KeepAlive struct {
		Interval time.Duration // default 1s, at least >=100ms, better < TTL, invalid will set TTL-1
		Mode     uint8         // 0=ticker(default, KeepAliveOnce), 1=KeepAlive
	}

	CommonConfig
}

type DiscoverConfig struct {
	Name    string // the name for store the instance, unique. If empty will use Scheme, Service replace.
	Scheme  string // register resolver with name scheme, like: services
	Service string // service name, like: test/v1.0/grpc

	AddressesParser func(string, []byte) (string, error) // parse address, k string, val []byte, return address string.

	CommonConfig
}

func convertRegisterConfigToInternalETCDRegisterConfig(rgc *RegisterConfig) (rge *regETCD.Config) {
	if rgc != nil {
		rge = &regETCD.Config{
			Name:              rgc.Name,
			Key:               rgc.Key,
			Val:               rgc.Val,
			TTL:               rgc.TTL,
			ChannelBufferSize: rgc.ChannelBufferSize,
			MutableVal:        rgc.MutableVal,
			KeepAlive:         rgc.KeepAlive,
			Return:            rgc.Return,
			ErrorsHandler:     rgc.ErrorsHandler,
			MessagesHandler:   rgc.MessagesHandler,
			Logger:            rgc.Logger,
		}

	}
	return rge
}

func convertDiscoverConfigToInternalETCDDiscoverConfig(dc *DiscoverConfig) (dce *disETCD.Config) {
	if dc != nil {
		dce = &disETCD.Config{
			Name:              dc.Name,
			Scheme:            dc.Scheme,
			Service:           dc.Service,
			ChannelBufferSize: dc.ChannelBufferSize,
			Return:            dc.Return,
			ErrorsHandler:     dc.ErrorsHandler,
			MessagesHandler:   dc.MessagesHandler,
			AddressesParser:   dc.AddressesParser,
			Logger:            dc.Logger,
		}
	}
	return dce
}

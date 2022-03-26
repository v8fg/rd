package etcd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientV3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
)

type none struct{}

type Config struct {
	Scheme     string // register resolver with name schema(key)
	Service    string // Service={serviceKey}/...
	serviceKey string // ServiceKey={scheme}/{serviceKey}/..., prefix.

	ChannelBufferSize int  // default 256, for errors or messages channel
	canCloseClient    bool // If you use input client will true, else false.
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
	ErrorsHandler   func(err <-chan error)       // consume errors, if not set drop.
	MessagesHandler func(string <-chan string)   // consume messages expect errors, if not set drop.
	AddressesParser func([]byte) (string, error) // parse address
	Logger          *log.Logger                  // shall not set, use for debug
}

// discover for etcd
type discover struct {
	client   *clientV3.Client
	config   *Config
	errors   chan error
	messages chan string

	ctx       context.Context
	lock      sync.RWMutex
	closed    chan none
	closeOnce sync.Once

	cc resolver.ClientConn

	cacheAddresses []string
	start          time.Time // used to calculate the time to the present
}

func newDiscover(client *clientV3.Client, config *Config) (*discover, error) {
	return &discover{
		client:   client,
		config:   config,
		errors:   make(chan error, config.ChannelBufferSize),
		messages: make(chan string, config.ChannelBufferSize),
		ctx:      context.Background(),
		closed:   make(chan none),
	}, nil
}

func NewDiscover(config *Config, client *clientV3.Client, etcdConfig clientV3.Config) (*discover, error) {
	err := checkConfig(config)
	if err != nil {
		return nil, fmt.Errorf("NewDiscover err: %w", err)
	}
	if client == nil {
		client, err = newClient(etcdConfig)
		if err != nil {
			return nil, fmt.Errorf("NewRegister create : %w", err)
		}
		config.canCloseClient = true
	} else {
		config.canCloseClient = false
	}

	// watch out &config
	r, err := newDiscover(client, config)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	r.lock.Lock()
	defer r.lock.Unlock()

	return r, err
}

func newClient(config clientV3.Config) (*clientV3.Client, error) {
	client, err := clientV3.New(config)
	if err != nil {
		return nil, fmt.Errorf("create etcd client err: %w", err)
	}

	// fetch memberList
	_, err = client.MemberList(context.Background())
	if err != nil {
		return nil, fmt.Errorf("fetch memberList err: %w", err)
	}

	return client, err
}

func checkConfig(config *Config) error {
	if config == nil {
		return errors.New("checkConfig failed, config nil")
	}
	if len(config.Scheme) == 0 {
		return errors.New("checkConfig failed, Scheme empty")
	}
	if len(config.Service) == 0 {
		return errors.New("checkConfig failed, Service empty")
	}

	if config.AddressesParser == nil {
		return errors.New("checkConfig failed, AddressesParser must not nil")
	}

	// must
	config.serviceKey = fmt.Sprintf("/%s/%s", config.Scheme, config.Service)

	if config.ChannelBufferSize <= 0 {
		config.ChannelBufferSize = 256
	}
	return nil

}

// Errors implements Discover and return errors.
func (r *discover) Errors() <-chan error {
	return r.errors
}

// Messages implements Discover.
func (r *discover) Messages() <-chan string {
	return r.messages
}

// Scheme return etcdV3 schema
func (r *discover) Scheme() string {
	return r.config.Scheme
}

// Build return the resolver.Resolver
func (r *discover) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.cc = cc
	r.handlerMsg(fmt.Sprintf("discover111 Build  key:%v", r.config.serviceKey))
	go r.watch()
	r.handlerMsg(fmt.Sprintf("discover122 Build  key:%v", r.config.serviceKey))
	return r, nil
}

// Run with register resolver
func (r *discover) Run() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.initHandlerAndLogger()
	resolver.Register(r)
	return nil
}

func (r *discover) watch() {
	r.handlerMsg(fmt.Sprintf("discover123 Build  key:%v", r.config.serviceKey))
	keyPrefix := r.config.serviceKey
	addrDict := make(map[string][]byte)
	updateAddr := func() {
		addrList := make([]resolver.Address, 0, len(addrDict))
		cacheAddresses := make([]string, 0, len(addrDict))
		for k, v := range addrDict {
			// parse address
			endPoint, err := r.config.AddressesParser(v)
			if err != nil {
				r.handlerError(fmt.Errorf("discover watch, key:%v, [k=%v, v=%s], err:%w",
					keyPrefix, k, v, err))
				continue
			}
			addrList = append(addrList, resolver.Address{Addr: endPoint})
			cacheAddresses = append(cacheAddresses, endPoint)
		}
		r.cacheAddresses = cacheAddresses
		err := r.cc.UpdateState(resolver.State{Addresses: addrList})
		r.handlerMsg(fmt.Sprintf("discover watch UpdateState, key:%v, addresses:%v, err:%v",
			keyPrefix, cacheAddresses, err))
	}

	resp, err := r.client.Get(r.ctx, keyPrefix, clientV3.WithPrefix())
	if err != nil {
		r.handlerError(fmt.Errorf("discover watch, key:%v, err:%w",
			keyPrefix, err))
	} else {
		for i := range resp.Kvs {
			addrDict[string(resp.Kvs[i].Key)] = resp.Kvs[i].Value
		}
		updateAddr()
	}

	rch := r.client.Watch(r.ctx, keyPrefix, clientV3.WithPrefix())
	for n := range rch {
		for _, ev := range n.Events {
			switch ev.Type {
			case mvccpb.PUT:
				addrDict[string(ev.Kv.Key)] = ev.Kv.Value
				r.handlerMsg(fmt.Sprintf("discover watch etcd[PUT], key:%v, value:%v", string(ev.Kv.Key), ev.Kv.Value))
			case mvccpb.DELETE:
				if ev.PrevKv != nil {
					delete(addrDict, string(ev.PrevKv.Key))
					r.handlerMsg(fmt.Sprintf("discover watch etcd[DELETE], key:%v, current addrDict:%+v",
						string(ev.PrevKv.Key), addrDict))
				} else {
					r.handlerMsg(fmt.Sprintf("discover watch etcd[DELETE], key:%v, current addrDict:%+v",
						string(ev.Kv.Key), addrDict))
				}
			}
		}
		updateAddr()
	}
}

func (r *discover) ResolveNow(rn resolver.ResolveNowOptions) {
	r.handlerMsg(fmt.Sprintf("discover with key:%v, has started:%v, ResolveNow",
		r.config.Service, r.startedTime()))
}

// Close closes the gResolver.
func (r *discover) Close() {
	r.handlerMsg(fmt.Sprintf("discover with key:%v, has started:%v, will shutdown",
		r.config.Service, r.startedTime()))

	var err error

	r.closeOnce.Do(func() {
		if r.config.canCloseClient && r.client != nil {
			err = r.client.Close()
			r.handlerError(fmt.Errorf("discover with key:%v, close client error:%v",
				r.config.Service, err))
		}

		close(r.closed)
	})

	// drain errors and messages
	go func() {
		close(r.errors)
		close(r.messages)
	}()

	for e := range r.errors {
		err = e
	}

	for e := range r.messages {
		_ = e
	}
}

func (r *discover) startedTime() time.Duration {
	return time.Now().Sub(r.start)
}

func (r *discover) initHandlerAndLogger() {
	if r.config.Return.Errors && r.config.ErrorsHandler != nil {
		go func() {
			r.config.ErrorsHandler(r.errors)
		}()
	} else {
		r.config.Return.Errors = false
	}

	if r.config.Return.Messages && r.config.MessagesHandler != nil {
		go func() {
			r.config.MessagesHandler(r.messages)
		}()
	} else {
		r.config.Return.Messages = false
	}

	if (!r.config.Return.Messages || !r.config.Return.Errors) && r.config.Logger == nil {
		r.config.Logger = log.New(io.Discard, "[rd] ", log.LstdFlags|log.Lshortfile)
	}
}

func (r *discover) handlerError(err error) {
	if !r.config.Return.Errors {
		// must consume, or else will block here
		r.config.Logger.Println(err)
		return
	}
	select {
	case <-r.closed:
		// register is closed
		return
	default:
	}

	select {
	case r.errors <- err:
	default:
		// no error listener
	}
}

func (r *discover) handlerMsg(msg string) {
	if !r.config.Return.Messages {
		// must consume, or else will block here
		r.config.Logger.Println(msg)
		return
	}
	select {
	case <-r.closed:
		// register is closed
		return
	default:
	}

	select {
	case r.messages <- msg:
	default:
		// no message listener
	}
}

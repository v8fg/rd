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

const moduleName = "discover-etcd-grpc"

type none struct{}

type Config struct {
	Name    string // the name for registry store the instance, unique. If empty will use the combine: Scheme, Service replace.
	Scheme  string // register resolver with name scheme, like: services
	Service string // service name, like: test/v1.0/grpc

	ReturnResolve     bool // if true, will output Resolve info to messages.
	ChannelBufferSize int  // default 256, for errors or messages channel

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
	ErrorsHandler   func(err <-chan error)               // consume errors, if not set drop.
	MessagesHandler func(string <-chan string)           // consume messages expect errors, if not set drop.
	AddressesParser func(string, []byte) (string, error) // parse address, k string, val []byte, return address string.
	Logger          *log.Logger                          // shall not set, use for debug
}

// discover for etcd
type discover struct {
	client   *clientV3.Client
	config   *Config
	errors   chan error
	messages chan string

	uniKey string // format like: /{scheme}/{service}/..., prefix.

	ctx            context.Context
	lock           sync.RWMutex
	closed         chan none
	closeOnce      sync.Once
	canCloseClient bool // If you use input client will true, else false.

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
		uniKey:   fmt.Sprintf("/%s/%s", config.Scheme, config.Service),
		ctx:      context.Background(),
		closed:   make(chan none),
	}, nil
}

func NewDiscover(config *Config, client *clientV3.Client, etcdConfig *clientV3.Config) (*discover, error) {
	err := checkConfig(config)
	if err != nil {
		return nil, fmt.Errorf("[%s] NewDiscover err: %w", moduleName, err)
	}

	canCloseClient := false
	if client == nil {
		if etcdConfig == nil {
			return nil, fmt.Errorf("[%s] NewDiscover err: %w", moduleName, errors.New("client and etcdConfig, both nil"))
		}

		client, err = newClient(*etcdConfig)
		if err != nil {
			return nil, fmt.Errorf("[%s] NewRegister create : %w", moduleName, err)
		}
		canCloseClient = true
	}

	// watch out &config
	r, err := newDiscover(client, config)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
	r.lock.Lock()
	defer r.lock.Unlock()

	r.canCloseClient = canCloseClient

	return r, err
}

func newClient(config clientV3.Config) (*clientV3.Client, error) {
	client, err := clientV3.New(config)
	if err != nil {
		return nil, fmt.Errorf("[%s] create etcd client err: %w", moduleName, err)
	}

	// fetch memberList
	_, err = client.MemberList(context.Background())
	if err != nil {
		return nil, fmt.Errorf("[%s] fetch memberList err: %w", moduleName, err)
	}

	return client, err
}

func checkConfig(config *Config) error {
	if config == nil {
		return fmt.Errorf("[%s] checkConfig failed, config nil", moduleName)
	}

	if len(config.Scheme) == 0 {
		return fmt.Errorf("[%s] checkConfig failed, Scheme empty", moduleName)
	}
	if len(config.Service) == 0 {
		return fmt.Errorf("[%s] checkConfig failed, Service empty", moduleName)
	}

	if config.AddressesParser == nil {
		return fmt.Errorf("[%s] checkConfig failed, AddressesParser nil", moduleName)
	}

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

// Scheme return etcdV3 scheme
func (r *discover) Scheme() string {
	return r.config.Scheme
}

// Build return the resolver.Resolver
func (r *discover) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	r.cc = cc
	go r.watch()
	return r, nil
}

// Run with register resolver
func (r *discover) Run() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.start = time.Now()

	r.initHandlerAndLogger()
	resolver.Register(r)
	return nil
}

func (r *discover) watch() {
	keyPrefix := r.uniKey
	addrDict := make(map[string][]byte)
	updateAddr := func() {
		addrList := make([]resolver.Address, 0, len(addrDict))
		cacheAddresses := make([]string, 0, len(addrDict))
		for k, v := range addrDict {
			// parse address
			endPoint, err := r.config.AddressesParser(k, v)
			if err != nil {
				r.handlerError(fmt.Errorf("[%s] AddressesParser name:%s, key:%s, [k=%s, v=%s], err:%w",
					moduleName, r.config.Name, keyPrefix, k, v, err))
				continue
			}
			addrList = append(addrList, resolver.Address{Addr: endPoint})
			cacheAddresses = append(cacheAddresses, endPoint)
		}
		r.cacheAddresses = cacheAddresses
		err := r.cc.UpdateState(resolver.State{Addresses: addrList})
		r.handlerMsg(fmt.Sprintf("[%s] UpdateState name:%s, key:%s, addresses:%v, err:%v",
			moduleName, r.config.Name, keyPrefix, cacheAddresses, err))
	}

	resp, err := r.client.Get(r.ctx, keyPrefix, clientV3.WithPrefix())
	if err != nil {
		r.handlerError(fmt.Errorf("[%s] Get name:%s, key:%s, err:%w",
			moduleName, r.config.Name, keyPrefix, err))
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
				r.handlerMsg(fmt.Sprintf("[%s] Watch EventType[PUT], key:%s, value:%s", moduleName,
					string(ev.Kv.Key), ev.Kv.Value))
			case mvccpb.DELETE:
				if ev.PrevKv != nil {
					delete(addrDict, string(ev.PrevKv.Key))
					r.handlerMsg(fmt.Sprintf("[%s] Watch EventType[DELETE], PrevKv key:%s, current addrDict:%+v", moduleName,
						string(ev.PrevKv.Key), addrDict))
				} else {
					// expired del
					delete(addrDict, string(ev.Kv.Key))
					r.handlerMsg(fmt.Sprintf("[%s] Watch EventType[DELETE], key:%s, current addrDict:%+v", moduleName,
						string(ev.Kv.Key), addrDict))
				}
			}
		}
		updateAddr()
	}
}

// ResolveNow so frequent update, stop log out.
func (r *discover) ResolveNow(rn resolver.ResolveNowOptions) {
	if r.config.ReturnResolve {
		r.handlerMsg(fmt.Sprintf("[%s] ResolveNow name:%s, key:%s, startupTime:%s, uptime:%v, cacheAddresses:%v",
			moduleName, r.config.Name, r.uniKey, r.startupTime(), r.uptime(), r.cacheAddresses))
	}
}

// Close closes the gResolver.
func (r *discover) Close() {
	r.handlerMsg(fmt.Sprintf("[%s] Close name:%s, key:%s, startupTime:%s, uptime:%v, will shutdown",
		moduleName, r.config.Name, r.uniKey, r.startupTime(), r.uptime()))

	var err error

	r.closeOnce.Do(func() {
		if r.canCloseClient && r.client != nil {
			err = r.client.Close()
			r.handlerError(fmt.Errorf("[%s] Close name:%s, key:%s, error:%v",
				moduleName, r.config.Name, r.uniKey, err))
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

// uptime already run time
func (r *discover) uptime() time.Duration {
	return time.Now().Sub(r.start)
}

// startupTime the time Run at
func (r *discover) startupTime() string {
	return r.start.Format("2006-01-02T15:04:05.000+0800")
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

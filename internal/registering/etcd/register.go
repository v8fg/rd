package etcd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientV3 "go.etcd.io/etcd/client/v3"
)

type none struct{}

// Config is used to pass multiple configuration options to register's constructors.
// Watch out the Immutable set, allow override your KV, shall use default value false.
type Config struct {
	ServiceKey        string // register key/name, unique, must not update.
	ServiceVal        string
	TTL               time.Duration
	KeepAliveInterval time.Duration // shall less than ttl - 1s ,at least
	ChannelBufferSize int           // default 256, for errors or messages channel
	KeepAliveMode     uint8         // 0=ticker(default, KeepAliveOnce), 1=KeepAlive

	canCloseClient bool // If you use input client will true, else false.
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
	MutableVal      bool                       // If true you can override the 'ServiceVal', default false. Pls watch out other items shall not change, so dangerous.
	ErrorsHandler   func(err <-chan error)     // consume errors, if not set drop.
	MessagesHandler func(string <-chan string) // consume messages expect errors, if not set drop.
	Logger          *log.Logger                // shall not set, use for debug
}

type register struct {
	client   *clientV3.Client
	config   *Config
	errors   chan error
	messages chan string

	ctx       context.Context
	lock      sync.RWMutex
	closed    chan none
	closeOnce sync.Once

	curServiceVal string           // used for updating the old val, if set MutableVal=true
	leaseID       clientV3.LeaseID // auto generate, lease
	start         time.Time        // used to calculate the time to the present
}

func newRegister(client *clientV3.Client, config *Config) (*register, error) {
	return &register{
		client:        client,
		config:        config,
		errors:        make(chan error, config.ChannelBufferSize),
		messages:      make(chan string, config.ChannelBufferSize),
		ctx:           context.Background(),
		closed:        make(chan none),
		curServiceVal: config.ServiceVal,
	}, nil
}

// NewRegister creates a new register with the given configurations.
// Input client preferred, if nil will create with etcdConfig.
func NewRegister(config *Config, client *clientV3.Client, etcdConfig clientV3.Config) (*register, error) {
	err := checkConfig(config)
	if err != nil {
		return nil, fmt.Errorf("NewRegister err: %w", err)
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
	r, err := newRegister(client, config)
	if err != nil {
		_ = client.Close()
		return nil, err
	}
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
	if len(config.ServiceKey) == 0 {
		return errors.New("checkConfig failed, ServiceKey empty")
	}
	if len(config.ServiceVal) == 0 {
		return errors.New("checkConfig failed, ServiceVal empty")
	}
	if config.TTL <= time.Second {
		config.TTL = time.Second
	}
	if config.KeepAliveInterval >= config.TTL {
		config.KeepAliveInterval = config.TTL - time.Second
	}
	if config.KeepAliveInterval <= time.Second {
		config.KeepAliveInterval = time.Second
	}
	if config.KeepAliveMode == 1 {
		config.KeepAliveMode = 1
	} else {
		config.KeepAliveMode = 0
	}
	if config.ChannelBufferSize <= 0 {
		config.ChannelBufferSize = 256
	}
	return nil

}

func (r *register) initHandlerAndLogger() {
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

// Run init handler and start
func (r *register) Run() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	err := r.register()
	if err != nil {
		panic(fmt.Errorf("register key:%v, err: %w", r.config.ServiceKey, err))
	}

	r.initHandlerAndLogger()
	err = r.keepAliveChoose()
	r.start = time.Now()
	return err
}

// Errors implements Register and return errors.
func (r *register) Errors() <-chan error {
	return r.errors
}

// Messages implements Register and return messages.
func (r *register) Messages() <-chan string {
	return r.messages
}

// Close implements Register.
func (r *register) Close() (err error) {
	r.handlerMsg(fmt.Sprintf("register with serviceKey:%v, serviceVal:%v, has started:%v, will shutdown",
		r.config.ServiceKey, r.curServiceVal, r.startedTime()))

	r.closeOnce.Do(func() {
		if r.config.canCloseClient && r.client.Close() != nil {
			if e := r.client.Close(); e != nil {
				err = e
				r.handlerError(fmt.Errorf("register with serviceKey:%v, close clien error:%w",
					r.config.ServiceKey, err))
			}
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

	return err
}

func (r *register) startedTime() time.Duration {
	return time.Now().Sub(r.start)
}

func (r *register) keepAliveChoose() error {
	if r.config.KeepAliveMode == 0 {
		return r.keepAliveOnce(r.ctx, r.config.ServiceKey)
	}

	return r.keepAlive(r.ctx, r.config.ServiceKey)
}

func (r *register) register() error {
	rsp, err := r.client.Grant(r.ctx, int64(r.config.TTL.Seconds()))
	if err != nil {
		return err
	}
	r.leaseID = rsp.ID

	if _, err := r.client.Put(r.ctx, r.config.ServiceKey, r.curServiceVal, clientV3.WithLease(r.leaseID)); err != nil {
		return err
	}
	return nil
}

// keepAliveOnce recommend
func (r *register) keepAliveOnce(ctx context.Context, serviceKey string) error {
	go func() {
		ticker := time.NewTicker(r.config.KeepAliveInterval)
		defer ticker.Stop()

		for {
			select {
			case <-r.closed:
				dp, err := r.client.Delete(context.Background(), serviceKey)
				r.handlerMsg(fmt.Sprintf("keepAliveOnce close, delete key:%v, started:%v, err:%v, response: %v",
					serviceKey, r.startedTime(), err, dp))

				lrp, err := r.client.Revoke(ctx, r.leaseID)
				r.handlerMsg(fmt.Sprintf("keepAliveOnce close, revoke leaseID:%v, started:%v, err:%v, response: %v",
					r.leaseID, r.startedTime(), err, lrp))
				return
			case <-ticker.C:
				leaseKeepAliveResponse, err := r.client.KeepAliveOnce(ctx, r.leaseID)
				r.handlerMsg(fmt.Sprintf("keepAliveOnce key:%v, leaseID:%v, started:%v, err:%v, response: %v",
					r.config.ServiceKey, r.leaseID, r.startedTime(), err, leaseKeepAliveResponse))

				if err == nil {
					if r.config.MutableVal && r.curServiceVal != r.config.ServiceVal {
						oldServiceVal := r.curServiceVal
						r.curServiceVal = r.config.ServiceVal
						if err = r.register(); err != nil {
							r.handlerError(fmt.Errorf("keepAliveOnce change serviceVal, from:%v, to:%v, but failed:%w", oldServiceVal, r.curServiceVal, err))
							r.curServiceVal = oldServiceVal
						} else {
							r.handlerMsg(fmt.Sprintf("keepAliveOnce changed serviceVal, from:%v, to:%v", oldServiceVal, r.curServiceVal))
						}
						continue
					}
				} else {
					// if lease missing, try quick generate new leaseID again
					if errors.Is(err, rpctypes.ErrLeaseNotFound) {
						// requested lease not found
						r.handlerError(fmt.Errorf("keepAliveOnce leaseID:%v, err:%w", r.leaseID, err))
					}
					if err := r.register(); err == nil {
						r.handlerError(fmt.Errorf("keepAliveOnce messages count:%d, re register success", len(r.messages)))
					}
				}
			}
		}
	}()
	return nil
}

// keepAlive start and keepAlive, keep the init val. If you want dynamic update val, replace with keepAliveOnce.
func (r *register) keepAlive(ctx context.Context, serviceKey string) (err error) {
	reps, err := r.client.KeepAlive(ctx, r.leaseID)
	if err != nil {
		r.handlerError(fmt.Errorf("keepAlive leaseID:%v, err:%w", r.leaseID, err))
		return err
	}

	go func() {
		for rsp := range reps {
			r.handlerMsg(fmt.Sprintf("keepAlive key:%v, leaseID:%v, started:%v, response: %v", serviceKey, r.leaseID, r.startedTime(), rsp))
		}
	}()

	go func() {
		<-r.closed
		rsp, err := r.client.Delete(context.Background(), serviceKey)
		r.handlerMsg(fmt.Sprintf("keepAlive close, delete key:%v, err:%v, response:%v", serviceKey, err, rsp))
	}()
	return err
}

func (r *register) handlerError(err error) {
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

func (r *register) handlerMsg(msg string) {
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

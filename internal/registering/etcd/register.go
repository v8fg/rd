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
	"go.uber.org/atomic"
)

const moduleName = "register-etcd"

type none struct{}

// Config is used to pass multiple configuration options to register's constructors.
// Watch out the Immutable set, allow override your KV, shall use default value false.
type Config struct {
	Name string // the name for store the instance, unique. If empty will use Key replace.
	Key  string // register key, unique. The format maybe like: /{scheme}/{service}/{endPoint}.
	Val  string
	TTL  time.Duration

	MaxLoopTry        uint64 // default 64, if error and try max times, register effect only KeepAlive.Mode=1.
	ChannelBufferSize int    // default 256, for errors or messages channel
	MutableVal        bool   // If true you can override the 'Val', default false. Pls watch out other items shall not change, so dangerous.

	KeepAlive struct {
		Interval time.Duration // default 1s, at least >=100ms, better < TTL, invalid will set TTL-1
		Mode     uint8         // 0=ticker(default, KeepAliveOnce), 1=KeepAlive(not support val update)
	}

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

type register struct {
	client   *clientV3.Client
	config   *Config
	errors   chan error
	messages chan string

	uniKey string // format like: /{scheme}/{service}/{endPoint}.

	ctx            context.Context
	lock           sync.RWMutex
	closed         chan none
	closeOnce      sync.Once
	canCloseClient bool // If you use input client will true, else false.

	curServiceVal string           // used for updating the old val, if set MutableVal=true
	leaseID       clientV3.LeaseID // auto generate, lease
	start         time.Time        // used to calculate the time to the present

	curLoopTry             atomic.Uint64
	maxLoopTry             uint64    // default 64
	latestLoopTryTime      time.Time // now - latestLoopTryTime > deltaResetTimeDuration, reset curLoopTry=0
	deltaResetTimeDuration time.Duration

	etcdConfig *clientV3.Config // can use reconnect, if you use config create the etcd client.
}

func newRegister(client *clientV3.Client, config *Config) (*register, error) {
	return &register{
		client:                 client,
		config:                 config,
		errors:                 make(chan error, config.ChannelBufferSize),
		messages:               make(chan string, config.ChannelBufferSize),
		uniKey:                 config.Key, // copy to avoid update this.
		ctx:                    context.Background(),
		closed:                 make(chan none),
		curServiceVal:          config.Val,
		maxLoopTry:             config.MaxLoopTry,
		deltaResetTimeDuration: time.Minute * 1,
	}, nil
}

// NewRegister creates a new register with the given configurations.
// Input client preferred, if nil will create with etcdConfig.
func NewRegister(config *Config, client *clientV3.Client, etcdConfig *clientV3.Config) (*register, error) {
	err := checkConfig(config)
	if err != nil {
		return nil, fmt.Errorf("[%s] NewRegister err: %w", moduleName, err)
	}

	canCloseClient := false
	if client == nil {
		if etcdConfig == nil {
			return nil, fmt.Errorf("[%s] NewRegister err: %w", moduleName, errors.New("client and etcdConfig, both nil"))
		}

		client, err = newClient(*etcdConfig)
		if err != nil {
			return nil, fmt.Errorf("[%s] NewRegister create : %w", moduleName, err)
		}
		canCloseClient = true
	}

	// watch out &config
	r, err := newRegister(client, config)
	if err != nil {
		_ = client.Close()
		return nil, err
	}

	r.lock.Lock()
	defer r.lock.Unlock()

	r.canCloseClient = canCloseClient
	r.etcdConfig = etcdConfig

	return r, err
}

func (r *register) reConnectEtcd() error {
	var err error
	if r.etcdConfig != nil {
		r.client, err = newClient(*r.etcdConfig)
		if err != nil {
			return fmt.Errorf("[%s] reConnectEtcd create : %w", moduleName, err)
		}
	}
	return nil
}

func newClient(config clientV3.Config) (*clientV3.Client, error) {
	client, err := clientV3.New(config)
	if err != nil {
		return nil, fmt.Errorf("[%s] create etcd client err: %w", moduleName, err)
	}

	// fetch memberList
	_, err = client.MemberList(context.Background())
	if err != nil {
		return nil, fmt.Errorf("[%s] fetch etcd memberList err: %w", moduleName, err)
	}

	return client, err
}

func checkConfig(config *Config) error {
	if config == nil {
		return fmt.Errorf("[%s] checkConfig failed, config nil", moduleName)
	}
	if len(config.Key) == 0 {
		return fmt.Errorf("[%s] checkConfig failed, Key empty", moduleName)
	}
	if len(config.Val) == 0 {
		return fmt.Errorf("[%s] checkConfig failed, Val empty", moduleName)
	}
	if config.TTL <= time.Second {
		config.TTL = time.Second
	}
	if config.KeepAlive.Interval >= config.TTL {
		config.KeepAlive.Interval = config.TTL - time.Second
	}
	if config.KeepAlive.Interval < 100*time.Millisecond {
		config.KeepAlive.Interval = time.Second
	}
	if config.KeepAlive.Mode == 1 {
		config.KeepAlive.Mode = 1
	} else {
		config.KeepAlive.Mode = 0
	}
	if config.ChannelBufferSize <= 0 {
		config.ChannelBufferSize = 256
	}
	if config.MaxLoopTry <= 0 {
		config.ChannelBufferSize = 64
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
		panic(fmt.Errorf("[%s] register name:%s, key:%s, err: %w", moduleName, r.config.Name, r.uniKey, err))
	}

	r.start = time.Now()

	r.initHandlerAndLogger()
	err = r.keepAliveChoose()
	return err
}

// Update the store val, shall not call, if no necessary.
func (r *register) Update(val string) error {
	if !r.config.MutableVal {
		return fmt.Errorf("[%s] no permit to update val, as your init MutableVal is false", moduleName)
	}
	r.lock.Lock()
	defer r.lock.Unlock()

	r.curServiceVal = val // judge the change by curServiceVal; config.Val can auto update.
	return nil
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
	r.handlerMsg(fmt.Sprintf("[%s] Close name:%s, key:%s, val:%s, startupTime:%s, uptime:%v, will shutdown",
		moduleName, r.config.Name, r.uniKey, r.curServiceVal, r.startupTime(), r.uptime()))

	r.closeOnce.Do(func() {
		if r.canCloseClient && r.client.Close() != nil {
			if e := r.client.Close(); e != nil {
				err = e
				r.handlerError(fmt.Errorf("[%s] Close name:%s, key:%s, close clien error:%w",
					moduleName, r.config.Name, r.uniKey, err))
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

// uptime already run time
func (r *register) uptime() time.Duration {
	return time.Now().Sub(r.start)
}

// startupTime the time Run at
func (r *register) startupTime() string {
	return r.start.Format("2006-01-02T15:04:05.000+0800")
}

func (r *register) keepAliveChoose() error {
	if r.config.KeepAlive.Mode == 0 {
		return r.keepAliveOnce(r.ctx, r.uniKey)
	}

	return r.keepAlive(r.ctx, r.uniKey)
}

func (r *register) register() error {
	rsp, err := r.client.Grant(r.ctx, int64(r.config.TTL.Seconds()))
	if err != nil {
		return err
	}
	r.leaseID = rsp.ID

	if _, err := r.client.Put(r.ctx, r.uniKey, r.curServiceVal, clientV3.WithLease(r.leaseID)); err != nil {
		return err
	}
	return nil
}

// keepAliveOnce recommend
func (r *register) keepAliveOnce(ctx context.Context, key string) error {
	go func() {
		ticker := time.NewTicker(r.config.KeepAlive.Interval)
		defer ticker.Stop()

		var revision int64
		var raftTerm uint64

		for {
			select {
			case <-r.closed:
				dp, err := r.client.Delete(context.Background(), key)
				r.handlerMsg(fmt.Sprintf("[%s] keepAliveOnce Delete name:%s, key:%s, startupTime:%s, uptime:%v, response: %v, err:%v",
					moduleName, r.config.Name, key, r.startupTime(), r.uptime(), dp, err))

				lrp, err := r.client.Revoke(ctx, r.leaseID)
				r.handlerMsg(fmt.Sprintf("[%s] keepAliveOnce Revoke name:%s, key:%s, leaseID:%v, startupTime:%s, uptime:%v, response: %v, err:%v",
					moduleName, r.config.Name, key, r.leaseID, r.startupTime(), r.uptime(), lrp, err))
				return
			case <-ticker.C:
				leaseKeepAliveResponse, err := r.client.KeepAliveOnce(ctx, r.leaseID)
				if leaseKeepAliveResponse != nil {
					revision = leaseKeepAliveResponse.GetRevision()
					raftTerm = leaseKeepAliveResponse.GetRaftTerm()
				}

				r.handlerMsg(fmt.Sprintf("[%s] keepAliveOnce name:%s, key:%s, leaseID:%v, startupTime:%s, uptime:%v, mutable:%v, revision:%v, raft_term:%v, err:%v",
					moduleName, r.config.Name, key, r.leaseID, r.startupTime(), r.uptime(), r.config.MutableVal, revision, raftTerm, err))

				if err == nil {
					if r.config.MutableVal && r.curServiceVal != r.config.Val {
						oldServiceVal := r.config.Val
						r.config.Val = r.curServiceVal // update old val
						if err = r.register(); err != nil {
							r.handlerError(fmt.Errorf("[%s] keepAliveOnce name:%s, key:%s, change val from:%s, to:%s, but failed:%w",
								moduleName, r.config.Name, key, oldServiceVal, r.curServiceVal, err))
						} else {
							r.handlerMsg(fmt.Sprintf("[%s] keepAliveOnce name:%s, key:%s, changed val from:%s, to:%s",
								moduleName, r.config.Name, key, oldServiceVal, r.curServiceVal))
						}
						continue
					}
				} else {
					// if lease missing, try quick generate new leaseID again
					if errors.Is(err, rpctypes.ErrLeaseNotFound) {
						// requested lease not found
						r.handlerError(fmt.Errorf("[%s] keepAliveOnce name:%s, key:%s, leaseID:%v, err:%w",
							moduleName, r.config.Name, key, r.leaseID, err))
					}
					if err := r.register(); err == nil {
						r.handlerError(fmt.Errorf("[%s] keepAliveOnce name:%s, key:%s, messages count:%d, re register success",
							moduleName, r.config.Name, key, len(r.messages)))
					}
				}
			}
		}
	}()
	return nil
}

// keepAlive start and keepAlive, keep the init val. If you want dynamic update val, replace with keepAliveOnce.
func (r *register) keepAlive(ctx context.Context, key string) (err error) {
	aliveDeal := func() error {
		reps, err := r.client.KeepAlive(ctx, r.leaseID)
		if err != nil {
			r.handlerError(fmt.Errorf("[%s] keepAlive name:%s, key:%v, leaseID:%v, err:%w", moduleName, r.config.Name, key, r.leaseID, err))
			return err
		}
		for rsp := range reps {
			if rsp == nil {
				continue
			}
			r.handlerMsg(fmt.Sprintf("[%s] keepAlive name:%s, key:%s, leaseID:%v, startupTime:%s, uptime:%v, revision:%v, raft_term:%v",
				moduleName, r.config.Name, key, r.leaseID, r.startupTime(), r.uptime(), rsp.GetRevision(), rsp.GetRaftTerm()))
		}
		return nil
	}

	// only first KeepAlive error will return
	if err = aliveDeal(); err != nil {
		return err
	}

loop:
	for {
		r.latestLoopTryTime = time.Now()
		r.curLoopTry.Add(1)
		loopCount := r.curLoopTry.Load()
		if loopCount > r.maxLoopTry {
			r.handlerError(fmt.Errorf("[%s] keepAlive name:%s, key:%v, leaseID:%v, break and exited, over max loopCount:%v",
				moduleName, r.config.Name, key, r.leaseID, r.maxLoopTry))
			break loop
		}

		r.handlerMsg(fmt.Sprintf("[%s] keepAlive enter loop:%v, maxLoopTry:%v, name:%s, key:%s, err:%v", moduleName, loopCount, r.maxLoopTry, r.config.Name, key, err))

		err = aliveDeal()
		if err != nil {
			r.handlerError(fmt.Errorf("[%s] keepAlive name:%s, key:%v, leaseID:%v, err:%w", moduleName, r.config.Name, key, r.leaseID, err))
		}

		select {
		case <-r.closed:
			rsp, err := r.client.Delete(context.Background(), key)
			r.handlerMsg(fmt.Sprintf("[%s] keepAlive close and delete name:%s, key:%s, err:%v, response:%v", moduleName, r.config.Name, key, err, rsp))
			break loop
		default:
		}
		// reset after long time, no loop try
		latestNoLoopDeltaTime := time.Now().Sub(r.latestLoopTryTime)
		if latestNoLoopDeltaTime > r.deltaResetTimeDuration {
			r.curLoopTry.Store(0)
			r.handlerMsg(fmt.Sprintf("[%s] keepAlive reset curLoopTry name:%s, key:%s, latestNoLoopDeltaTime:%v more than r.deltaResetTimeDuration:%v",
				moduleName, r.config.Name, key, latestNoLoopDeltaTime, r.deltaResetTimeDuration))
		}

		if r.client == nil {
			err = r.reConnectEtcd()
			r.handlerMsg(fmt.Sprintf("[%s] keepAlive reConnectEtcd curLoopTry:%v, name:%s, key:%s, err:%v", moduleName, r.curLoopTry, r.config.Name, key, err))
		}

	}
	r.handlerMsg(fmt.Sprintf("[%s] keepAlive break loop and exit name:%s, key:%s, err:%v", moduleName, r.config.Name, key, err))
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

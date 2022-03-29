package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientV3 "go.etcd.io/etcd/client/v3"

	"github.com/v8fg/rd"
	"github.com/v8fg/rd/config"
)

const moduleName = "simple_register"

var (
	errorsHandler = func(errors <-chan error) {
		for errMsg := range errors {
			log.Printf("[%s] errors consume, count:%v, content:%v", moduleName, len(errors), errMsg)
		}
	}
	messagesHandler = func(messages <-chan string) {
		for message := range messages {
			log.Printf("[%s] messages consume, count:%v, content:%v", moduleName, len(messages), message)
		}
	}
	// logger = log.New(log.Writer(), fmt.Sprintf("[%s] ", moduleName), log.LstdFlags|log.Lshortfile)
	logger = log.New(log.Writer(), fmt.Sprintf("[%s] ", moduleName), log.LstdFlags)
	key    = fmt.Sprintf("/services/test/v1.0/grpc/127.0.0.1:33%v", time.Now().Second()+int(rand.Int31n(300)))
)

var cfg = config.RegisterConfig{
	Name: fmt.Sprintf("%s-", moduleName) + time.Now().Format("200601021504"),
	Key:  key,
	Val:  key,
	TTL:  time.Second * 15,
	CommonConfig: config.CommonConfig{
		ChannelBufferSize: 64,
		ErrorsHandler:     errorsHandler,
		MessagesHandler:   messagesHandler,
		Logger:            logger,
	},
	MaxLoopTry: 16,
	MutableVal: true,
	// KeepAliveMode:     1,
}

func main() {
	initRegisterETCD()
	runRegister()
}

func initRegisterETCD() {
	// cfg.Return.Errors = true
	// cfg.Return.Messages = true
	cfg.KeepAlive.Interval = time.Second * 8
	cfg.KeepAlive.Mode = 1
	// cfg.Logger = logger

	err := rd.RegisterEtcd(&cfg, nil, &clientV3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	})

	log.Printf("[%s] initRegisterETCD: %+v, err:%v", moduleName, rd.RegisterInfo(), err)
	if err != nil {
		panic(err)
	}
}

func runRegister() {
	errs := rd.RegisterRun()
	if len(errs) != 0 {
		panic(errs)
		return
	}
	tk := time.NewTicker(time.Second * 20)
	defer tk.Stop()

	quit := make(chan struct{})
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		sig := <-ch
		log.Printf("[%s] received signal: %v", moduleName, sig)
		quit <- struct{}{}
	}()

loop:
	for {
		select {
		case <-tk.C:
			newVal := fmt.Sprintf("127.0.0.1:33%v", time.Now().Second()+int(rand.Int31n(300)))
			err := rd.RegisterUpdateVal(cfg.Name, newVal)
			log.Printf("[%s] runRegister ticker update val:%v, err:%v", moduleName, newVal, err)
		case <-quit:
			errs := rd.RegisterClose()
			if len(errs) != 0 {
				panic(fmt.Sprintf("[%s] close err:%+v", moduleName, errs))
			}
			log.Printf("[%s] close success", moduleName)
			break loop
		}
		// log.Printf("[%s] out select, but in for loop", moduleName)
	}
	log.Printf("[%s] all is done", moduleName)
}

package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	clientV3 "go.etcd.io/etcd/client/v3"

	"github.com/v8fg/rd"
)

var (
	errorsHandler = func(errors <-chan error) {
		for errMsg := range errors {
			fmt.Printf("errors consume, count:%v, content:%v\n", len(errors), errMsg)
		}
	}
	messagesHandler = func(messages <-chan string) {
		for message := range messages {
			fmt.Printf("messages consume, count:%v, content:%v\n", len(messages), message)
		}
	}
	// logger = log.New(log.Writer(), "[rd-test-register] ", log.LstdFlags|log.Lshortfile)
	logger = log.New(log.Writer(), "[rd-test-register] ", log.LstdFlags)
)

var cfg = rd.RegisterConfig{
	Name: "my-rd-test-register" + time.Now().Format("200601021504"),
	Key:  fmt.Sprintf("/services/test/v1.0/grpc/127.0.0.1:33%v", time.Now().Second()+int(rand.Int31n(300))),
	Val:  "first" + time.Now().Format("20060102150405"),
	TTL:  time.Second * 15,
	CommonConfig: rd.CommonConfig{
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

	fmt.Printf("RegisterEtcd: %+v, err:%v\n", rd.RegisterInfo(), err)
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

	done := make(chan struct{})

	go func() {
		<-time.After(time.Second * 6000)
		done <- struct{}{}
	}()

loop:
	for {
		select {
		case <-tk.C:
			newVal := fmt.Sprintf("127.0.0.1:33%v", time.Now().Second()+int(rand.Int31n(300)))
			err := rd.RegisterUpdateVal(cfg.Name, newVal)
			log.Printf("ticker update val:%v, err:%v\n", newVal, err)
		case <-done:
			errs := rd.RegisterClose()
			if len(errs) != 0 {
				panic(fmt.Sprintf("close err:%+v", errs))
			}
			fmt.Printf("close success")
			break loop
		}
		// log.Println("out select, but in for loop")
	}
	log.Println("all is done")
}

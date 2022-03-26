package main

import (
	"fmt"
	"log"
	"time"

	clientV3 "go.etcd.io/etcd/client/v3"

	"github.com/v8fg/rd"
	"github.com/v8fg/rd/internal/registering/etcd"
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
	// logger = log.New(log.Writer(), "[rd-test] ", log.LstdFlags|log.Llongfile)
	logger = log.New(log.Writer(), "[rd-test-register] ", log.LstdFlags|log.Lshortfile)
)

var cfg = etcd.Config{
	ServiceKey:        fmt.Sprintf("/services/test/v1.0/grpc/127.0.0.1:99%v", time.Now().Second()),
	ServiceVal:        "first" + time.Now().Format("20060102150405"),
	TTL:               time.Second * 15,
	KeepAliveInterval: time.Second * 6,
	ChannelBufferSize: 4,
	ErrorsHandler:     errorsHandler,
	MessagesHandler:   messagesHandler,
	MutableVal:        true,
	Logger:            logger,
	// KeepAliveMode:     1,
}

func main() {
	initRegisterETCD()
	runRegister()
}

func initRegisterETCD() {

	// cfg.Return.Errors = true
	// cfg.Return.Messages = true

	err := rd.RegisterEtcd(&cfg, nil, clientV3.Config{
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
	tk := time.NewTicker(time.Second * 3)
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
			cfg.ServiceVal = fmt.Sprintf("127.0.0.1:80%v", time.Now().Second())
		case <-done:
			errs := rd.RegisterClose()
			if len(errs) != 0 {
				panic(fmt.Sprintf("close err:%+v", errs))
				return
			}
			fmt.Printf("close success")
			break loop
		}
		log.Println("out select, but in for loop")
	}
	log.Println("all is done")
}

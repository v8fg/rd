package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientV3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/v8fg/rd"
	"github.com/v8fg/rd/internal/discovering/etcd"
)

var (
	scheme  = "services"
	service = "test/v1.0"
	// builder resolver.Builder
)

func init() {
	initDiscoverGRPCETCD()

	// rd.DiscoverRun()

}
func main() {

	newWithDefaultServiceConfig := grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`)
	_, err := grpc.DialContext(context.TODO(), scheme+"://authority/"+service, newWithDefaultServiceConfig, grpc.WithBlock(), grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	runDiscover()
}

func initDiscoverGRPCETCD() {
	var err error

	errorsHandler := func(errors <-chan error) {
		for errMsg := range errors {
			fmt.Printf("errors consume, count:%v, content:%v\n", len(errors), errMsg)
		}
	}

	messagesHandler := func(messages <-chan string) {
		for message := range messages {
			fmt.Printf("messages consume, count:%v, content:%v\n", len(messages), message)
		}
	}

	addressesParser := func(data []byte) (string, error) {
		fmt.Printf("addressesParser consume, content:%s\n", data)
		return string(data), nil
	}

	// logger := log.New(log.Writer(), "[rd-test] ", log.LstdFlags|log.Llongfile)
	logger := log.New(log.Writer(), "[rd-test-discover] ", log.LstdFlags|log.Lshortfile)
	cfg := etcd.Config{
		Scheme:            "services",
		Service:           "test/v1.0",
		ChannelBufferSize: 16,
		ErrorsHandler:     errorsHandler,
		MessagesHandler:   messagesHandler,
		Logger:            logger,
		AddressesParser:   addressesParser,
	}
	// cfg.Return.Errors = true
	// cfg.Return.Messages = true

	err = rd.DiscoverEtcd(&cfg, nil, clientV3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	})

	errs := rd.DiscoverRun()
	if len(errs) > 0 {
		fmt.Printf("DiscoverRun errors:%v\n", errs)
	}
	fmt.Printf("DiscoverEtcd: %+v, err:%v\n", rd.DiscoverInfo(), err)
	if err != nil {
		panic(err)
	}
}

func runDiscover() {

	tk := time.NewTicker(time.Second * 3)
	defer tk.Stop()

	done := make(chan struct{})

	go func() {
		<-time.After(time.Second * 90)
		done <- struct{}{}
	}()

loop:
	for {
		select {
		case <-tk.C:
		case <-done:
			rd.DiscoverClose()
			fmt.Printf("close success")
			break loop
		}
		log.Println("out select, but in for loop")
	}
	log.Println("all is done")
}

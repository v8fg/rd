package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	clientV3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

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

	addressesParser = func(key string, data []byte) (addr string, err error) {
		fmt.Printf("addressesParser consume, key:%v, data: %s\n", key, data)
		return string(data), err
	}

	addressesParserJSON = func(key string, data []byte) (addr string, err error) {
		fmt.Printf("addressesParser consume, key:%v, data:%s\n", key, data)
		dict := make(map[string]interface{})
		err = json.Unmarshal(data, &dict)
		fmt.Printf("dict:%v, endPoint: %s\n", dict, dict["endPoint"].(string))
		return addr, err
	}

	// logger = log.New(log.Writer(), "[rd-test-discover] ", log.LstdFlags|log.Lshortfile)
	logger = log.New(log.Writer(), "[rd-test-discover] ", log.LstdFlags)

	// 	your register key will be: /{scheme}/{service}
	scheme               = "services"
	service              = "test/v1.0"
	discoverRegistryName = "my-rd-test-discover" + time.Now().Format("200601021504")
)

func init() {
	initDiscoverGRPCETCD()

}

func main() {
	runDiscover()
	newWithDefaultServiceConfig := grpc.WithDefaultServiceConfig(`{"loadBalancingPolicy":"round_robin"}`)
	// _, err := grpc.DialContext(context.TODO(), scheme+"://authority/"+service, newWithDefaultServiceConfig, grpc.WithBlock(), grpc.WithInsecure())
	_, err := grpc.DialContext(context.TODO(), scheme+"://authority/"+service, newWithDefaultServiceConfig, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}

	block()
}

func initDiscoverGRPCETCD() {
	var err error
	cfg := rd.DiscoverConfig{
		Name:    discoverRegistryName,
		Scheme:  scheme,
		Service: service,
		CommonConfig: rd.CommonConfig{
			ChannelBufferSize: 16,
			ErrorsHandler:     errorsHandler,
			MessagesHandler:   messagesHandler,
			Logger:            logger,
		},
		AddressesParser: addressesParser,
	}
	cfg.Return.Errors = true
	cfg.Return.Messages = true

	err = rd.DiscoverEtcd(&cfg, nil, &clientV3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	})
	fmt.Printf("DiscoverEtcd: %+v, err:%v\n", rd.DiscoverInfo(), err)
	if err != nil {
		panic(err)
	}
}

func runDiscover() {
	errs := rd.DiscoverRun()
	if len(errs) > 0 {
		fmt.Printf("DiscoverRun errors:%v\n", errs)
	}
}

func block() {
	log.Printf("[block] block main exit\n")

	tk := time.NewTicker(time.Second * 5)
	defer tk.Stop()

	done := make(chan struct{})

	go func() {
		<-time.After(time.Second * 900)
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
	log.Println("[block] all is done")
}

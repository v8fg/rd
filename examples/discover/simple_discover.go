package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientV3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"

	"github.com/v8fg/rd"
	"github.com/v8fg/rd/config"
)

const moduleName = "simple_discover"

var (
	errorsHandler = func(errors <-chan error) {
		for errMsg := range errors {
			log.Printf("[%s] errors consume, count:%v, content:%v\n", moduleName, len(errors), errMsg)
		}
	}

	messagesHandler = func(messages <-chan string) {
		for message := range messages {
			log.Printf("[%s] messages consume, count:%v, content:%v\n", moduleName, len(messages), message)
		}
	}

	addressesParser = func(key string, data []byte) (addr string, err error) {
		log.Printf("[%s] addressesParser consume, key:%v, data: %s\n", moduleName, key, data)
		return string(data), err
	}

	addressesParserJSON = func(key string, data []byte) (addr string, err error) {
		log.Printf("[%s] addressesParser consume, key:%v, data:%s\n", moduleName, key, data)
		dict := make(map[string]interface{})
		err = json.Unmarshal(data, &dict)
		log.Printf("[%s] dict:%v, endPoint: %s\n", moduleName, dict, dict["endPoint"].(string))
		return addr, err
	}

	// logger = log.New(log.Writer(), fmt.Sprintf("[%s] ", moduleName), log.LstdFlags|log.Lshortfile)
	logger = log.New(log.Writer(), fmt.Sprintf("[%s] ", moduleName), log.LstdFlags)

	// 	your register key will be: /{scheme}/{service}
	scheme               = "services"
	service              = "test/v1.0"
	discoverRegistryName = fmt.Sprintf("%s-", moduleName) + time.Now().Format("200601021504")
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
	cfg := config.DiscoverConfig{
		Name:    discoverRegistryName,
		Scheme:  scheme,
		Service: service,
		CommonConfig: config.CommonConfig{
			ChannelBufferSize: 16,
			ErrorsHandler:     errorsHandler,
			MessagesHandler:   messagesHandler,
			Logger:            logger,
		},
		AddressesParser: addressesParser,
		ReturnResolve:   true,
	}
	cfg.Return.Errors = true
	cfg.Return.Messages = true

	err = rd.DiscoverEtcd(&cfg, nil, &clientV3.Config{
		Endpoints:   []string{"127.0.0.1:2379"},
		DialTimeout: time.Second * 5,
	})
	log.Printf("[%s] initDiscoverGRPCETCD: %+v, err:%v\n", moduleName, rd.DiscoverInfo(), err)
	if err != nil {
		panic(err)
	}
}

func runDiscover() {
	errs := rd.DiscoverRun()
	if len(errs) > 0 {
		log.Printf("[%s] runDiscover errors:%v", moduleName, errs)
	}
}

func block() {
	log.Printf("[%s] block enter block main", moduleName)

	tk := time.NewTicker(time.Second * 5)
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
		case <-quit:
			rd.DiscoverClose()
			log.Printf("[%s] block close success and exit block main", moduleName)
			break loop
		}
		log.Printf("[%s] out select, but in for loop", moduleName)
	}
	log.Printf("[%s] block all is done", moduleName)
}

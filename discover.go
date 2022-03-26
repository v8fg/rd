package rd

import (
	clientV3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"

	"github.com/v8fg/rd/internal/discovering"
	"github.com/v8fg/rd/internal/discovering/etcd"
)

var (
	discoverRegistry = discovering.NewDiscoverRegistry()
)

func init() {
}

// DiscoverEtcd etcd discover with some configurations.
func DiscoverEtcd(config *etcd.Config, client *clientV3.Client, etcdConfig clientV3.Config) error {
	return discoverRegistry.Register(config, client, etcdConfig)
}

// DiscoverInfo return the basic info about discover: service key and discover addr.
func DiscoverInfo() string {
	return discoverRegistry.Info()
}

func Resolver(serviceKey string) resolver.Builder {
	return discoverRegistry.Resolver(serviceKey)
}

// DiscoverRun the registers have been registered.
func DiscoverRun() []error {
	return discoverRegistry.Run()
}

// DiscoverRunService run discover have been registered and with the specified service key or name.
func DiscoverRunService(service string) []error {
	return discoverRegistry.RunService(service)
}

// DiscoverClose close all the running discovers.
func DiscoverClose() {
	discoverRegistry.Close()
}

// DiscoverCloseService close all the running discovers with the specified service key or name.
func DiscoverCloseService(service string) {
	discoverRegistry.CloseService(service)
}

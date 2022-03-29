package rd

import (
	clientV3 "go.etcd.io/etcd/client/v3"

	"github.com/v8fg/rd/config"
	"github.com/v8fg/rd/internal/discovering"
)

var (
	discoverRegistry = discovering.NewDiscoverRegistry()
)

func init() {
}

// DiscoverEtcd etcd discover with some configurations.
// registry key: name or key, the name preferred.
func DiscoverEtcd(config *config.DiscoverConfig, client *clientV3.Client, etcdConfig *clientV3.Config) error {
	return discoverRegistry.Register(config, client, etcdConfig)
}

// DiscoverInfo return the basic info about discover: key and discover addr.
func DiscoverInfo() string {
	return discoverRegistry.Info()
}

// Resolver shall call auto by the run
// func Resolver(scheme, service string) resolver.Builder {
// 	return discoverRegistry.Resolver(scheme, service)
// }

// DiscoverRun the registers have been registered.
func DiscoverRun() []error {
	return discoverRegistry.Run()
}

// DiscoverRunWithParam run discover have been registered with the specified name or the combine: scheme and service.
func DiscoverRunWithParam(name, scheme, service string) []error {
	return discoverRegistry.RunWithParam(name, scheme, service)
}

// DiscoverClose close all the running discovers.
func DiscoverClose() {
	discoverRegistry.Close()
}

// DiscoverCloseWithParam close all the running discovers with the specified name or the combine: scheme and service.
func DiscoverCloseWithParam(name, scheme, service string) {
	discoverRegistry.CloseWithParam(name, scheme, service)
}

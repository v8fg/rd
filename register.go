package rd

import (
	clientV3 "go.etcd.io/etcd/client/v3"

	"github.com/v8fg/rd/internal/registering"
	"github.com/v8fg/rd/internal/registering/etcd"
)

var (
	registerRegistry = registering.NewRegisterRegistry()
)

func init() {
}

// RegisterEtcd etcd register with some configurations.
func RegisterEtcd(config *etcd.Config, client *clientV3.Client, etcdConfig clientV3.Config) error {
	return registerRegistry.Register(config, client, etcdConfig)
}

// RegisterInfo return the basic info about register: service key and register addr.
func RegisterInfo() string {
	return registerRegistry.Info()
}

// RegisterRun the registers have been registered.
func RegisterRun() []error {
	return registerRegistry.Run()
}

// RegisterRunService run the register have been registered and with the specified service key or name.
func RegisterRunService(service string) []error {
	return registerRegistry.RunService(service)
}

// RegisterClose close all the running registers.
func RegisterClose() []error {
	return registerRegistry.Close()
}

// RegisterCloseService close all the running registers with the specified service key or name.
func RegisterCloseService(service string) []error {
	return registerRegistry.CloseService(service)
}

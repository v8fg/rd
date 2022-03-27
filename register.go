package rd

import (
	"log"

	clientV3 "go.etcd.io/etcd/client/v3"

	"github.com/v8fg/rd/internal/registering"
)

var (
	registerRegistry = registering.NewRegisterRegistry()
)

func init() {
}

// RegisterEtcd etcd register with some configurations.
// registry key: name or key, the name preferred.
func RegisterEtcd(config *RegisterConfig, client *clientV3.Client, etcdConfig *clientV3.Config) error {
	_cfg := convertRegisterConfigToInternalETCDRegisterConfig(config)
	return registerRegistry.Register(_cfg, client, etcdConfig)
}

// RegisterInfo return the basic info about register: key and register addr.
func RegisterInfo() string {
	return registerRegistry.Info()
}

// RegisterRun the registers have been registered.
func RegisterRun() []error {
	return registerRegistry.Run()
}

// RegisterRunWithParam run the register have been registered and with the specified name or key.
func RegisterRunWithParam(nameOrKey string) []error {
	return registerRegistry.RunWithParam(nameOrKey)
}

// RegisterClose close all the running registers.
func RegisterClose() []error {
	return registerRegistry.Close()
}

// RegisterCloseWithParam close all the running registers with the specified key or name.
func RegisterCloseWithParam(nameOrKey string) []error {
	return registerRegistry.CloseWithParam(nameOrKey)
}

// RegisterUpdateVal update the register val, shall not call, if no necessary.
// If you want call success, must set MutableVal=true when init the register.
func RegisterUpdateVal(nameOrKey, val string) error {
	log.Printf("RegisterUpdateVal nameOrKey:%v, val:%v", nameOrKey, val)
	return registerRegistry.UpdateVal(nameOrKey, val)
}

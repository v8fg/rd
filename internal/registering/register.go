package registering

import (
	"errors"
	"fmt"
	"sync"

	clientV3 "go.etcd.io/etcd/client/v3"

	"github.com/v8fg/rd/internal/registering/etcd"
)

// Register register interface
type Register interface {
	Run() error
	Errors() <-chan error
	Messages() <-chan string
	Close() error
}

// RegisterRegistry can choose an appropriate Register based on the provided plugin, with service key.
type RegisterRegistry struct {
	registers map[string]Register

	mu sync.RWMutex
}

// NewRegisterRegistry returns a new, initialized RegisterRegistry.
func NewRegisterRegistry() *RegisterRegistry {
	return &RegisterRegistry{
		registers: make(map[string]Register),
	}
}

// Info return the basic info, service key and Register addr.
func (r *RegisterRegistry) Info() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r != nil {
		regK := make([]string, 0, len(r.registers))
		for k, v := range r.registers {
			regK = append(regK, fmt.Sprintf("name:%v, addr:%p", k, v))
		}
		return fmt.Sprintf("%v", regK)
	}
	return ""
}

// Register the service with some configurations. You can pass the etcd client or only pass the related config items.
func (r *RegisterRegistry) Register(config *etcd.Config, client *clientV3.Client, etcdConfig clientV3.Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.registers[config.ServiceKey]; ok {
		return fmt.Errorf("register %v falied, existed", config.ServiceKey)
	}

	reg, err := etcd.NewRegister(config, client, etcdConfig)
	if err != nil {
		return err
	}

	r.registers[config.ServiceKey] = reg
	return nil
}

// Run the registers have been registered.
func (r *RegisterRegistry) Run() (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	errs = make([]error, 0, len(r.registers))
	for k, register := range r.registers {
		if err := register.Run(); err != nil {
			errs = append(errs, fmt.Errorf("run register key:%v, err:%w", k, err))
		}
	}
	if len(r.registers) == 0 {
		errs = append(errs, errors.New("no registers here, pls register, then run"))
	}
	return errs
}

// RunService run the register have been registered and with the specified service key or name.
func (r *RegisterRegistry) RunService(serviceKey string) (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	errs = make([]error, 0, len(r.registers))
	if register, ok := r.registers[serviceKey]; ok {
		if err := register.Run(); err != nil {
			errs = append(errs, fmt.Errorf("run register key:%v, err:%w", serviceKey, err))
		}
	} else {
		errs = append(errs, fmt.Errorf("no register with key:%v, pls register, then run", serviceKey))
	}
	return errs
}

// Close all the running registers.
func (r *RegisterRegistry) Close() (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	errs = make([]error, 0, len(r.registers))
	for k, register := range r.registers {
		if err := register.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close register key:%v, err:%w", k, err))
			delete(r.registers, k)
		}
	}
	return errs
}

// CloseService close all the running registers with the specified service key or name.
func (r *RegisterRegistry) CloseService(serviceKey string) (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	errs = make([]error, 0, len(r.registers))
	if register, ok := r.registers[serviceKey]; ok {
		if err := register.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close register key:%v, err:%w", serviceKey, err))
			delete(r.registers, serviceKey)
		}
	}
	return errs
}

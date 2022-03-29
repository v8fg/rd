package discovering

import (
	"errors"
	"fmt"
	"sync"

	clientV3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"

	"github.com/v8fg/rd/config"
	"github.com/v8fg/rd/internal/discovering/etcd"
)

type Discover interface {
	Scheme() string
	Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error)
	Run() error
	Errors() <-chan error
	Messages() <-chan string
	Close()
}

// DiscoverRegistry can choose an appropriate Discover based on the provided, with name or service prefix key.
type DiscoverRegistry struct {
	discovers map[string]Discover

	mu sync.RWMutex
}

// NewDiscoverRegistry returns a new, initialized DiscoverRegistry.
func NewDiscoverRegistry() *DiscoverRegistry {
	return &DiscoverRegistry{
		discovers: make(map[string]Discover),
	}
}

func generateStandardKey(scheme, service string) string {
	return fmt.Sprintf("/%s/%s", scheme, service)
}

// Info return the basic info, service key.
func (r *DiscoverRegistry) Info() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r != nil {
		regK := make([]string, 0, len(r.discovers))
		for k, v := range r.discovers {
			regK = append(regK, fmt.Sprintf("key:%v, addr:%p", k, v))
		}
		return fmt.Sprintf("%v", regK)
	}
	return ""
}

// func (r *DiscoverRegistry) Resolver(scheme, service string) resolver.Builder {
// 	r.mu.RLock()
// 	defer r.mu.RUnlock()
//
// 	key := generateStandardKey(scheme, service)
// 	if discover, ok := r.discovers[key]; ok {
// 		return discover
// 	}
// 	return nil
// }

// Register the Discover for the service.
func (r *DiscoverRegistry) Register(config *config.DiscoverConfig, client *clientV3.Client, etcdConfig *clientV3.Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if config == nil || client == nil && etcdConfig == nil {
		return fmt.Errorf("register discover falied, config, client or etcdConfig invalid")
	}

	k := generateStandardKey(config.Scheme, config.Service)

	realKey := config.Name
	if len(realKey) == 0 {
		realKey = k
	}

	if _, ok := r.discovers[realKey]; ok {
		return fmt.Errorf("register discovers[%v] falied, existed", realKey)
	}

	dis, err := etcd.NewDiscover(config, client, etcdConfig)
	if err != nil {
		return err
	}

	r.discovers[realKey] = dis
	return nil
}

// Run the discovers have been registered.
func (r *DiscoverRegistry) Run() (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	errs = make([]error, 0, len(r.discovers))
	for k, discover := range r.discovers {
		if err := discover.Run(); err != nil {
			errs = append(errs, fmt.Errorf("run discovers[%v], err:%w", k, err))
		}
	}
	if len(r.discovers) == 0 {
		errs = append(errs, errors.New("run discovers here, pls register first"))
	}
	return
}

// RunWithParam run discover has been registered and with the specified name or the combine: scheme and service.
func (r *DiscoverRegistry) RunWithParam(name, scheme, service string) (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := generateStandardKey(scheme, service)
	realKey := name
	if len(realKey) == 0 {
		realKey = k
	}

	if discover, ok := r.discovers[realKey]; ok {
		if err := discover.Run(); err != nil {
			errs = append(errs, fmt.Errorf("run discovers[%v], err:%w", realKey, err))
		}
	} else {
		errs = append(errs, fmt.Errorf("run discovers[%v] failed, no discover here, pls register first", realKey))
	}
	return
}

// Close all the discovers.
func (r *DiscoverRegistry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for k, discover := range r.discovers {
		discover.Close()
		delete(r.discovers, k)
	}
}

// CloseWithParam close all the running discovers with the specified name or the combine: scheme and service.
func (r *DiscoverRegistry) CloseWithParam(name, scheme, service string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	k := generateStandardKey(scheme, service)
	realKey := name
	if len(realKey) == 0 {
		realKey = k
	}
	if discover, ok := r.discovers[realKey]; ok {
		discover.Close()
		delete(r.discovers, realKey)
	}
}

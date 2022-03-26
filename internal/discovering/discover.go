package discovering

import (
	"errors"
	"fmt"
	"sync"

	clientV3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"

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

// DiscoverRegistry can choose an appropriate Discover based on the provided, with service prefix key.
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

// Info return the basic info, service key.
func (r *DiscoverRegistry) Info() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r != nil {
		regK := make([]string, 0, len(r.discovers))
		for k, v := range r.discovers {
			regK = append(regK, fmt.Sprintf("name:%v, addr:%p", k, v))
		}
		return fmt.Sprintf("%v", regK)
	}
	return ""
}

func (r *DiscoverRegistry) Resolver(serviceKey string) resolver.Builder {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if discover, ok := r.discovers[serviceKey]; ok {
		return discover
	}
	return nil
}

// Register the Discover for the service.
func (r *DiscoverRegistry) Register(config *etcd.Config, client *clientV3.Client, etcdConfig clientV3.Config) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.discovers[config.Service]; ok {
		return fmt.Errorf("discover %v falied, existed", config.Service)
	}

	dis, err := etcd.NewDiscover(config, client, etcdConfig)
	if err != nil {
		return err
	}

	r.discovers[config.Service] = dis
	return nil
}

// Run the discovers have been registered.
func (r *DiscoverRegistry) Run() (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	errs = make([]error, 0, len(r.discovers))
	for k, discover := range r.discovers {
		// resolver.Register(discover)
		if err := discover.Run(); err != nil {
			errs = append(errs, fmt.Errorf("discover key:%v, err:%w", k, err))
		}
	}
	if len(r.discovers) == 0 {
		errs = append(errs, errors.New("no discovers here, pls register, then run"))
	}
	return
}

// RunService run discover has been registered and with the specified service key or name.
func (r *DiscoverRegistry) RunService(serviceKey string) (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if discover, ok := r.discovers[serviceKey]; ok {
		if err := discover.Run(); err != nil {
			errs = append(errs, fmt.Errorf("discover key:%v, err:%w", serviceKey, err))
		}
	} else {
		errs = append(errs, fmt.Errorf("no discover with key:%v, pls register, then run", serviceKey))
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

// CloseService close all the running discovers with the specified service key or name.
func (r *DiscoverRegistry) CloseService(serviceKey string) (errs []error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if discover, ok := r.discovers[serviceKey]; ok {
		discover.Close()
		delete(r.discovers, serviceKey)
	}
	return errs
}

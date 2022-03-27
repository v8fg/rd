# rd

Register and Discovery, will support etcd, consul, etc.

## feature

- Easy configuration, service registration and discovery.
- Support component error message and process information subscription.
- Configure your service discovery content resolution handler freely.
- Access or automatically generate component clients based on your configuration.

### support plugins

| plugin                                        | register | discover | state   | note |
|:----------------------------------------------|:---------|:---------|:--------|:-----|
| [etcd](https://github.com/etcd-io/etcd)       | ✅        | ✅        | **Pre** ||
| [consul](https://github.com/hashicorp/consul) ||||

## usage

>`go get github.com/v8fg/rd`

- [register](./examples/register)
- [discover](./examples/discover)

## oncache

Oncache provides a key-value cache with to a peer-to-peer broadcast system to deliver
invalidation messages. Its primary purpose is an *easy-to-deploy* cache layer for a
multi-node deployment, where the cache application is attached to every node, preventing a
single point of failure. Message delivery is O(n), so it is not intended for large scale.
The use case aims for clusters with less than 50 nodes, common in multi-tenant
architecture or private on-premises deployments.

Shared-key encryption is used to authenticate nodes in the cluster and protect the data in
transit. The messaging network can also be used for broadcasting other general text
messages to all nodes.

The cache is not meant to be a source of truth, but rather an optimization. Invalidation
messages delete cache keys, so nodes will refresh data from the source. Custom messages
can also be implemented to further optimize data sharing to avoid expensive queries.

Initial node discovery is outside of the scope of this package. The host application
maintains the list of nodes to connect to, e.g., from a K8s query or configuration file.

### Usage

```sh
go get go.mukunda.com/oncache
```

```
onc := oncache.New()
onc.Init(
TODO: usage, examples, docs, etc.

### License

Licensed under MIT. See LICENSE for details.

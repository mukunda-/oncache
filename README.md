## oncache

Oncache provides a key-value cache in addition to a peer-to-peer broadcast system to
deliver invalidation messages. Its purpose is an *easy-to-deploy* cache layer for a
multi-node deployment. Invalidation delivery is O(n), so it is not intended for large
scale.

### Usage

```sh
go get go.mukunda.com/oncache
```

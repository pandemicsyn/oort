# ortkv

### A fast persistent clustered key/value store

"It's not the future" ~ wreese

### Installation

You need godep.

* go get -u github.com/pandemicsyn/ort
* cd $GOPATH/src/github.com/pandemicsyn/ort
* make build or make run or make packages

### Daemons/Binaries/Backends

* ortd - The redis speaking storage daemon
* ort-bench - The redis speaking simple benchmark utility
* mapstore - The map based kv storage
* valuestore - gholt's sstable/bitcask like persistent value store

# Building packages

Requires fpm.

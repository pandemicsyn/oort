# photonkv

### A fast persistent clustered key/value store

"It's not the future" ~ wreese

### Installation

* go get -u github.com/pandemicsyn/photon
* cd $GOPATH/src/github.com/pandemicsyn/photon
* go install ./photond or go build it

### Daemons/Binaries/Backends

* photond - The redis speaking storage daemon
* photon-bench - The redis speaking simple benchmark utility
* mapstore - The map based kv storage
* valuestore - gholt's sstable/bitcask like persistent value store


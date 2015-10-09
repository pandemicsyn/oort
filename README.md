# ortkv

### A fast persistent clustered key/value store

"It's not the future" ~ wreese

### Installation

You need godep.

* go get -u github.com/pandemicsyn/ort
* go install github.com/pandemicsyn/ort/ortd
* mkdir -p /etc/ort/ortd
* If you'll be using the CmdCtrl interface you'll need to deploy your SSL key/crt to /etc/ort or whatever path you've specified in the ring config.

### Ort Daemons/Binaries/Backends

* ortd - The redis speaking storage daemon
* ort-bench - The redis speaking simple benchmark utility
* mapstore - The map based kv storage
* valuestore - gholt's sstable/bitcask like persistent value store


### Other temporary (i.e. testing) stuff

* cfs - A test fuse file system you don't want to use for anything.
* apid - The frontend api server that's the relay between fuse and ortd

# Building packages

Requires fpm.

# installing a non-dev instance (i.e. you actually want an init script)

Make sure you have a synd instance setup and running.

1. mkdir -p /etc/ort/ortd
2. touch /etc/default/ortd
3. go get -u github.com/pandemicsyn/ort/ortd && go install -a github.com/pandemicsyn/ort/ortd
4. cp -av $GOHOME/github.com/pandemicsyn/ort/packaging/root/usr/share/ort/systemd/ortd.service /lib/systemd/system
5. cp -av $GOHOME/github.com/pandemicsyn/ort/packaging/root/etc/ort/server.crt /etc/ort
6. cp -av $GOHOME/github.com/pandemicsyn/ort/packaging/root/etc/ort/server.key /etc/ort
7. systemctl start ortd
8. journalctl -u ortd -f

# Testing out a POC using cfs -> apid -> ortd

### ort-syndicate

Ortd obtains configuration info and the rings by communicate with a running ort-syndicate server (synd).
To discover the syndicate server it either attempts to use SRV records. The SRV record should be structured as follows:

```_syndicate._tcp.iad3.velocillama.com. 300 IN SRV 1 1 8443 syndicate1.iad3.velocillama.com.```

The service needs to be "syndicate", "proto" should be tcp. The rest of the service address (the iad3.velocillama.com portion) is derived from the systems local hostname. So if the local hostname is "devmachine.iad3.domain.com" the service record it would look for would be `_syndicate._tcp.iad3.domain.com`. The address and port target should be the address and port of your running synd instance. If you're running a local dev instance and don't have or want to setup a DNS record you can also use "env ORT_SYNDICATE_OVERRIDE=127.0.0.1:8443" to fake a return SRV record.

The boot sequence for Ort at the moment is:

1. Look for a cached config in /var/cache/ortd-config.cache and apply it if present and not stale
    1. Load the cached ring config
    2. Overlay the cached ring node config
2. If SKIP_SRV is set
    1. Generated SRV service id
    2. Query SRV record to find Syndicate host
        1. If ORT_SYNDICATE_OVERRIDE is present the SRV records isn't actually queried the override address is returned
    3. Register with syndicate host
    4. Use return Ring Local ID and Ring
        1. load the ring conf
        2. Overlay the ring node config
3. Bind to 0.0.0.0:6139
4. Start up the Gholt ValueStore using the layered config.

While not recommended, you can by pass SRV lookups and Syndicate usage completely by setting the following env vars:

- `ORTD_SKIP_SRV=true`
- `ORT_LISTEN_ADDRESS=something`
- `ORT_LOCALID=1010101010101`
- `ORT_RING_FILE=/path/to/ring`

### Run a backend

1. Fire up ortd

### Run apid the api server
1. cd apid/; go run main.go

### Build and mount an instance of cfs
1. cd cfs; go build .
2. mkdir /mnt/test
3. ./cfs -debug=true /mnt/test

# Testing out ort with the gholt valuestore and replication:



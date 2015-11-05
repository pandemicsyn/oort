oort

### A fast persistent clustered key/value store

"It's not the future" ~ wreese

### Installation

You need godep.

* go get -u github.com/pandemicsyn/oort
* go install github.com/pandemicsyn/oort/oortd
* mkdir -p /etc/oort/oortd
* If you'll be using the CmdCtrl interface you'll need to deploy your SSL key/crt to /etc/oort or whatever path you've specified in the ring config.

### Oort Daemons/Binaries/Backends

* oortd - The redis speaking storage daemon
* oort-bench - The redis speaking simple benchmark utility
* mapstore - The map based kv storage
* valuestore - gholt's sstable/bitcask like persistent value store

Requires fpm.

# installing a non-dev instance (i.e. you actually want an init script)

Make sure you have a synd instance setup and running.

1. mkdir -p /etc/oort/oortd
2. touch /etc/default/oortd
3. go get -u github.com/pandemicsyn/oort/oortd && go install -a github.com/pandemicsyn/oort/oortd
4. cp -av $GOHOME/github.com/pandemicsyn/oort/packaging/root/usr/share/oort/systemd/oortd.service /lib/systemd/system
5. cp -av $GOHOME/github.com/pandemicsyn/oort/packaging/root/etc/oort/server.crt /etc/oort
6. cp -av $GOHOME/github.com/pandemicsyn/oort/packaging/root/etc/oort/server.key /etc/oort
7. systemctl start oortd
8. journalctl -u oortd -f

# Testing out a POC using cfs -> formic -> oortd

### syndicate

Oortd obtains configuration info and the rings by communicate with a running syndicate server (synd).
To discover the syndicate server it either attempts to use SRV records. The SRV record should be structured as follows:

```_syndicate._tcp.iad3.velocillama.com. 300 IN SRV 1 1 8443 syndicate1.iad3.velocillama.com.```

The service needs to be "syndicate", "proto" should be tcp. The rest of the service address (the iad3.velocillama.com portion) is derived from the systems local hostname. So if the local hostname is "devmachine.iad3.domain.com" the service record it would look for would be `_syndicate._tcp.iad3.domain.com`. The address and port target should be the address and port of your running synd instance. If you're running a local dev instance and don't have or want to setup a DNS record you can also use "env OORT_SYNDICATE_OVERRIDE=127.0.0.1:8443" to fake a return SRV record.

The boot sequence for Oort at the moment is:

1. Look for a cached config in /var/cache/oortd-config.cache and apply it if present and not stale
    1. Load the cached ring config
    2. Overlay the cached ring node config
2. If SKIP_SRV is set
    1. Generated SRV service id
    2. Query SRV record to find Syndicate host
        1. If OORT_SYNDICATE_OVERRIDE is present the SRV records isn't actually queried the override address is returned
    3. Register with syndicate host
    4. Use return Ring Local ID and Ring
        1. load the ring conf
        2. Overlay the ring node config
3. Bind to 0.0.0.0:6139
4. Start up the Gholt ValueStore using the layered config.

While not recommended, you can by pass SRV lookups and Syndicate usage completely by setting the following env vars:

- `OORTD_SKIP_SRV=true`
- `OORT_LISTEN_ADDRESS=something`
- `OORT_LOCALID=1010101010101`
- `OORT_RING_FILE=/path/to/ring`

### Run a backend

1. Fire up oortd

### Checkout and run formic

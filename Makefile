SHA := $(shell git rev-parse --short HEAD)
VERSION := $(shell cat VERSION)
ITTERATION := $(shell date +%s)

build:
	mkdir -p packaging/output
	mkdir -p packaging/root/usr/local/bin
	godep go build -o packaging/root/usr/local/bin/ortd github.com/pandemicsyn/ort/ortd

clean:
	rm -rf packaging/output
	rm -f packaging/root/usr/local/bin/ortd

install:
	install -t /usr/local/bin packaging/root/usr/local/bin/ortd

run:
	@godep go run ortd/main.go

test:
	@godep go test ./...

ring:
	ring /tmp/ort.builder create replicas=3
	ring /tmp/ort.builder add active=true capacity=1000 tier0=server1 tier1=z1 address0=127.0.0.1:8001 address1=127.0.0.2:8001 meta=onmetalv1
	ring /tmp/ort.builder add active=true capacity=1000 tier0=server2 tier1=z2 address0=127.0.0.1:8002 address1=127.0.0.2:8002 meta=onmetalv1
	ring /tmp/ort.builder add active=true capacity=1000 tier0=server3 tier1=z3 address0=127.0.0.1:8003 address1=127.0.0.2:8003 meta=onmetalv1
	ring /tmp/ort.builder ring

packages: clean build deb

deb:
	fpm -s dir -t deb -n ortd -v $(VERSION) -p packaging/output/ortd-$(VERSION)_amd64.deb \
		--deb-priority optional --category admin \
		--force \
		--iteration $(ITTERATION) \
		--deb-compression bzip2 \
	 	--after-install packaging/scripts/postinst.deb \
	 	--before-remove packaging/scripts/prerm.deb \
		--after-remove packaging/scripts/postrm.deb \
		--url https://github.com/pandemicsyn/ort \
		--description "Some sorta storage thingy" \
		-m "Florian Hines <syn@ronin.io>" \
		--license "Apache License 2.0" \
		--vendor "ort" -a amd64 \
		--config-files /etc/ort/ortd.toml-sample \
		packaging/root/=/
	cp packaging/output/ortd-$(VERSION)_amd64.deb packaging/output/ortd.deb.$(SHA)

SHA := $(shell git rev-parse --short HEAD)
VERSION := $(shell cat VERSION)
ITTERATION := $(shell date +%s)
LOCALPKGS :=  $(shell go list ./... | grep -v /vendor/)

deps:
	go get -f -u $(LOCALPKGS)

build:
	mkdir -p packaging/output
	mkdir -p packaging/root/usr/local/bin
	go build -i -v -o packaging/root/usr/local/bin/oort-valued --ldflags " \
		-X main.ringVersion=$(shell git -C $$GOPATH/src/github.com/gholt/ring rev-parse HEAD) \
		-X main.oortVersion=$(shell git rev-parse HEAD) \
		-X main.valuestoreVersion=$(shell git -C $$GOPATH/src/github.com/gholt/store rev-parse HEAD) \
		-X main.cmdctrlVersion=$(shell git -C $$GOPATH/src/github.com/pandemicsyn/cmdctrl rev-parse HEAD) \
		-X main.goVersion=$(shell go version | sed -e 's/ /-/g') \
		-X main.buildDate=$(shell date -u +%Y-%m-%d.%H:%M:%S)" github.com/pandemicsyn/oort/oort-valued
	go build -i -v -o packaging/root/usr/local/bin/oort-groupd --ldflags " \
		-X main.ringVersion=$(shell git -C $$GOPATH/src/github.com/gholt/ring rev-parse HEAD) \
		-X main.oortVersion=$(shell git rev-parse HEAD) \
		-X main.valuestoreVersion=$(shell git -C $$GOPATH/src/github.com/gholt/store rev-parse HEAD) \
		-X main.cmdctrlVersion=$(shell git -C $$GOPATH/src/github.com/pandemicsyn/cmdctrl rev-parse HEAD) \
		-X main.goVersion=$(shell go version | sed -e 's/ /-/g') \
		-X main.buildDate=$(shell date -u +%Y-%m-%d.%H:%M:%S)" github.com/pandemicsyn/oort/oort-groupd
	go build -i -v -o packaging/root/usr/local/bin/oort-cli github.com/pandemicsyn/oort/oort-cli

clean:
	rm -rf packaging/output
	rm -f packaging/root/usr/local/bin/oort-valued
	rm -f packaging/root/usr/local/bin/oort-groupd
	rm -f packaging/root/usr/local/bin/oort-cli

install: build
	mkdir -p /etc/oort/value
	mkdir -p /etc/oort/group
	cp -av packaging/root/usr/local/bin/* $(GOPATH)/bin

run:
	go run oort-valued/*.go

test:
	go test ./...

ring:
	ring /tmp/oort.builder create replicas=3
	ring /tmp/oort.builder add active=true capacity=1000 tier0=server1 tier1=z1 address0=127.0.0.1:8001 address1=127.0.0.2:8001 meta=onmetalv1
	ring /tmp/oort.builder add active=true capacity=1000 tier0=server2 tier1=z2 address0=127.0.0.1:8002 address1=127.0.0.2:8002 meta=onmetalv1
	ring /tmp/oort.builder add active=true capacity=1000 tier0=server3 tier1=z3 address0=127.0.0.1:8003 address1=127.0.0.2:8003 meta=onmetalv1
	ring /tmp/oort.builder ring

packages: clean deps build deb

deb:
	fpm -s dir -t deb -n oort-valued -v $(VERSION) -p packaging/output/oort-valued-$(VERSION)_amd64.deb \
		--deb-priority optional --category admin \
		--force \
		--iteration $(ITTERATION) \
		--deb-compression bzip2 \
	 	--after-install packaging/scripts/postinst.deb \
	 	--before-remove packaging/scripts/prerm.deb \
		--after-remove packaging/scripts/postrm.deb \
		--url https://github.com/pandemicsyn/oort \
		--description "Some sorta storage thingy" \
		-m "Florian Hines <syn@ronin.io>" \
		--license "Apache License 2.0" \
		--vendor "oort" -a amd64 \
		--config-files /etc/oort/oortd.toml-sample \
		packaging/root/=/
	cp packaging/output/oort-valued-$(VERSION)_amd64.deb packaging/output/oort-valued.deb.$(SHA)

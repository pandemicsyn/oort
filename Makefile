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

# An untested (probably not working) dockerfile

FROM golang

ADD . /go/src/github.com/pandemicsyn/photon/
WORKDIR /go/src/github.com/pandemicsyn/photon

RUN go get -u github.com/gholt/brimtime
RUN go get -u github.com/gholt/experimental-valuestore
RUN go get -u github.com/spaolacci/murmur3
RUN go install github.com/pandemicsyn/photon/cmd/photond

EXPOSE 6379
ENTRYPOINT /go/bin/photond

# An untested (probably not working) dockerfile

FROM golang

ADD . /go/src/github.com/pandemicsyn/ort/
WORKDIR /go/src/github.com/pandemicsyn/ort

RUN go get -u github.com/gholt/brimtime
RUN go get -u github.com/gholt/experimental-valuestore
RUN go get -u github.com/spaolacci/murmur3
RUN go install github.com/pandemicsyn/ort/cmd/ortd

EXPOSE 6379
ENTRYPOINT /go/bin/ortd

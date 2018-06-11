FROM golang:1.10

ENV GOPATH /go
ENV PATH ${GOPATH}/bin:$PATH
RUN go get -u github.com/golang/dep/cmd/dep
RUN go get -u github.com/golang/lint/golint
RUN go get -u github.com/mitchellh/gox




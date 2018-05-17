FROM golang:1.9-alpine3.7
RUN apk add --no-cache ca-certificates bash gcc git
RUN apk add --update tzdata curl && rm -rf /var/cache/apk/*
ENV GOPATH=/go
ADD . /go/src/github.com/adrianwit/dstransfer/
WORKDIR /go/src/github.com/adrianwit/dstransfer/app
RUN go get -u .
RUN go build -o dstransfer
RUN mkdir /app
RUN cp /go/src/github.com/adrianwit/dstransfer/app/dstransfer /app/
CMD ["/app/dstransfer"]
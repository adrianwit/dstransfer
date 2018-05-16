FROM golang:1.9-alpine3.7
RUN apk add --no-cache ca-certificates bash unixodbc
RUN apk add --update tzdata curl && rm -rf /var/cache/apk/*
ADD . /
WORKDIR /app
RUN go get -u .
RUN go build -o dstransfer .
CMD ["dstransfer"]
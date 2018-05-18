# dstransfer - simple cross datastore ETL transfer



# Installation


### With docker

- **Building app**
```bash
 
cd /tmp/ && git clone https://github.com/adrianwit/dstransfer
cd build
cp config/Dockerfile .
docker build -t adrianwit/dstrasfer:0.1.0 . 
```

- **Starting app**


### Standalone

- **Building app**

```bash
export GOPATH=~/go
go get -u github.com/adrianwit/dstransfer
go get -u github.com/adrianwit/dstransfer/dstransfer
```

- **Starting app**
```bash
$GOPATH/bin/dstransfer -port=8080
```

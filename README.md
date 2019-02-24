# dstransfer - simple cross datastore ETL SQL based transfer

## Motivation

Traditionally transferring data between different vendor data sources involved data export to text file i.e csv, 
followed by importing it to destination database. While this may work for the most scenarios, representing null values 
and converting incompatible data types like DATE/TIMESTAMP could be challenging.

This project provides a simple SQL based alternative addressing these concerns.
On top of that it copies data between arbitrary database/datastore (RDBMS/NoSQL) in a way that is both memory and writes optimized. 
While the first streamlining is achieved with using compacted slices as opposed to generic slice of a map, the latter
uses batch insert and concurrent writers.


## Installation

### With docker

- **Building app**
```bash
 cd /tmp/ && git clone https://github.com/adrianwit/dstransfer 
cd dstransfer
docker build -t adrianwit/dstransfer:0.1.0 . 
```

- **Starting app**
```bash
cd /tmp/dstransfer/config/ && docker-compose up  -d  
```


### Standalone


- **Building app**

Prerequisites: go 1.11+

```bash
go install github.com/adrianwit/dstransfer/dstransfer
```

- **Starting app**
```bash
export GOPATH=~/go/bin/
$GOPATH/bin/dstransfer -port=8080
```


## Usage

```bash
 curl  --header "Content-type: text/json" -d "@transfer.json" -X POST http://localhost:8080/v1/api/transfer
 
 curl http://127.0.0.1:8080/v1/api/tasks
 
 while :; do clear; curl http://127.0.0.1:8080/v1/api/tasks; sleep 2; done
``` 



**@transfer.json**

```json
{

  "BatchSize": 256,
  "WriterThreads": 4,
  "Mode": "insert",

  "Source": {
    "Credentials": "source_mysql",
    "Descriptor": "[username]:[password]@tcp(xxxxx:3306)/[dbname]?parseTime=true",
    "DriverName": "mysql",
    "Parameters": {
      "dbname": "db1"
    },
    "Query": "SELECT * FROM source_table"
  },


  "Dest": {
    "Credentials": "bq",
    "DriverName": "bigquery",
    "Parameters": {
      "datasetId": "db2"
    },
    "Table": "target_table"
  }

}
```


## Credentials

Credential are stored in ~/.secret/CREDENTIAL_NAME.json using [toolobx/cred/config.go](https://github.com/viant/toolbox/blob/master/cred/config.go) format.


For example:

@source_mysql
```json
{"Username":"root","Password":"dev"}
 ```

To generate encrypted credentials download and install the latest [endly](https://github.com/viant/endly/releases) and run the following

```bash
endly -c=source_mysql
```

For BigQuery: use service account generated JSON key type credentials.


## Supported datastores:

- any database/sql  (may need to include [driver import](dstransfer/dstransfer.go))

Already imported drivers:

 - mysql
 - postgresql
 - aerospike
 - bigquery
 - mongodb
 - casandra
 - dynamodb
 - firebase
 - firestore
 
## Supported but not imported drivers (CGO dependency)
 - oracle
 - vertica (vi odbc)
  

## Transfer mode
 - **insert**  use only INSERT INTO statement (suitable as append)
 - **persist** determine which record needs to be updated or inserted(slower option)

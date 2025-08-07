# M3-LOADER

This project is part of M3 Multi-Dimensional Data Model. It enables loading and exporting data to a postgres database from json files. This builds the database following the data model defined in order to be later visualized and explored using other tools.

## Requirements

- Python 3.10.11 or higher
- PostgreSQL 15
- Go 

Docker is currently not used on this project.

## Installation

To install the project, simply clone the repository.

## Usage

### Running the test database

1. Create the postgres database:

```shell
$ createdb -U postgres <database_name>
```

2. Create the tables using the ddl.sql file:

```shell
psql -U postgres -f ddl.sql <database_name>
```

The default database name is `loader-testing`. If you change it make sure to update the database connection parameters in the `server` code.

### Running the server

The server is currently implemented in Python and Go. Both can be used interchangeably. They both connect to the client via a gRPC interface defined in the `protos` folder.

#### Python server

The Python server is located in the `server` directory. It is a grpc server which directly interacts with the Postgres DB using the library _psycopg2_. The whole code is in `app.py`, and all the "pb2" files are generated using grpc. The `words.py` file is a list of 6-letters english words used to identify requests and processes when troubleshooting code.

To use the server:

- Open a terminal in the `server` directory.
- Create a python virtual environment and activate it:

```shell
$ python -m venv [server_venv_name]
$ source [server_venv_name]/Scripts/activate
```

- Install requirements:

```
$ pip install -r requirements.txt_
```

- Update the database connection parameters in app.py, line 21:

```python
self.conn = psycopg2.connect(
            database="loader-testing",
            user="postgres",
            password="root",
            host="localhost",
            port="5432",
        )
```

- Run the server:

```shell
$ python app.py
```

#### Go Server

The Go server is located in the `go-server` folder. Make sure the database parameters are set correctly in the `server.go` file, lines 25-34

```go
const (
	dbname     = "loader-testing"
	user       = "postgres"
	pwd        = "root"
	db_host    = "localhost"
	db_port    = 5432
	sv_host    = "localhost"
	sv_port    = 50051
	BATCH_SIZE = 5000
)
```

You can then run the server :

```shell
$ cd go-server
$ go mod tidy
$ go build -o server/go-server ./server
$ cd server
$ ./go-server
```

### Client and CLI

The client, CLI and test files are located in the `client` folder. All the "pb2" files are generated using grpc. The code of the grpc client is found in `grpc_client.py`, with functions specific to imports and exports located in the `filemgmt` folder. The CLI is based on the _Click_ library, and located in the `cli.py` file. Several JSON files for manual import/export testing are located in `json_testfiles` - 'm' is for medias, 'ts' for tagsets and 'h' for hierarchies; while files related to automated _Pytest_ testing are loacted in the `tests` folder. To use the CLI:

In the `client` folder, enable the virtual environment:

```shell
$ python -m venv [client_venv_name]
$ source [client_venv_name]/Scripts/activate
```

Now build the CLI:

```shell
$ pip install --editable .
```

Now, you will have instant updates upon changing the client code.

Note: after adding new py files or changing dependencies, you need to update the _setup.py_ file and run the above command again.

To interact with the loader, use `loader` command in the console from anywhere.

To import data from a JSON file :

```shell
$ loader import -f json <json_file>
```

To export the data to a JSON file:

```shell
$ loader export -f json <json_file>
```

Currently, only json files are supported. The format for the json files is :

```json
{
    "tagsets": [
        {
            "name": "ImageNet",
            "type": 1
        },
        [...]
    ],

    "medias": [
        {
            "path": "http://localhost:5005/lsc/201903/16/20190316_131738_000.jpg",
            "thumbnail": "http://localhost:5005/lsc/201903/16/20190316_131738_000.jpg",
            "tags": [
                {
                    "tagset": "Collection",
                    "value": "LSC2024"
                },
                {
                    "tagset": "ImageNet",
                    "value": "window screen"
                },
                [...]
            ]
        },
        [...]
    ],
    "hierarchies": [
        {
            "name": "Day of Week",
            "tagset": "Day of week (string)",
            "rootnode": {
                "tag": "Day of week",
                "children": [
                    {
                        "tag": "Monday",
                        "children": []
                    },
                    {
                        "tag": "Tuesday",
                        "children": []
                    },
                    [...]
                ]
            }
        },
        [...]
    ]
}
```

### Tests

- Enable the client virtual environment.
- Navigate to the `tests` folder.
- Run the tests:

```shell
$ source [client_venv_name]/Scripts/activate
$ cd tests
$ pytest -vv
```

### Updating Protocol Buffers

To generate Python protoc files from the protobuf definition, use the following script in the `client` or `server` folder.

```
$ source ../generate_protos
```

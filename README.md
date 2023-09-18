__M3-LOADER__
***

**Requirements:**
- Python 3.10.11 or higher
- PostgreSQL 15

Docker is currently not used on this project.

**Python server:**

The Python server is located in the `server` directory. It is a grpc server which directly interacts with the Postgres DB using the library _psycopg2_. The whole code is in `app.py`, and all the "pb2" files are generated using grpc. The `words.py` file is a list of 6-letters english words used to identify requests and processes when troubleshooting code.

To use the server:

- Open a terminal in the `server` directory.
- Create a python virtual environment and activate it: 
```
$ python -m venv [server_venv_name]
$ source [server_venv_name]/Scripts/activate
```
- Install requirements:
```
$ pip install -r requirements.txt_
```
- Update the database connection parameters in app.py, line 21:
```
self.conn = psycopg2.connect(
            database="loader-testing",
            user="postgres",
            password="root",
            host="localhost",
            port="5432",
        )
```
- Run the server:
```
$ python app.py
```
**Go server (in the `go-server` folder)**
```
$ go mod tidy
$ go run .
```

**Client and CLI**

The client, CLI and test files are located in the `client` folder. All the "pb2" files are generated using grpc. The code of the grpc client is found in `grpc_client.py`, with functions specific to imports and exports located in the `filemgmt` folder. The CLI is based on the _Click_ library, and located in the `cli.py` file. Several JSON files for manual import/export testing are located in `json_testfiles` - 'm' is for medias, 'ts' for tagsets and 'h' for hierarchies; while files related to automated _Pytest_ testing are loacted in the `tests` folder. To use the CLI:


In the `client` folder, enable the virtual environment:
```
$ python -m venv [client_venv_name]
$ source [client_venv_name]/Scripts/activate
```
Now build the CLI:
```
$ pip install --editable .
```
Now, you will have instant updates upon changing the client code. 

Note: after adding new py files or changing dependencies, you need to update the _setup.py_ file and run the above command again.

To interact with the loader, do
```
$ loader [COMMAND]
```
in the console from anywhere.



**Tests**
- Enable the client virtual environment.
- Navigate to the `tests` folder.
- Run the tests:
```
$ source [client_venv_name]/Scripts/activate
$ cd tests
$ pytest -vv
```

**Updating Protocol Buffers**

To generate Python protoc files from the protobuf definition, use the following script in the `client` or `server` folder.

```
$ source ../generate_protos
```
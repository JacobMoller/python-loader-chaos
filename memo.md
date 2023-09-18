<!-- // TO GENERATE AN IMAGE
$ docker build -t [name] .

// To RUN AN IMAGE => CREATE THE CONTAINER
$ docker run -p -d [localport]:[containerport] [image-name]

// TO START OR RESTART AN EXISTING CONTAINER
$ docker restart [container-ID] -->

// TO START PYTHON SERVER (in server folder)
$ source .venv/Scripts/activate
$ pip install requirements.txt
$ python app.py

// TO GENERATE PROTOC FILES
$ python3 -m grpc_tools.protoc -I../protos --python_out=. --pyi_out=. --grpc_python_out=. ../protos/medias.proto

Then copy pb files to client side

// TO START GO SERVER (in server folder)
$ go mod tidy
$ go run .

// TO RUN CLI WITH AUTO UPDATE (run in client folder)
$ pip install --editable .
then you can use 'loader [COMMAND]' in the console from anywhere 

// TO TEST ALL THE CLI FUNCTION
-> enable the client virtual environment (source .venv/Scripts/activate in client folder)
-> cd tests
$ pytest -vv
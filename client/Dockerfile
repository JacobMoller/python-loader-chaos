FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./

RUN pip install -r requirements.txt

COPY *.py ./

COPY ./filemgmt ./filemgmt

COPY ./tests ./tests

COPY ./data_loader_cli.egg-info ./data_loader_cli.egg-info

RUN pip install .


CMD ["sleep", "infinity"]

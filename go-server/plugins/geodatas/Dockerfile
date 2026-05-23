FROM python:3.11

WORKDIR /app

COPY ./plugins/geodatas/requirements.txt ./geodatas/requirements.txt

RUN pip install -r ./geodatas/requirements.txt

COPY ./plugins/geodatas ./geodatas

COPY ./rabbitMQ ./geodatas


CMD [ "python", "./geodatas/main.py" ]
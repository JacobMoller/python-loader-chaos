FROM python:3.11

WORKDIR /app

COPY ./plugins/captioning/requirements.txt ./captioning/requirements.txt

RUN pip install -r ./captioning/requirements.txt

COPY ./plugins/captioning ./captioning

# COPY ./media_downloader/gen/python ./captioning

COPY ./rabbitMQ ./captioning


CMD [ "python", "./captioning/main.py" ]
FROM python:3.12

WORKDIR /app

COPY ./plugins/sleep_level/requirements.txt ./sleep_level/requirements.txt

RUN pip install -r ./sleep_level/requirements.txt

COPY ./plugins/sleep_level ./sleep_level

COPY ./media_downloader/gen/python ./sleep_level

COPY ./rabbitMQ ./sleep_level

COPY ./dataloader/ ./sleep_level


CMD [ "python", "./sleep_level/main.py" ]
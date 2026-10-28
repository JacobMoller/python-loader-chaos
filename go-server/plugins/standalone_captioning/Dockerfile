FROM python:3.11

WORKDIR /app

COPY ./plugins/standalone_captioning/requirements.txt ./standalone_captioning/requirements.txt

RUN pip install -r ./standalone_captioning/requirements.txt

COPY ./plugins/standalone_captioning ./standalone_captioning

# Shared code is copied next to main.py so it can be imported directly
COPY ./rabbitMQ ./standalone_captioning

COPY ./media_downloader/gen/python ./standalone_captioning

COPY ./media_downloader/client ./standalone_captioning


CMD [ "python", "./standalone_captioning/main.py" ]

FROM python:3.12

WORKDIR /app

COPY ./plugins/color_detector_python/requirements.txt ./color_detector_python/requirements.txt

RUN pip install -r ./color_detector_python/requirements.txt

COPY ./plugins/color_detector_python ./color_detector_python

# Shared code is copied next to main.py so it can be imported directly
COPY ./rabbitMQ ./color_detector_python

COPY ./media_downloader/gen/python ./color_detector_python

COPY ./dataloader/ ./color_detector_python


CMD [ "python", "./color_detector_python/main.py" ]

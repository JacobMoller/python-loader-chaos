FROM python:3.12

WORKDIR /app

COPY ./plugins/exif_extractor/requirements.txt ./exif_extractor/requirements.txt

RUN pip install -r ./exif_extractor/requirements.txt

COPY ./plugins/exif_extractor ./exif_extractor

COPY ./media_downloader/gen/python ./exif_extractor

COPY ./rabbitMQ ./exif_extractor

COPY ./dataloader/ ./exif_extractor


CMD [ "python", "./exif_extractor/main.py" ]
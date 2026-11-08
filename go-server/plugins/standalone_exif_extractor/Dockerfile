FROM python:3.12

WORKDIR /app

COPY ./plugins/standalone_exif_extractor/requirements.txt ./standalone_exif_extractor/requirements.txt

RUN pip install -r ./standalone_exif_extractor/requirements.txt

COPY ./plugins/standalone_exif_extractor ./standalone_exif_extractor

# Shared code is copied next to main.py so it can be imported directly
COPY ./rabbitMQ ./standalone_exif_extractor

COPY ./dataloader/ ./standalone_exif_extractor

COPY ./media_downloader/gen/python ./standalone_exif_extractor

COPY ./media_downloader/client ./standalone_exif_extractor


CMD [ "python", "./standalone_exif_extractor/main.py" ]

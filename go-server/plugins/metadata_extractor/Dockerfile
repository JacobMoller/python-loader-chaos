FROM python:3.12

WORKDIR /app

COPY ./plugins/metadata_extractor/requirements.txt ./metadata_extractor/requirements.txt

RUN pip install -r ./metadata_extractor/requirements.txt

COPY ./plugins/metadata_extractor ./metadata_extractor

COPY ./rabbitMQ ./metadata_extractor

COPY ./dataloader/ ./metadata_extractor


CMD [ "python", "./metadata_extractor/main.py" ]
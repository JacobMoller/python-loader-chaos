FROM python:3.12

WORKDIR /app

COPY ./plugins/media_orchestrator/requirements.txt ./media_orchestrator/requirements.txt

RUN pip install -r ./media_orchestrator/requirements.txt

COPY ./plugins/media_orchestrator ./media_orchestrator

COPY ./rabbitMQ ./media_orchestrator

COPY ./media_downloader/gen/python ./media_orchestrator

COPY ./dataloader/ ./media_orchestrator


CMD [ "python", "./media_orchestrator/main.py" ]
FROM ultralytics/ultralytics:latest-cpu

WORKDIR /app

COPY ./plugins/classifier/requirements.txt ./classifier/requirements.txt

RUN pip install -r ./classifier/requirements.txt

COPY ./plugins/classifier ./classifier

# COPY ./media_downloader/gen/python ./classifier

COPY ./rabbitMQ ./classifier



# RUN apt-get update && apt-get -y install libgl1 libglib2.0-0


CMD [ "python", "./classifier/main.py" ]
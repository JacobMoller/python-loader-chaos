FROM ultralytics/ultralytics:latest-cpu

WORKDIR /app

# The pinned ultralytics pulls in opencv-python, which needs these system libraries
RUN apt-get update && apt-get -y install libxcb1 libgl1 libglib2.0-0 && rm -rf /var/lib/apt/lists/*

COPY ./plugins/standalone_classifier/requirements.txt ./standalone_classifier/requirements.txt

RUN pip install -r ./standalone_classifier/requirements.txt

COPY ./plugins/standalone_classifier ./standalone_classifier

# Shared code is copied next to main.py so it can be imported directly
COPY ./rabbitMQ ./standalone_classifier

COPY ./media_downloader/gen/python ./standalone_classifier

COPY ./media_downloader/client ./standalone_classifier


CMD [ "python", "./standalone_classifier/main.py" ]

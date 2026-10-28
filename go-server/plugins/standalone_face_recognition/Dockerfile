FROM python:3.11

WORKDIR /app

# Install build tools and dependencies for dlib
RUN apt-get update && \
apt-get -y install cmake g++ && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

COPY ./plugins/standalone_face_recognition/requirements.txt ./standalone_face_recognition/requirements.txt

RUN pip install -r ./standalone_face_recognition/requirements.txt

COPY ./plugins/standalone_face_recognition ./standalone_face_recognition

# Shared code is copied next to main.py so it can be imported directly
COPY ./rabbitMQ ./standalone_face_recognition

COPY ./media_downloader/gen/python ./standalone_face_recognition

COPY ./media_downloader/client ./standalone_face_recognition


CMD [ "python", "./standalone_face_recognition/main.py" ]

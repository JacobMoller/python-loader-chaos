FROM python:3.11

WORKDIR /app

# Install build tools and dependencies for dlib
RUN apt-get update && \
apt-get -y install cmake g++ && \
apt-get clean && \
rm -rf /var/lib/apt/lists/*

COPY ./plugins/face_recognition/requirements.txt ./face_recognition/requirements.txt

RUN pip install -r ./face_recognition/requirements.txt

COPY ./plugins/face_recognition ./face_recognition

# COPY ./media_downloader/gen/python ./face_recognition

COPY ./rabbitMQ ./face_recognition


CMD [ "python", "./face_recognition/main.py" ]
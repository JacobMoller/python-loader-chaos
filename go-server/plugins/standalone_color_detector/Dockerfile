FROM golang:1.24.2


WORKDIR /src

COPY go.mod go.sum ./
COPY ./rabbitMQ ./rabbitMQ
COPY ./dataloader ./dataloader

COPY ./media_downloader/go.mod ./media_downloader/go.sum ./media_downloader/
COPY ./media_downloader/gen ./media_downloader/gen

COPY ./plugins/standalone_color_detector ./plugins/standalone_color_detector

WORKDIR /src/plugins/standalone_color_detector

RUN go build -o /app/standalone_color_detector .

CMD [ "/app/standalone_color_detector" ]

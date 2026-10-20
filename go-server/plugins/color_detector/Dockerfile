FROM golang:1.24.2


WORKDIR /src

COPY go.mod go.sum ./
COPY ./rabbitMQ ./rabbitMQ
COPY ./dataloader ./dataloader

COPY ./media_downloader/go.mod ./media_downloader/go.sum ./media_downloader/
COPY ./media_downloader/gen ./media_downloader/gen

COPY ./plugins/color_detector ./plugins/color_detector

WORKDIR /src/plugins/color_detector

RUN go build -o /app/color_detector .

CMD [ "/app/color_detector" ]

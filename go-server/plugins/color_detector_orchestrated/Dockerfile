FROM golang:1.24.2

# Unlike the standalone color_detector, this plugin does not talk to the
# media-downloader, so it is part of the go-server module (m3.dataloader)
# and needs no go.mod of its own.
WORKDIR /src

COPY go.mod go.sum ./
COPY ./rabbitMQ ./rabbitMQ
COPY ./dataloader ./dataloader

COPY ./plugins/color_detector_orchestrated ./plugins/color_detector_orchestrated

RUN go build -o /app/color_detector_orchestrated ./plugins/color_detector_orchestrated

CMD [ "/app/color_detector_orchestrated" ]

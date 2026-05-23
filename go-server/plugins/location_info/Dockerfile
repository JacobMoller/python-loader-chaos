FROM golang:1.24.2

WORKDIR /app

COPY ./plugins/location_info ./location_info

COPY ./rabbitMQ ./rabbitMQ

COPY go.mod go.sum ./

RUN go mod tidy

CMD [ "go", "run", "./location_info/main.go" ]
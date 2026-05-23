FROM golang:1.24.2

WORKDIR /app

COPY ./plugins/time_info ./time_info

COPY ./rabbitMQ ./rabbitMQ

COPY go.mod go.sum ./

RUN go mod tidy

CMD [ "go", "run", "./time_info/main.go" ]
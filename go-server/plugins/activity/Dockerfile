FROM golang:1.24.2

WORKDIR /app

COPY ./plugins/activity ./activity

COPY ./rabbitMQ ./rabbitMQ

COPY go.mod go.sum ./

RUN go mod tidy

CMD [ "go", "run", "./activity/main.go" ]
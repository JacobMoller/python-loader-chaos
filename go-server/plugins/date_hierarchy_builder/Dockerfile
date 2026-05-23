FROM golang:1.24.2

WORKDIR /app

COPY ./plugins/date_hierarchy_builder ./date_hierarchy_builder

COPY ./rabbitMQ ./rabbitMQ

COPY go.mod go.sum ./

RUN go mod tidy

CMD [ "go", "run", "./date_hierarchy_builder/main.go" ]
FROM golang:1.24.2

WORKDIR /app

RUN go install github.com/grpc-ecosystem/grpc-health-probe@latest

COPY . .

RUN go build -o main ./main.go

CMD ["./main"]
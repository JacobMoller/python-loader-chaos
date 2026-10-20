FROM golang:1.24.2

WORKDIR /app

RUN go install github.com/grpc-ecosystem/grpc-health-probe@v0.4.49

COPY . .

RUN go build -o ./serverexec ./server/server.go

CMD ["./serverexec"]

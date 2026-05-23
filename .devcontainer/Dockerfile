# 1. Base off Microsoft’s Go Dev Container image (includes Go, gopls, dlv)
FROM mcr.microsoft.com/vscode/devcontainers/go:1-1.24-bullseye

RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
RUN go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
RUN go install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates protobuf-compiler \
 && rm -rf /var/lib/apt/lists/* \
 && go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest \
 && mv "$(go env GOPATH)/bin/grpcurl" /usr/local/bin/ \
 # now fix up the module & build caches:
 && chown -R vscode:vscode "$(go env GOPATH)" \
 && chmod +x /usr/local/bin/grpcurl

# Vendor only the minimal proto files into /usr/local/include/googleapis
RUN mkdir -p /usr/local/include/googleapis/google/api \
         /usr/local/include/googleapis/google/protobuf \
         /usr/local/include/googleapis/google/rpc \
 && curl -sSL \
      https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/annotations.proto \
    -o /usr/local/include/googleapis/google/api/annotations.proto \
 && curl -sSL \
      https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/http.proto \
    -o /usr/local/include/googleapis/google/api/http.proto \
 && curl -sSL \
      https://raw.githubusercontent.com/protocolbuffers/protobuf/master/src/google/protobuf/descriptor.proto \
    -o /usr/local/include/googleapis/google/protobuf/descriptor.proto \
 && curl -sSL \
      https://raw.githubusercontent.com/googleapis/googleapis/master/google/api/httpbody.proto \
    -o /usr/local/include/googleapis/google/api/httpbody.proto \
 && curl -sSL \
      https://raw.githubusercontent.com/protocolbuffers/protobuf/master/src/google/protobuf/any.proto \
    -o /usr/local/include/googleapis/google/protobuf/any.proto \
 && curl -sSL \
      https://raw.githubusercontent.com/grpc/grpc/master/src/proto/grpc/status/status.proto \
    -o /usr/local/include/googleapis/google/protobuf/status.proto \
  && curl -sSL \
        https://raw.githubusercontent.com/protocolbuffers/protobuf/master/src/google/protobuf/wrappers.proto \
    -o /usr/local/include/googleapis/google/protobuf/wrappers.proto


# 3. Define the working directory inside the container
WORKDIR /workspace
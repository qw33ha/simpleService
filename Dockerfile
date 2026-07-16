FROM golang:1.22 AS builder

WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download

COPY . .


RUN go mod tidy \
    && CGO_ENABLED=0 go build \
        -trimpath \
        -o /out/simpleService \
        ./

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=builder /out/simpleService /app/simpleService
COPY trpc_go.yaml /app/trpc_go.yaml
COPY ca.pem /etc/ssl/certs/kafka-ca.pem
ENV SSL_CERT_FILE=/etc/ssl/certs/kafka-ca.pem

ENTRYPOINT ["/app/simpleService"]
CMD ["-conf=/app/trpc_go.yaml"]
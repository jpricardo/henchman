FROM golang:1.25-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o henchd ./cmd/henchd

FROM gcr.io/distroless/static-debian12
COPY --from=builder /build/henchd /henchd
EXPOSE 6474 9090
ENTRYPOINT ["/henchd"]

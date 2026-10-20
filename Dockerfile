FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
RUN go mod download || true

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/raft-server ./cmd/server

FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/raft-server /app/raft-server

EXPOSE 8080
ENTRYPOINT ["/app/raft-server"]

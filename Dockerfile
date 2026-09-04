FROM golang:1.27-alpine AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o /out/taskforge-api ./cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/taskforge-worker ./cmd/worker

FROM alpine:3.22

WORKDIR /app

RUN addgroup -S taskforge \
    && adduser -S taskforge -G taskforge \
    && mkdir -p /app/output \
    && chown -R taskforge:taskforge /app

COPY --from=builder /out/taskforge-api /usr/local/bin/taskforge-api
COPY --from=builder /out/taskforge-worker /usr/local/bin/taskforge-worker

USER taskforge
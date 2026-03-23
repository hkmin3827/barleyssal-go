FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o barleyssal-go ./cmd/server

FROM alpine:latest

WORKDIR /app

RUN apk --no-cache add ca-certificates tzdata
ENV TZ=Asia/Seoul

COPY --from=builder /app/barleyssal-go .

CMD ["./barleyssal-go"]
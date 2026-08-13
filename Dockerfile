FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o gomailer .

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/gomailer .
COPY --from=builder /app/email.tmpl .
COPY --from=builder /app/dummy_emails.csv .

CMD ["./gomailer"]
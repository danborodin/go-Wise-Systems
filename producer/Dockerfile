FROM golang:1.20
WORKDIR /app
COPY . .
RUN go mod download
RUN go build -o ./bin/app ./cmd/main.go
CMD ["./bin/app"]
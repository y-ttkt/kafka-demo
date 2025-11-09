FROM golang:1.23.5
ENV TZ=Asia/Tokyo

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

EXPOSE 8080
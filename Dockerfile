FROM golang:1.22.12-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
ARG GOPROXY=https://proxy.golang.org,direct
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /api .
FROM alpine:3.21
WORKDIR /app
RUN adduser -D app && mkdir upload && chown app upload
COPY --from=build /api /app/api
USER app
ENTRYPOINT ["/app/api"]

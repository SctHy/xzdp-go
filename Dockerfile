FROM node:24-alpine AS web
WORKDIR /src/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run check

FROM golang:1.27-alpine AS backend
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY --from=web /src/internal/httpapi/web ./internal/httpapi/web
RUN CGO_ENABLED=0 go build -trimpath -o /app/api ./cmd/api && CGO_ENABLED=0 go build -trimpath -o /app/worker ./cmd/worker

FROM alpine:3.23
RUN apk add --no-cache ca-certificates tzdata
COPY --from=backend /app /app
USER 65534:65534
CMD ["/app/api"]

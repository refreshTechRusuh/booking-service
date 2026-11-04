# ---- Build stage ----
FROM golang:1.26-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o bin/api cmd/main.go

# ---- Runtime stage (distroless non-root) ----
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /app/bin/api .
EXPOSE 8081
USER nonroot:nonroot
ENTRYPOINT ["/app/api"]

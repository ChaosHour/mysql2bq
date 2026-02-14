# Build stage
FROM golang:1.24-alpine AS builder

# Install git and ca-certificates (needed for Go modules and HTTPS requests)
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o mysql2bq .

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests to BigQuery
RUN apk --no-cache add ca-certificates

# Create a non-root user
RUN adduser -D -s /bin/sh mysql2bq

# Create logs directory and set permissions
RUN mkdir -p /app/logs && chown -R mysql2bq:mysql2bq /app && chmod -R 755 /app

WORKDIR /app/

# Copy the binary from builder stage
COPY --from=builder /app/mysql2bq .

# Change ownership to non-root user
RUN chown mysql2bq:mysql2bq mysql2bq

# Switch to non-root user
USER mysql2bq

# Expose any ports if needed (though this app might not need to expose ports)
# EXPOSE 8080

# Run the application
CMD ["./mysql2bq"]
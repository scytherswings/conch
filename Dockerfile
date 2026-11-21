FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /conch ./cmd/conch

FROM scratch
COPY --from=build /conch /conch
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
ENTRYPOINT ["/conch"]
CMD ["run", "--config", "/etc/conch/conch.yml"]

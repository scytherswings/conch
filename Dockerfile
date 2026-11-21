# golang:1.27-alpine (multi-arch manifest list)
FROM golang:1.27-alpine@sha256:70bd7ddea8b6cbe5d6943cde279d6b67feb10cc924fb9100d1d8757c1575b280 AS build
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

# syntax=docker/dockerfile:1

FROM golang:1.27.1-alpine AS build

ARG TARGETOS
ARG TARGETARCH
ARG VERSION=dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w -X main.version=${VERSION}" \
    -o /out/cms-labs-terminal ./cmd/cms-labs-terminal

# ttyd supplies the maintained xterm.js/WebSocket frontend. It starts only the tiny loopback client;
# the long-lived Kubernetes stream belongs to the Go broker process above.
FROM ghcr.io/tsl0922/ttyd:alpine@sha256:dafdb93a65c9b487f3056933733a06c12f6e447e3bd31ccd4ef6c279cf9a7648 AS ttyd

FROM scratch

ARG VERSION=dev

LABEL org.opencontainers.image.title="cms-labs-terminal" \
      org.opencontainers.image.description="Persistent namespace-local browser terminals for Kubernetes labs" \
      org.opencontainers.image.source="https://github.com/maintainer64/cms-labs-terminal" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.version="${VERSION}"

COPY --from=build /out/cms-labs-terminal /cms-labs-terminal
COPY --from=ttyd /usr/bin/ttyd /usr/bin/ttyd
COPY --from=ttyd /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

USER 65532:65532

EXPOSE 7681 7682 7683

ENTRYPOINT ["/cms-labs-terminal"]
CMD ["serve"]

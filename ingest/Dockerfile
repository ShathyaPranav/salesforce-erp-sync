# Ingest poller Lambda. Build context: the repo root (go.mod lives there).
#   docker build -f ingest/Dockerfile -t relay-ingest .

FROM golang:1.27.1 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal/ internal/
COPY ingest/ ingest/
# Static binary (no cgo), so the newer glibc in the build image doesn't matter.
# lambda.norpc drops the legacy RPC mode that provided.al2023 doesn't use.
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -tags lambda.norpc -ldflags="-s -w" -o /out/main ./ingest

FROM public.ecr.aws/lambda/provided:al2023
COPY --from=build /out/main /var/task/main
# On AWS the Lambda service runs this. Locally, docker compose wraps it in the
# Runtime Interface Emulator that ships in this base image.
ENTRYPOINT ["/var/task/main"]

FROM golang:1.16-alpine AS builder

ARG VERSION="2.2.3-beta.4-PRVD"

WORKDIR $GOPATH/src/github.com/kthomas/nats-server

RUN apk add --update git
COPY . .

RUN go mod tidy && go build -ldflags "-s -w -X main.version=${VERSION}" -o /nats-server

FROM alpine

RUN apk add --update ca-certificates && mkdir -p /nats/bin && mkdir /nats/conf

COPY docker/nats-server.conf /nats/conf/nats-server.conf
COPY --from=builder /nats-server /nats/bin/nats-server

RUN ln -ns /nats/bin/nats-server /bin/nats-server && ln -ns /nats/bin/nats-server /nats-server

# Expose client, management, cluster and gateway ports
EXPOSE 4221 4222 8222 6222 5222

ENTRYPOINT ["/bin/nats-server"]

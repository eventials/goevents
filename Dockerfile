FROM golang:1.12

ARG PLATFORM=linux
ENV PLATFORM $PLATFORM_VERSION

ARG ARCH=amd64
ENV ARCH $ARCH_VERSION

ARG DOCKERIZE_VERSION=v0.6.1
ENV DOCKERIZE_VERSION $DOCKERIZE_VERSION

ADD https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-$PLATFORM-$ARCH-$DOCKERIZE_VERSION.tar.gz /usr/local/bin

RUN cd /usr/local/bin \
    && tar -xzf ./dockerize-$PLATFORM-$ARCH-$DOCKERIZE_VERSION.tar.gz \
    && rm -f ./dockerize-$PLATFORM-$ARCH-$DOCKERIZE_VERSION.tar.gz

RUN mkdir -p /go/src/github.com/eventials/goevents
WORKDIR /go/src/github.com/eventials/goevents

RUN go mod download

ENTRYPOINT ["dockerize"]

CMD ["-wait", "tcp://broker:5672", "-timeout", "60s", "go", "run", "examples/consumer/consumer.go"]

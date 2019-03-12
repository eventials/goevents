FROM golang:1.12

RUN DOCKERIZE_VERSION=v0.6.1 \
	&& wget --no-check-certificate https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-alpine-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && mkdir -p /goevents

WORKDIR /goevents

ENTRYPOINT ["dockerize"]

CMD ["-wait", "tcp://broker:5672", "-timeout", "60s", "go", "run", "examples/consumer/consumer.go"]

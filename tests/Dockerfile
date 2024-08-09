FROM amazoncorretto:17.0.12-alpine3.20

RUN apk update && apk upgrade && \
    apk add --no-cache git bash ncurses && \
    git clone https://github.com/unitycatalog/unitycatalog.git && \
    cd /unitycatalog && \
    ./build/sbt package

WORKDIR /unitycatalog

EXPOSE 8080

ENTRYPOINT ["./bin/start-uc-server"]

FROM debian as builder

ARG TARGETPLATFORM

COPY ./aarch64-unknown-linux-gnu /target/aarch64-unknown-linux-gnu

RUN ls -lR /target

RUN if [ $TARGETPLATFORM = "linux/arm64" ]; then \
    mv /target/aarch64-unknown-linux-gnu/release/winds /winds; \
  elif [ $TARGETPLATFORM = "linux/amd64" ]; then \
    mv x86_64-unknown-linux-gnu/release/winds /winds; \
  fi; \
  chmod +x /winds


FROM debian

RUN apt-get update && apt-get upgrade --yes && apt-get install --yes --no-install-recommends openjdk-11-jre

COPY /grib2json /grib2json
COPY --from=builder /winds /

ENV JAVA_HOME "/usr/lib/jvm/java-11-openjdk-armhf"

CMD ["/winds"]

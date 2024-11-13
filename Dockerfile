FROM debian as builder

ARG TARGETPLATFORM

COPY target_aarch64/aarch64-unknown-linux-gnu/ /target/aarch64-unknown-linux-gnu
COPY target_x86-64/x86_64-unknown-linux-gnu/ /target/x86_64-unknown-linux-gnu

RUN if [ $TARGETPLATFORM = "linux/arm64" ]; then \
    mv /target/aarch64-unknown-linux-gnu/release/winds /winds; \
  elif [ $TARGETPLATFORM = "linux/amd64" ]; then \
    mv /target/x86_64-unknown-linux-gnu/release/winds /winds; \
  fi; \
  chmod +x /winds


FROM debian

RUN apt-get update && apt-get upgrade --yes && apt-get install --yes --no-install-recommends openjdk-17-jre

COPY /grib2json /grib2json
COPY --from=builder /winds /

RUN echo "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(type -P java))))" > /etc/profile.d/javahome.sh \
    && chmod +x /etc/profile.d/javahome.sh

CMD ["/winds"]
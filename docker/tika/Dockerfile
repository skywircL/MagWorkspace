FROM eclipse-temurin:21-jre@sha256:ccbad9e7cb84c27092b12c40a4143e5733e050db9c3784d84f632dcf423fb2ef

WORKDIR /opt/tika

# Apache's 3.3.3 release and published SHA512 are fixed together. Do not use
# the older apache/tika image tag as a substitute for this patched release.
RUN curl --fail --show-error --location --retry 3 --proto '=https' \
       https://downloads.apache.org/tika/3.3.3/tika-server-standard-3.3.3.jar \
       --output /opt/tika/tika-server.jar \
    && printf '%s  %s\n' \
       '5242b1c9d7aa455bdb0fe101304b2c4a274882406f4d054af39cc91eef678aecb7bf31127d8cc692cd5f47a9f602f4840139f9e8e44a708e312b2ba59d1ebe89' \
       '/opt/tika/tika-server.jar' | sha512sum --check --strict -

COPY tika-config.xml /opt/tika/tika-config.xml

EXPOSE 9998
ENTRYPOINT ["java", "-Xms32m", "-Xmx128m", "-Djava.awt.headless=true", "-jar", "/opt/tika/tika-server.jar", "--config", "/opt/tika/tika-config.xml"]

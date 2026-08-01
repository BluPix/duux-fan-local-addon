ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache \
    python3 \
    py3-pip \
    openssl

RUN pip3 install --no-cache-dir --break-system-packages paho-mqtt==2.1.0

COPY rootfs /

RUN chmod a+x /opt/duux_bridge/__main__.py

ENTRYPOINT []
CMD [ "python3", "/opt/duux_bridge" ]

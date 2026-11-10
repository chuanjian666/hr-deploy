FROM docker:28-dind
RUN apk add --no-cache bash coreutils util-linux e2fsprogs
COPY node-entrypoint.sh /usr/local/bin/kafka-test-node
ENTRYPOINT ["/bin/sh", "/usr/local/bin/kafka-test-node"]

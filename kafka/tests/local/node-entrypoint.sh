#!/bin/sh
set -eu
mkdir -p /test-disk /data1
if [ ! -f /test-disk/data.img ]; then
    truncate -s 20G /test-disk/data.img
    mkfs.ext4 -q -F /test-disk/data.img
fi
mount -o loop /test-disk/data.img /data1
exec /usr/local/bin/dockerd-entrypoint.sh dockerd \
    --host=unix:///var/run/docker.sock --data-root=/data1/docker \
    --storage-driver=overlay2 --tls=false

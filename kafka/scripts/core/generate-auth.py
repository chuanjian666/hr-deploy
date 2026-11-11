#!/usr/bin/env python3
"""Generate initial Kafka SCRAM material using only the Python standard library."""
import base64
import hashlib
import os
from pathlib import Path
import re
import sys


def private_file(path, content):
    with path.open("w", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        stream.write(content)


def main():
    output, previous = map(Path, sys.argv[1:3])
    hosts, port = sys.argv[3:5]
    username = os.environ["AUTH_USERNAME"]
    password = os.environ["AUTH_PASSWORD"]
    salt = os.urandom(32)
    old = previous / "bootstrap.scram"
    if old.is_file():
        # Preserve the salt across regeneration, including partially prepared clusters.
        match = re.search(r'salt="([A-Za-z0-9+/=]+)"', old.read_text())
        if not match:
            raise SystemExit("已有 bootstrap.scram 格式无效，拒绝重新生成初始凭据")
        salt = base64.b64decode(match[1], validate=True)
    salted = hashlib.pbkdf2_hmac("sha512", password.encode(), salt, 8192)
    credential = (
        f'SCRAM-SHA-512=[name="{username}",iterations=8192,'
        f'salt="{base64.b64encode(salt).decode()}",'
        f'saltedpassword="{base64.b64encode(salted).decode()}"]\n'
    )
    if old.is_file() and old.read_text() != credential:
        raise SystemExit("初始账号或密码与已有节点包不一致；已有集群请使用 kafka-configs.sh 管理账号。全新集群请使用独立输出目录")
    private_file(output / "bootstrap.scram", credential)
    (output / "clients").mkdir(mode=0o700)
    bootstrap = ",".join(f"{host}:{port}" for host in hosts.replace(" ", ",").split(",") if host)
    private_file(output / "clients/admin.properties", (
        f"bootstrap.servers={bootstrap}\n"
        "security.protocol=SASL_PLAINTEXT\n"
        "sasl.mechanism=SCRAM-SHA-512\n"
        "sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{username}" password="{password}";\n'
    ))


if __name__ == "__main__":
    main()

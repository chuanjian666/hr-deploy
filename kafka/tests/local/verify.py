#!/usr/bin/env python3
"""Functional checks only; run against the isolated test-local.sh deployment."""
import datetime
from decimal import Decimal
import json
import os
import pathlib
import re
import secrets
import subprocess
import sys
import time

runtime = pathlib.Path(sys.argv[1])
logs = runtime / "logs"
cluster_dir = pathlib.Path(sys.argv[2])
manifest = json.loads((cluster_dir / "manifest.json").read_text())
prefix = manifest["cluster"]["name"]
authentication = manifest.get("security", {}).get("authenticationEnabled", False)
client_port = manifest.get("security", {}).get("clientPort", 9092)
bootstrap = ",".join(f"172.30.93.{i}:{client_port}" for i in (11, 12, 13))
stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
topic = f"deploy-smoke-{stamp.lower()}"
sequence = 0
checks = []
client_configs = {}
client_dir = f"/tmp/kafka-verify-{stamp}"


def run(args, *, data=None, check=True, timeout=60):
    global sequence
    sequence += 1
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    # Kafka may echo a malformed credential value in an error message.
    result.stdout = re.sub(r"password=[^\],\s]+", "password=<redacted>", result.stdout)
    result.stderr = re.sub(r"password=[^\],\s]+", "password=<redacted>", result.stderr)
    (logs / f"{stamp}-{sequence:03d}.log").write_text(
        "$ " + " ".join(args) + "\n" + result.stdout + result.stderr, encoding="utf-8"
    )
    if check and result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {args}\n{result.stdout}{result.stderr}")
    return result


def node(index, *args, **kwargs):
    return run(["docker", "exec", "-i", f"hr-kafka-local-test-node-{index}-1", *args], **kwargs)


def deploy(index, role, command, **kwargs):
    return node(index, "bash", f"/root/deploy/kafka/{role}/deploy.sh", command, **kwargs)


def cli(tool, *args, account="admin", internal=False, target=None, **kwargs):
    options = ["--command-config", f"/test-clients/{account}.properties"] if authentication and not internal and account else []
    endpoint = target or (f"172.30.93.11:{manifest['brokers'][0]['port']}" if internal else bootstrap)
    return node(1, "docker", "run", "--rm", "-i", "--network", "host", "--user", "0",
                "--memory", "512m", "--memory-swap", "512m",
                "-v", f"{client_dir}:/test-clients:ro",
                "-e", "KAFKA_HEAP_OPTS=-Xms128m -Xmx256m",
                "-e", "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml",
                "--entrypoint", f"/opt/kafka/bin/{tool}", manifest["kafka"]["image"],
                "--bootstrap-server", endpoint, *options, *args, **kwargs)


def install_private(name, content):
    # Pass secrets on stdin; run() logs commands and outputs, never stdin.
    node(1, "sh", "-ec", f"umask 077; cat > {client_dir}/{name}.properties", data=content)


def install_clients():
    for name, content in client_configs.items():
        install_private(name, content)


def create_account(name):
    password = secrets.token_hex(24)
    install_private("account-update", f"SCRAM-SHA-512=iterations=8192,password={password}\n")
    try:
        cli("kafka-configs.sh", "--alter", "--entity-type", "users", "--entity-name", name,
            "--add-config-file", "/test-clients/account-update.properties")
    finally:
        node(1, "rm", "-f", f"{client_dir}/account-update.properties")
    content = re.sub(r'username="[^"]*" password="[^"]*"',
                     f'username="{name}" password="{password}"', client_configs["admin"])
    client_configs[name] = content
    path = cluster_dir / f"clients/{name}.properties"
    with path.open("w") as stream:
        os.chmod(path, 0o600)
        stream.write(content)
    install_private(name, content)


def wait_for(label, action, predicate, seconds=120):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        last = action()
        if last.returncode == 0 and predicate(last.stdout):
            checks.append(label)
            print(f"PASS {label}", flush=True)
            return last.stdout
        time.sleep(2)
    raise AssertionError(f"Timed out: {label}\n{last.stdout}\n{last.stderr}")


def quorum_ready(text):
    voters = re.search(r"CurrentVoters:\s*(\[.*\])", text)
    leader = re.search(r"LeaderId:\s*(\d+)", text)
    if not voters or not leader:
        return False
    ids = {v["id"] if isinstance(v, dict) else v for v in json.loads(voters.group(1))}
    return ids == {101, 102, 103} and int(leader.group(1)) in ids


def replication_ready(text):
    rows = [line.split() for line in text.splitlines() if re.match(r"^\s*10[123]\s", line)]
    return {int(row[0]) for row in rows} == {101, 102, 103} and all(row[3] == "0" for row in rows)


def topic_ready(text):
    rows = [line for line in text.splitlines() if re.search(r"\bPartition:\s*\d+", line)]
    if len(rows) != 3 or "min.insync.replicas=2" not in text:
        return False
    for row in rows:
        leader = re.search(r"Leader:\s*(\d+)", row)
        replicas = re.search(r"Replicas:\s*([0-9,]+)", row)
        isr = re.search(r"Isr:\s*([0-9,]+)", row)
        if not leader or int(leader.group(1)) not in {1, 2, 3}:
            return False
        if any(not field or set(field.group(1).split(",")) != {"1", "2", "3"}
               for field in (replicas, isr)):
            return False
    return True


def wait_topic(label):
    return wait_for(label, lambda: cli("kafka-topics.sh", "--describe", "--topic", topic, check=False), topic_ready)


messages = [f"{topic}-message-{i}" for i in range(1, 4)]


def consume(label):
    result = cli("kafka-console-consumer.sh", "--topic", topic, "--from-beginning",
                 "--group", f"{topic}-{label}", "--max-messages", "3", "--timeout-ms", "30000",
                 account="hr_consumer" if authentication else "admin")
    assert sorted(result.stdout.splitlines()) == sorted(messages), result.stdout
    checks.append(label)
    print(f"PASS {label}", flush=True)


try:
    node(1, "install", "-d", "-m", "700", client_dir)
    resources = []
    for index in (1, 2, 3):
        outer = json.loads(run(["docker", "inspect", f"hr-kafka-local-test-node-{index}-1"]).stdout)[0]
        assert outer["HostConfig"]["NanoCpus"] == 2_000_000_000
        assert outer["HostConfig"]["Memory"] == 4 * 1024**3
        for role in ("controller", "broker"):
            limits = manifest["resources"][role]
            cpu = int(Decimal(limits["cpu"]) * 10**9)
            memory = limits["memoryMiB"] * 1024**2
            info = json.loads(node(index, "docker", "inspect", f"{prefix}-{role}-{index}").stdout)[0]
            assert info["HostConfig"]["NanoCpus"] == cpu
            assert info["HostConfig"]["Memory"] == memory
            assert not info["State"]["OOMKilled"]
            resources.append({"node": index, "role": role, "cpu": cpu / 1e9, "memoryBytes": memory})
    checks.append("test resource limits")
    print("PASS test resource limits", flush=True)
    if authentication:
        admin = (cluster_dir / "clients/admin.properties").read_text()
        limits = "\ndefault.api.timeout.ms=10000\nrequest.timeout.ms=5000\n"
        client_configs["admin"] = admin + limits
        client_configs["wrong"] = re.sub(r'password="[^"]*"', 'password="invalid-password"', admin) + limits
        client_configs["anonymous"] = "security.protocol=PLAINTEXT\n" + limits
        install_clients()
    quorum = wait_for("controller quorum", lambda: cli("kafka-metadata-quorum.sh", "describe", "--status", check=False), quorum_ready)
    replication = wait_for("controller replication lag zero", lambda: cli("kafka-metadata-quorum.sh", "describe", "--replication", check=False), replication_ready)
    brokers = wait_for("three brokers registered", lambda: cli("kafka-broker-api-versions.sh", check=False),
                       lambda text: set(re.findall(r"\bid:\s*(\d+)", text)) == {"1", "2", "3"})
    if authentication:
        for host in (11, 12, 13):
            cli("kafka-topics.sh", "--list", target=f"172.30.93.{host}:{client_port}")
        for account in ("wrong", "anonymous"):
            denied = cli("kafka-topics.sh", "--list", account=account, check=False)
            assert denied.returncode != 0, f"unexpected authentication success: {account}"
            expected = "SaslAuthenticationException" if account == "wrong" else "TimeoutException"
            assert expected in denied.stdout + denied.stderr
        cli("kafka-topics.sh", "--list", internal=True)
        create_account("hr_producer")
        create_account("hr_consumer")
        checks.append("all broker authentication endpoints; wrong and anonymous denied; INTERNAL unauthenticated; independent accounts")
        print("PASS authentication endpoints, negative checks, INTERNAL and independent accounts", flush=True)
    cli("kafka-topics.sh", "--create", "--topic", topic, "--partitions", "3",
        "--replication-factor", "3", "--config", "min.insync.replicas=2")
    description = wait_topic("three partitions with three replicas and ISR 3")
    cli("kafka-console-producer.sh", "--topic", topic, "--producer-property", "acks=all",
        account="hr_producer" if authentication else "admin", data="\n".join(messages) + "\n")
    consume("produce-consume")

    missing = f"{topic}-missing"
    negative = cli("kafka-console-producer.sh", "--topic", missing,
                   "--producer-property", "max.block.ms=3000",
                   "--producer-property", "delivery.timeout.ms=4000",
                   "--producer-property", "request.timeout.ms=2000",
                   "--producer-property", "retries=0", data="must-not-create\n", check=False,
                   account="hr_producer" if authentication else "admin")
    assert "TimeoutException" in negative.stderr + negative.stdout, negative
    assert missing not in cli("kafka-topics.sh", "--list").stdout.splitlines()
    checks.append("automatic topic creation disabled")
    print("PASS automatic topic creation disabled", flush=True)

    identities = {}
    for index in (1, 2, 3):
        for role, leaf in (("controller", "metadata"), ("broker", "data")):
            base_dir = manifest['paths']['dataBaseDir']
            path = f"{base_dir}/{role}/{leaf}/meta.properties"
            before = node(index, "sha256sum", path).stdout
            prepared = deploy(index, role, "prepare")
            assert "身份一致，跳过" in prepared.stdout
            assert node(index, "sha256sum", path).stdout == before
            identities[f"{role}-{index}"] = before.split()[0]
            deploy(index, role, "status")
    checks.append("prepare idempotent for all six roles")
    print("PASS prepare idempotent for all six roles", flush=True)

    deploy(1, "broker", "restart")
    wait_topic("ISR restored after broker restart")
    consume("messages-after-restart")
    deploy(1, "broker", "stop")
    deploy(1, "broker", "start")
    wait_topic("ISR restored after broker recreation")
    consume("messages-after-recreation")
    for index in (1, 2, 3):
        info = json.loads(node(index, "docker", "inspect", f"{prefix}-controller-{index}", f"{prefix}-broker-{index}").stdout)
        assert all(item["State"]["Running"] and not item["State"]["OOMKilled"] for item in info)
    result = {"status": "passed", "completedAtUTC": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "topology": "three Docker-in-Docker Linux nodes; not independent VMs",
              "kafkaImage": manifest["kafka"]["image"], "profile": manifest["profile"], "topic": topic,
              "security": manifest.get("security", {}), "bootstrapServers": bootstrap,
              "checks": checks, "resources": resources, "identityChecksums": identities,
              "quorum": quorum, "replication": replication, "brokers": brokers, "topicDescription": description}
    (runtime / "result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(f"All functional checks passed. Evidence: {runtime / 'result.json'}", flush=True)
except Exception:
    for index in (1, 2, 3):
        for role in ("controller", "broker"):
            node(index, "docker", "logs", "--tail", "100", f"{prefix}-{role}-{index}", check=False)
    raise
finally:
    node(1, "rm", "-rf", client_dir, check=False)

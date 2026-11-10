#!/usr/bin/env python3
"""Functional checks only; run against the isolated test-local.sh deployment."""
import datetime
import json
import pathlib
import re
import subprocess
import sys
import time

runtime = pathlib.Path(sys.argv[1])
logs = runtime / "logs"
prefix = "hr-kafka-local-test"
bootstrap = "172.30.93.11:9092,172.30.93.12:9092,172.30.93.13:9092"
stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
topic = f"deploy-smoke-{stamp.lower()}"
sequence = 0
checks = []


def run(args, *, data=None, check=True, timeout=60):
    global sequence
    sequence += 1
    result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    (logs / f"{stamp}-{sequence:03d}.log").write_text(
        "$ " + " ".join(args) + "\n" + result.stdout + result.stderr, encoding="utf-8"
    )
    if check and result.returncode:
        raise RuntimeError(f"Command failed ({result.returncode}): {args}\n{result.stdout}{result.stderr}")
    return result


def node(index, *args, **kwargs):
    return run(["docker", "exec", "-i", f"{prefix}-node-{index}-1", *args], **kwargs)


def deploy(index, role, command, **kwargs):
    return node(index, "bash", f"/root/deploy/kafka/{role}-{index}/deploy.sh", command, **kwargs)


def cli(tool, *args, **kwargs):
    return node(1, "docker", "exec", "-i", "-e", "KAFKA_HEAP_OPTS=-Xms128m -Xmx256m",
                "-e", "KAFKA_LOG4J_OPTS=-Dlog4j2.configurationFile=/opt/kafka/config/tools-log4j2.yaml",
                f"{prefix}-broker-1", f"/opt/kafka/bin/{tool}",
                "--bootstrap-server", bootstrap, *args, **kwargs)


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
                 "--group", f"{topic}-{label}", "--max-messages", "3", "--timeout-ms", "30000")
    assert sorted(result.stdout.splitlines()) == sorted(messages), result.stdout
    checks.append(label)
    print(f"PASS {label}", flush=True)


try:
    resources = []
    for index in (1, 2, 3):
        outer = json.loads(run(["docker", "inspect", f"{prefix}-node-{index}-1"]).stdout)[0]
        assert outer["HostConfig"]["NanoCpus"] == 2_000_000_000
        assert outer["HostConfig"]["Memory"] == 4 * 1024**3
        for role, cpu, memory in (("controller", 500_000_000, 512 * 1024**2),
                                  ("broker", 1_000_000_000, 1536 * 1024**2)):
            info = json.loads(node(index, "docker", "inspect", f"{prefix}-{role}-{index}").stdout)[0]
            assert info["HostConfig"]["NanoCpus"] == cpu
            assert info["HostConfig"]["Memory"] == memory
            assert not info["State"]["OOMKilled"]
            resources.append({"node": index, "role": role, "cpu": cpu / 1e9, "memoryBytes": memory})
    checks.append("test resource limits")
    print("PASS test resource limits", flush=True)
    quorum = wait_for("controller quorum", lambda: cli("kafka-metadata-quorum.sh", "describe", "--status", check=False), quorum_ready)
    replication = wait_for("controller replication lag zero", lambda: cli("kafka-metadata-quorum.sh", "describe", "--replication", check=False), replication_ready)
    brokers = wait_for("three brokers registered", lambda: cli("kafka-broker-api-versions.sh", check=False),
                       lambda text: set(re.findall(r"\bid:\s*(\d+)", text)) == {"1", "2", "3"})
    cli("kafka-topics.sh", "--create", "--topic", topic, "--partitions", "3",
        "--replication-factor", "3", "--config", "min.insync.replicas=2")
    description = wait_topic("three partitions with three replicas and ISR 3")
    cli("kafka-console-producer.sh", "--topic", topic, "--producer-property", "acks=all",
        data="\n".join(messages) + "\n")
    consume("produce-consume")

    missing = f"{topic}-missing"
    negative = cli("kafka-console-producer.sh", "--topic", missing,
                   "--producer-property", "max.block.ms=3000",
                   "--producer-property", "delivery.timeout.ms=4000",
                   "--producer-property", "request.timeout.ms=2000",
                   "--producer-property", "retries=0", data="must-not-create\n", check=False)
    assert "TimeoutException" in negative.stderr + negative.stdout, negative
    assert missing not in cli("kafka-topics.sh", "--list").stdout.splitlines()
    checks.append("automatic topic creation disabled")
    print("PASS automatic topic creation disabled", flush=True)

    identities = {}
    for index in (1, 2, 3):
        for role, leaf in (("controller", "metadata"), ("broker", "data")):
            path = f"/data1/kafka/{role}-{index}/{leaf}/meta.properties"
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
              "kafkaImage": "apache/kafka:4.3.2", "profile": "test", "topic": topic,
              "checks": checks, "resources": resources, "identityChecksums": identities,
              "quorum": quorum, "replication": replication, "brokers": brokers, "topicDescription": description}
    (runtime / "result.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(f"All functional checks passed. Evidence: {runtime / 'result.json'}", flush=True)
except Exception:
    for index in (1, 2, 3):
        for role in ("controller", "broker"):
            node(index, "docker", "logs", "--tail", "100", f"{prefix}-{role}-{index}", check=False)
    raise

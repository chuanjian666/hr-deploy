#!/usr/bin/env python3
"""Isolated Docker authentication smoke test: one controller and three brokers.

Uses the generated configuration with a single-controller quorum to limit resources.
Does not deploy to or modify the existing test-local.sh cluster. Standard library only.
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import uuid


ROOT = Path(__file__).resolve().parents[1]
IMAGE = os.environ.get("KAFKA_AUTH_TEST_IMAGE", "apache/kafka:4.3.2")
PREFIX = "kafka-auth-" + uuid.uuid4().hex[:8]
CONTAINERS = []


def run(args, *, data=None, check=True, timeout=60, env=None):
    result = subprocess.run(args, input=data, text=True, capture_output=True,
                            timeout=timeout, env=env)
    if check and result.returncode:
        raise RuntimeError(f"Command failed: {args}\n{result.stdout}{result.stderr}")
    return result


def docker(*args, **kwargs):
    return run(["docker", *args], **kwargs)


def wait_for(label, action, predicate=lambda result: result.returncode == 0):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        result = action()
        if predicate(result):
            print(f"PASS {label}", flush=True)
            return result
        time.sleep(2)
    raise AssertionError(f"Timed out: {label}\n{result.stdout}{result.stderr}")


def cli(tool, *args, config="admin", authenticated=True, **kwargs):
    port = 19092 if authenticated else 9092
    options = ["--command-config", f"/test-clients/{config}.properties"] if config else []
    return docker("exec", "-i", "-e", "KAFKA_HEAP_OPTS=-Xms64m -Xmx128m",
                  f"{PREFIX}-broker-1", f"/opt/kafka/bin/{tool}",
                  "--bootstrap-server", f"broker-1:{port}", *options, *args, **kwargs)


def main():
    with tempfile.TemporaryDirectory(prefix="kafka-auth-") as temp:
        work = Path(temp)
        config = work / "kafka-admin.conf"
        config.write_text((ROOT / "config/kafka-admin.conf.example").read_text() + '''
CONTROLLER_HOSTS="controller-1,controller-2,controller-3"
BROKER_HOSTS="broker-1,broker-2,broker-3"
DEPLOY_PROFILE="manual"
MANUAL_CONTROLLER_CPU=0.5
MANUAL_CONTROLLER_MEMORY=512m
MANUAL_BROKER_CPU=1
MANUAL_BROKER_MEMORY=1536m
ENABLE_AUTH="true"
AUTH_PASSWORD="Auth-Smoke-Password_2026"
''')
        env = dict(os.environ, KAFKA_CONFIG_FILE=str(config),
                   KAFKA_CLUSTER_DIR=str(work / "cluster"), KAFKA_INTERACTIVE="false")
        run([str(ROOT / "kafka-admin.sh"), "cluster", "init"], env=env)
        cluster = work / "cluster"
        manifest = json.loads((cluster / "manifest.json").read_text())
        clients = cluster / "clients"
        admin = (clients / "admin.properties").read_text()
        # Bound failures so negative checks cannot hang on metadata retries.
        limits = "\ndefault.api.timeout.ms=5000\nrequest.timeout.ms=3000\n"
        (clients / "admin.properties").write_text(admin + limits)
        (clients / "wrong.properties").write_text(admin.replace("Auth-Smoke-Password_2026", "wrong-password") + limits)
        (clients / "anonymous.properties").write_text("security.protocol=PLAINTEXT\n" + limits)
        docker("network", "create", PREFIX)
        try:
            for role, index in [("controller", 1), ("broker", 1), ("broker", 2), ("broker", 3)]:
                alias = f"{role}-{index}"
                package = cluster / "nodes" / f"{alias}-{alias}"
                props = package / "config/server.properties"
                props.write_text(props.read_text().replace(
                    "controller-1:9093,controller-2:9093,controller-3:9093", "controller-1:9093"))
                data = work / f"{alias}-data"
                data.mkdir()
                mounts = ["-v", f"{package / 'config'}:/etc/kafka/custom:ro",
                          "-v", f"{data}:/var/lib/kafka/data"]
                format_args = ["format", "--cluster-id", manifest["cluster"]["id"],
                               "--config", "/etc/kafka/custom/server.properties"]
                if role == "controller":
                    directory_id = manifest["controllers"][0]["directoryId"]
                    format_args += ["--initial-controllers", f"101@controller-1:9093:{directory_id}",
                                    "--add-scram", (package / "config/bootstrap.scram").read_text().strip()]
                else:
                    format_args += ["--no-initial-controllers"]
                # Root only in this portable smoke test, to read host-owned 600 bind mounts.
                docker("run", "--rm", "--user", "0", "--network", PREFIX, *mounts,
                       "-e", "KAFKA_HEAP_OPTS=-Xms64m -Xmx128m",
                       "--entrypoint", "/opt/kafka/bin/kafka-storage.sh", IMAGE, *format_args)
                name = f"{PREFIX}-{alias}"
                CONTAINERS.append(name)
                docker("run", "-d", "--name", name, "--network", PREFIX,
                       "--network-alias", alias, "--user", "0", "--memory", "512m",
                       *mounts, "-v", f"{clients}:/test-clients:ro",
                       "-e", "KAFKA_HEAP_OPTS=-Xms128m -Xmx256m",
                       "--entrypoint", "/opt/kafka/bin/kafka-server-start.sh",
                       IMAGE, "/etc/kafka/custom/server.properties")

            wait_for("initial SCRAM account", lambda: cli("kafka-topics.sh", "--list", check=False))
            for account in ("producer", "consumer"):
                password = f"Smoke-{account}-Password_2026"
                cli("kafka-configs.sh", "--alter", "--entity-type", "users", "--entity-name", account,
                    "--add-config", f"SCRAM-SHA-512=[iterations=8192,password={password}]")
                (clients / f"{account}.properties").write_text(
                    admin.replace('username="admin"', f'username="{account}"').replace(
                        "Auth-Smoke-Password_2026", password) + limits)
            print("PASS independent producer and consumer accounts created", flush=True)
            for config_name in ("wrong", "anonymous"):
                result = cli("kafka-topics.sh", "--list", config=config_name, check=False)
                assert result.returncode != 0, config_name
                assert ("SaslAuthenticationException" if config_name == "wrong" else "TimeoutException") in result.stdout + result.stderr
                print(f"PASS rejected {config_name} credentials", flush=True)
            cli("kafka-topics.sh", "--create", "--topic", "auth-smoke", "--partitions", "1",
                "--replication-factor", "3", "--config", "min.insync.replicas=2")
            wait_for("three replicas over unauthenticated INTERNAL", lambda: cli(
                "kafka-topics.sh", "--describe", "--topic", "auth-smoke", check=False),
                lambda result: result.returncode == 0 and any(
                    "Isr:" in line and len(line.split("Isr:")[1].split()[0].split(",")) == 3
                    for line in result.stdout.splitlines()))
            cli("kafka-console-producer.sh", "--topic", "auth-smoke", "--producer-property", "acks=all",
                config="producer", data="authenticated-message\n")
            result = cli("kafka-console-consumer.sh", "--topic", "auth-smoke", "--from-beginning",
                         "--group", "auth-smoke-group", "--max-messages", "1", "--timeout-ms", "15000",
                         config="consumer")
            assert result.stdout.strip() == "authenticated-message", result.stdout
            print("PASS separate accounts produce and consume", flush=True)
            cli("kafka-topics.sh", "--list", authenticated=False, config=None)
            print("PASS INTERNAL remains unauthenticated", flush=True)
            cli("kafka-configs.sh", "--alter", "--entity-type", "users", "--entity-name", "producer",
                "--add-config", "SCRAM-SHA-512=[iterations=8192,password=Rotated-Producer-Password_2026]")
            result = cli("kafka-topics.sh", "--list", config="producer", check=False)
            assert result.returncode != 0 and "SaslAuthenticationException" in result.stdout + result.stderr
            producer_file = clients / "producer.properties"
            producer_file.write_text(producer_file.read_text().replace(
                "Smoke-producer-Password_2026", "Rotated-Producer-Password_2026"))
            cli("kafka-topics.sh", "--list", config="producer")
            cli("kafka-topics.sh", "--list", config="consumer")
            print("PASS password rotation leaves other accounts working", flush=True)
            cli("kafka-configs.sh", "--alter", "--entity-type", "users", "--entity-name", "producer",
                "--delete-config", "SCRAM-SHA-512")
            result = cli("kafka-topics.sh", "--list", config="producer", check=False)
            assert result.returncode != 0 and "SaslAuthenticationException" in result.stdout + result.stderr
            print("PASS deleted account rejected on new connection", flush=True)
        except BaseException:
            for name in CONTAINERS:
                result = docker("logs", "--tail", "35", name, check=False)
                print(name, result.stdout, result.stderr)
            raise
        finally:
            if CONTAINERS:
                docker("rm", "-f", *CONTAINERS, check=False)
            docker("network", "rm", PREFIX, check=False)


if __name__ == "__main__":
    main()

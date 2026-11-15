#!/usr/bin/env python3
"""Exercise the generated controller health command using simulated CLI responses."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = (ROOT / 'templates/deploy.sh.tmpl').read_text()
ENDPOINTS = [f'10.20.0.{i}:9093' for i in (11, 12, 13)]


class ControllerHealthTest(unittest.TestCase):
    def run_health(self, *, ids=(101, 102, 103), cluster='test-cluster', voters=None,
                   leader=None, watermark=10, replication=None, failed_endpoint=None,
                   different_leader=False, role='controller', wait='0', retry=False):
        leader = ids[0] if leader is None else leader
        voters = [{'id': i, 'directoryId': f'directory-{i}'} for i in ids] if voters is None else voters
        with tempfile.TemporaryDirectory(prefix='controller-health-') as tmp:
            root = Path(tmp)
            for index, endpoint in enumerate(ENDPOINTS):
                selected_leader = ids[1] if different_leader and index == 1 else leader
                status = (f'ClusterId: {cluster}\nLeaderId: {selected_leader}\n'
                          f'HighWatermark: {watermark}\nCurrentVoters: {json.dumps(voters)}\n')
                (root / f'{endpoint}.--status').write_text(status)
                if retry:
                    (root / 'not-ready').write_text(status.replace(f'LeaderId: {selected_leader}', 'LeaderId: -1'))
            if replication is None:
                replication = 'NodeId DirectoryId LogEndOffset Lag LastFetchTimestamp LastCaughtUpTimestamp Status\n'
                for i in ids:
                    replication += f'{i} directory-{i} 10 0 1000 1000 {"Leader" if i == ids[0] else "Follower"}\n'
                replication += '1 broker-dir 10 0 1000 1000 Observer\n'
            (root / f'{ENDPOINTS[0]}.--replication').write_text(replication)
            script = root / 'deploy.sh'
            prefix = TEMPLATE.split('\ncase "${1:-}" in', 1)[0]
            script.write_text(prefix + r'''
load_env() {
    NODE_ROLE=$MOCK_ROLE
    CLUSTER_ID=test-cluster
    INITIAL_CONTROLLERS=$MOCK_INITIAL
    KAFKA_IMAGE=test-kafka-image
}
check_files() { :; }
check_runtime() { :; }
docker() {
    [[ "$1" == run && "$*" == *"--network host"* && "$*" == *"--bootstrap-controller"* ]] || return 9
    [[ "$*" == *"--memory 512m --memory-swap 512m"* ]] || return 9
    [[ "$*" == *"-Xms128m -Xmx256m"* && "$*" == *"default.api.timeout.ms=5000"* ]] || return 9
    local -a args=("$@")
    local count=${#args[@]} endpoint query
    endpoint=${args[$((count-2))]}; query=${args[$((count-1))]}
    if [[ "$endpoint" == "$MOCK_FAILED_ENDPOINT" ]]; then echo 'connection failed' >&2; return 1; fi
    if [[ "$MOCK_RETRY" == true && ! -f "$MOCK_DIR/attempted" ]]; then
        touch "$MOCK_DIR/attempted"
        cat "$MOCK_DIR/not-ready"
        return
    fi
    cat "$MOCK_DIR/$endpoint.$query"
}
sleep() { :; }
health "$@"
''')
            initial = ','.join(f'{i}@{endpoint}:directory-{i}' for i, endpoint in zip(ids, ENDPOINTS))
            env = dict(os.environ, MOCK_ROLE=role, MOCK_INITIAL=initial, MOCK_DIR=str(root),
                       MOCK_RETRY=str(retry).lower(), MOCK_FAILED_ENDPOINT=failed_endpoint or '')
            return subprocess.run(['bash', str(script), 'health', wait], env=env,
                                  text=True, capture_output=True, timeout=10)

    def test_healthy_and_dynamic_ids(self):
        for ids in ((101, 102, 103), (201, 202, 203)):
            result = self.run_health(ids=ids)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertIn('controller 集群健康', result.stdout)

    def test_invalid_voter_format(self):
        result = self.run_health(voters=[101, 102, 103])
        self.assertNotEqual(result.returncode, 0)

    def test_status_failures(self):
        for settings in (dict(cluster='other-cluster'), dict(leader=-1), dict(watermark=-1),
                         dict(voters=[101, 102]), dict(voters=[101, 101, 103]),
                         dict(different_leader=True), dict(failed_endpoint=ENDPOINTS[2])):
            result = self.run_health(**settings)
            self.assertNotEqual(result.returncode, 0, (settings, result.stdout))
            self.assertIn('controller 集群未就绪', result.stderr)

    def test_replication_failures(self):
        header = 'NodeId DirectoryId LogEndOffset Lag LastFetchTimestamp LastCaughtUpTimestamp Status\n'
        rows = ['101 d1 10 0 -1 -1 Leader\n', '102 d2 10 0 1000 1000 Follower\n',
                '103 d3 10 0 1000 1000 Follower\n']
        for replication in ('invalid output', header+''.join(rows[:2]),
                            header+''.join(rows).replace('103 d3 10 0', '103 d3 10 1'),
                            header+''.join(rows).replace('102 d2 10 0 1000 1000 Follower', '102 d2 10 0 1000 1000 Observer'),
                            header+''.join(rows)+rows[1]):
            result = self.run_health(replication=replication)
            self.assertNotEqual(result.returncode, 0, result.stdout)

    def test_wait_until_ready(self):
        result = self.run_health(retry=True, wait='3')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('重试', result.stderr)

    def test_invalid_arguments_and_role(self):
        for settings in (dict(role='broker'), dict(wait='-1'), dict(wait='abc'), dict(wait='86401')):
            result = self.run_health(**settings)
            self.assertNotEqual(result.returncode, 0)


if __name__ == '__main__':
    unittest.main()

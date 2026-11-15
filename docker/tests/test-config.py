#!/usr/bin/env python3
"""Check Docker setup configuration without root, a daemon, or host changes."""
import json
import pathlib
import subprocess
import tempfile


source = (pathlib.Path(__file__).resolve().parents[1] / 'setup-docker.sh').read_text()
with tempfile.TemporaryDirectory(prefix='docker-storage-config-') as tmp:
    root = pathlib.Path(tmp).resolve()
    config_dir = root / 'etc-docker'
    config_dir.mkdir()
    probe = root / 'setup-probe.sh'
    # Redirect configuration writes and replace host installation/service operations.
    code = source.rsplit('\nmain', 1)[0].replace('/etc/docker', str(config_dir))
    probe.write_text(code + '''
check_root() { :; }
detect_os() { :; }
check_docker() { return 0; }
start_docker() { :; }
verify_docker() { :; }
main "$@"
''')
    storage = root / 'custom-storage'
    result = subprocess.run(['bash', str(probe), '--reconfigure', '--data-root', str(storage), '-y'],
                            text=True, capture_output=True)
    assert result.returncode == 0, result.stderr + result.stdout
    config = config_dir / 'daemon.json'
    assert json.loads(config.read_text())['data-root'] == str(storage)
    assert storage.is_dir()
    before = config.read_bytes()
    for invalid in ('/', 'relative', '/tmp/a b', '/tmp/a;id', '/tmp/../escape', '/tmp/a/', '/tmp/a"b'):
        result = subprocess.run(['bash', str(probe), '--reconfigure', '--data-root', invalid, '-y'],
                                text=True, capture_output=True)
        assert result.returncode != 0, f'invalid path accepted: {invalid}'
        assert config.read_bytes() == before, 'invalid path changed Docker configuration'
print('Docker storage configuration tests passed')

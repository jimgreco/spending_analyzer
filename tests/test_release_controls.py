"""Release policy: PR validation is secret-free; only main pushes deploy, after CI, via the helper."""
from pathlib import Path
import re
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]
REMOTE = re.compile(r'\b(?:ssh|scp|rsync|prune)\b', re.I)


def load(name):
    source = (ROOT / '.github/workflows' / name).read_text()
    text = '\n'.join(line for line in source.splitlines() if not line.lstrip().startswith('#'))
    return text, yaml.load(text, Loader=yaml.BaseLoader)


def job_text(job):
    return yaml.dump(job, default_flow_style=False, width=10000)


def validate_ci(text):
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    assert set(workflow['on']) == {'pull_request', 'workflow_call', 'workflow_dispatch'}
    assert workflow['permissions'] == {'contents': 'read'}
    assert not re.search(r'\bsecrets\b', text)
    assert not REMOTE.search(text)
    assert not re.search(r'\b(?:docker-compose|compose\s+(?:up|down|restart|build))\b', text)
    assert not re.search(r'\b(?:workflow_run|pull_request_target)\b', text)
    assert not re.search(r'\b(?:docker\s+(?:push|login)|gh\s+workflow\s+run)\b', text)
    assert workflow['jobs']['required']['needs'] == ['tests', 'image']
    assert workflow['jobs']['image']['runs-on'] == 'ubuntu-24.04-arm'
    assert '--platform linux/arm64' in workflow['jobs']['image']['steps'][-1]['run']
    for job in workflow['jobs'].values():
        assert 'environment' not in job
        assert 'permissions' not in job


def validate_deploy(text):
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    assert set(workflow['on']) == {'push', 'workflow_dispatch'}
    assert workflow['on']['push'] == {'branches': ['main']}
    assert not re.search(r'\b(?:pull_request|pull_request_target|workflow_run)\b', text)
    assert workflow['permissions'] == {'contents': 'read'}
    assert workflow['concurrency']['cancel-in-progress'] == 'false'
    assert set(workflow['jobs']) == {'validate', 'deploy'}

    validate = workflow['jobs']['validate']
    # The full CI workflow (ending in Required Checks) runs first, with no secrets passed.
    assert validate == {'name': validate.get('name'), 'uses': './.github/workflows/ci.yml'}

    deploy = workflow['jobs']['deploy']
    assert deploy['needs'] == 'validate'
    assert "github.ref == 'refs/heads/main'" in deploy['if']
    assert deploy['environment'] == 'production'
    assert 'permissions' not in deploy

    steps = deploy['steps']
    body = job_text(deploy) + '\n'.join(step.get('run', '') for step in steps)
    assert 'secrets.EC2_SSH_KNOWN_HOSTS' in body
    # Secrets live only in the deploy job.
    assert 'secrets' not in job_text(validate)
    # Host key is pinned from the secret; no fallback to trust-on-first-use.
    assert not re.search(r'StrictHostKeyChecking[\s=]+(?!yes\b)', body)
    assert "StrictHostKeyChecking yes" in body and 'UserKnownHostsFile $dir/known_hosts' in body
    assert '"$EC2_SSH_KNOWN_HOSTS" > "$dir/known_hosts"' in body
    for step in steps:
        for line in step.get('run', '').splitlines():
            if re.search(r'^\s*(?:ssh|rsync)(?![\w-])', line):
                assert re.search(r'ssh -F "?\$SSH_CONFIG"? ', line), line
            assert not re.search(r'\bssh\s+-o|\bssh\s+-i', line), line
    # Only the single-service helper touches containers; no compose/prune in the workflow.
    assert not re.search(r'\b(?:docker-compose|docker\s+compose|docker\s+(?:system|builder|image)\s+prune|prune)\b', body)
    remote = [step for step in steps if '--release' in step.get('run', '')]
    assert len(remote) == 1 and 'deploy-service.py' in remote[0]['run'] and '$GITHUB_SHA' in remote[0]['run']
    assert any('/api/version' in step.get('run', '') for step in steps)
    cleanup = steps[-1]
    assert cleanup.get('if') == 'always()' and 'rm -rf' in cleanup['run'] and 'spending-ssh' in cleanup['run']
    names = [step.get('name', '') for step in steps]
    assert names.index('Bake version metadata') < names.index('Transfer webapp source')
    assert names.index('Transfer release helper and run read-only preflight') < names.index('Transfer webapp source')


class ValidationWorkflow(unittest.TestCase):
    def setUp(self):
        self.text, _ = load('ci.yml')

    def test_validation_workflow_is_secret_free(self):
        validate_ci(self.text)

    def test_rejects_remote_deploy_or_secret_reintroduction(self):
        for command in ('ssh host command', 'docker system prune -f', 'rsync source host:dest',
                        'docker-compose up -d spending', 'docker push example', '${{ secrets.EC2_SSH_KEY }}'):
            with self.subTest(command=command), self.assertRaises(AssertionError):
                validate_ci(self.text.replace('test "$TESTS_RESULT"', command + ' && test "$TESTS_RESULT"'))

    def test_rejects_privileged_or_push_triggers(self):
        for old, new in (('  pull_request:', '  pull_request_target:'),
                         ('  workflow_dispatch:', '  workflow_dispatch:\n  push:\n    branches: [main]')):
            with self.subTest(new=new), self.assertRaises(AssertionError):
                validate_ci(self.text.replace(old, new))

    def test_rejects_environment_in_validation(self):
        with self.assertRaises(AssertionError):
            validate_ci(self.text.replace('    runs-on: ubuntu-24.04-arm',
                                          '    runs-on: ubuntu-24.04-arm\n    environment: production'))


class DeployWorkflow(unittest.TestCase):
    def setUp(self):
        self.text, _ = load('deploy.yml')

    def test_deploys_only_main_after_validation(self):
        validate_deploy(self.text)

    def test_rejects_pull_request_triggers(self):
        for trigger in ('pull_request', 'pull_request_target', 'workflow_run'):
            with self.subTest(trigger=trigger), self.assertRaises(AssertionError):
                validate_deploy(self.text.replace('  workflow_dispatch:', '  workflow_dispatch:\n  {}:'.format(trigger), 1))

    def test_rejects_unpinned_host_keys(self):
        for old, new in (("StrictHostKeyChecking yes", "StrictHostKeyChecking no"),
                         ("StrictHostKeyChecking yes", "StrictHostKeyChecking accept-new"),
                         ('ssh -F "$SSH_CONFIG" production', 'ssh -o StrictHostKeyChecking=no host')):
            with self.subTest(new=new), self.assertRaises(AssertionError):
                validate_deploy(self.text.replace(old, new))

    def test_rejects_skipping_validation_or_environment(self):
        for old, new in (('    needs: validate\n', ''), ('    environment: production\n', ''),
                         ("github.ref == 'refs/heads/main' && ", ''),
                         ('    uses: ./.github/workflows/ci.yml',
                          '    uses: ./.github/workflows/ci.yml\n    secrets: inherit')):
            with self.subTest(old=old), self.assertRaises((AssertionError, KeyError)):
                validate_deploy(self.text.replace(old, new))

    def test_rejects_direct_compose_or_prune(self):
        for command in ('docker-compose up -d spending', 'docker compose build spending', 'docker system prune -f'):
            with self.subTest(command=command), self.assertRaises(AssertionError):
                validate_deploy(self.text.replace('set -euo pipefail\n          expected=',
                                                  'set -euo pipefail\n          ' + command + '\n          expected='))

    def test_ssh_material_cleanup_is_unconditional(self):
        with self.assertRaises(AssertionError):
            validate_deploy(self.text.replace('        if: always()\n', ''))


class ImageMetadata(unittest.TestCase):
    def test_baked_version_reaches_the_image(self):
        ignored = (ROOT / 'webapp/.dockerignore').read_text().split()
        self.assertNotIn('version.json', ignored)


if __name__ == '__main__':
    unittest.main()

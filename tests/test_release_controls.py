"""Release policy: PRs only verify; only main releases, after verify, via the pinned shared engine."""
import json
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


def validate_verify(text):
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    # Only release.yml calls it; it has no triggers of its own.
    assert set(workflow['on']) == {'workflow_call'}
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


RT_SHA = 'bfd9d14bb12907b74bceaa28b7c0382fb3549ea6'
RELEASE_WORKFLOW = 'jimgreco/release-tools/.github/workflows/server-release.yml@' + RT_SHA
EC2_SECRETS = ('EC2_HOST', 'EC2_USER', 'EC2_SSH_KEY', 'EC2_SSH_KNOWN_HOSTS')


def validate_release(text):
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    assert set(workflow['on']) == {'pull_request', 'push', 'workflow_dispatch'}
    assert workflow['on']['push'] == {'branches': ['main']}
    assert set(workflow['on']['workflow_dispatch']['inputs']) == {'server'}
    assert not re.search(r'\b(?:pull_request_target|workflow_run)\b', text)
    assert workflow['permissions'] == {'contents': 'read'}
    # Only pull requests cancel in progress; releases on main queue.
    assert workflow['concurrency']['cancel-in-progress'] == "${{ github.event_name == 'pull_request' }}"
    assert set(workflow['jobs']) == {'verify', 'server'}

    verify = workflow['jobs']['verify']
    # Every check in verify.yml (ending in Required Checks) runs first.
    assert verify == {'uses': './.github/workflows/verify.yml', 'secrets': 'inherit'}

    server = workflow['jobs']['server']
    assert server['needs'] == 'verify'
    # Pull requests never release: only main, on push or a manual run that selects the server.
    assert server['if'] == ("github.ref == 'refs/heads/main' && (github.event_name == 'push' || "
                            "(github.event_name == 'workflow_dispatch' && inputs.server))")
    # The shared engine is pinned by full commit, and tools_ref names the same commit.
    assert server['uses'] == RELEASE_WORKFLOW
    assert server['with'] == {'tools_ref': RT_SHA}
    assert server['permissions'] == {'contents': 'read', 'packages': 'write'}
    assert server['secrets'] == {name: '${{ secrets.%s }}' % name for name in EC2_SECRETS}
    assert set(server) <= {'needs', 'if', 'permissions', 'uses', 'with', 'secrets'}
    # No app-local host access: the engine owns ssh, Compose and health checks.
    assert not REMOTE.search(text)
    assert not re.search(r'\b(?:docker-compose|docker\s+compose|deploy-service)\b', text)


def validate_manifest(manifest):
    assert manifest['app'] == 'spending'
    assert manifest['image'] == 'ghcr.io/jimgreco/spending_analyzer'
    assert set(manifest['services']) == {'spending'}
    # DB runtime-role cutover overlays must stay in the Compose chain.
    for pattern in manifest.get('legacy_overrides', []):
        assert not re.fullmatch(pattern, 'db-runtime-spending-0123456789abcdef.json'), pattern
    # The container never migrates on start and no release step migrates.
    assert 'migrate' not in manifest
    assert '{sha7}' in manifest['services']['spending']['health']
    assert 'public' not in manifest and 'backup' not in manifest
    assert manifest['refuse_backwards'] is True


class WorkflowSet(unittest.TestCase):
    def test_only_the_standard_workflows_exist(self):
        names = {p.name for p in (ROOT / '.github/workflows').iterdir()}
        self.assertEqual(names, {'release.yml', 'verify.yml'})


class VerifyWorkflow(unittest.TestCase):
    def setUp(self):
        self.text, _ = load('verify.yml')

    def test_verify_workflow_is_secret_free(self):
        validate_verify(self.text)

    def test_rejects_remote_deploy_or_secret_reintroduction(self):
        for command in ('ssh host command', 'docker system prune -f', 'rsync source host:dest',
                        'docker-compose up -d spending', 'docker push example', '${{ secrets.EC2_SSH_KEY }}'):
            with self.subTest(command=command), self.assertRaises(AssertionError):
                validate_verify(self.text.replace('test "$TESTS_RESULT"', command + ' && test "$TESTS_RESULT"'))

    def test_rejects_own_triggers(self):
        for trigger in ('pull_request:', 'pull_request_target:', 'workflow_dispatch:', 'push:\n    branches: [main]'):
            with self.subTest(trigger=trigger), self.assertRaises(AssertionError):
                validate_verify(self.text.replace('  workflow_call:', '  workflow_call:\n  ' + trigger, 1))

    def test_rejects_environment_in_verification(self):
        with self.assertRaises(AssertionError):
            validate_verify(self.text.replace('    runs-on: ubuntu-24.04-arm',
                                              '    runs-on: ubuntu-24.04-arm\n    environment: production'))


class ReleaseWorkflow(unittest.TestCase):
    def setUp(self):
        self.text, _ = load('release.yml')

    def test_releases_only_main_after_verification(self):
        validate_release(self.text)

    def test_rejects_privileged_triggers(self):
        for trigger in ('pull_request_target', 'workflow_run'):
            with self.subTest(trigger=trigger), self.assertRaises(AssertionError):
                validate_release(self.text.replace('  workflow_dispatch:', '  {}:\n  workflow_dispatch:'.format(trigger), 1))

    def test_rejects_skipping_verification_or_unpinned_engine(self):
        for old, new in (('    needs: verify\n', ''),
                         ("github.ref == 'refs/heads/main' && ", ''),
                         ("github.event_name == 'push'", "github.event_name != 'workflow_dispatch'"),
                         ('server-release.yml@' + RT_SHA, 'server-release.yml@main'),
                         ('tools_ref: ' + RT_SHA, 'tools_ref: main'),
                         ('    uses: ./.github/workflows/verify.yml',
                          '    uses: ./.github/workflows/verify.yml\n    if: false')):
            with self.subTest(old=old), self.assertRaises((AssertionError, KeyError)):
                validate_release(self.text.replace(old, new))


class ReleaseManifest(unittest.TestCase):
    def setUp(self):
        self.manifest = json.loads((ROOT / '.github/release/server.json').read_text())

    def test_manifest_policy(self):
        validate_manifest(self.manifest)

    def test_rejects_dropping_db_cutover_overlay(self):
        manifest = json.loads(json.dumps(self.manifest))
        manifest['legacy_overrides'].append(r'db-runtime-spending-[0-9a-f]{16}\.json')
        with self.assertRaises(AssertionError):
            validate_manifest(manifest)


class ImageMetadata(unittest.TestCase):
    def test_baked_version_reaches_the_image(self):
        ignored = (ROOT / 'webapp/.dockerignore').read_text().split()
        self.assertNotIn('version.json', ignored)


if __name__ == '__main__':
    unittest.main()

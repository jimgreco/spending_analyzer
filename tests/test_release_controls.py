"""Production publication must never expose credentials or implicitly activate."""
from pathlib import Path
import re
import unittest
import yaml

ROOT = Path(__file__).resolve().parents[1]


def validate(text):
    workflow = yaml.load(text, Loader=yaml.BaseLoader)
    assert set(workflow['on']) == {'push', 'pull_request', 'workflow_dispatch'}
    assert workflow['on']['push']['branches'] == ['main']
    assert workflow['permissions'] == {'contents': 'read'}
    assert not re.search(r'\bsecrets\s*\.', text)
    assert not re.search(r'\b(?:ssh|scp|rsync|prune)\b', text, re.I)
    assert not re.search(r'\b(?:docker-compose|compose\s+(?:up|down|restart))\b', text)
    assert not re.search(r'\b(?:workflow_run|pull_request_target)\b', text)
    assert not re.search(r'\b(?:docker\s+(?:push|login)|gh\s+workflow\s+run)\b', text)
    assert workflow['jobs']['required']['needs'] == ['tests', 'image']
    assert workflow['jobs']['image']['runs-on'] == 'ubuntu-24.04-arm'
    assert '--platform linux/arm64' in workflow['jobs']['image']['steps'][-1]['run']
    for job in workflow['jobs'].values():
        assert 'environment' not in job
        assert 'permissions' not in job


class PublicationControls(unittest.TestCase):
    def setUp(self):
        source = (ROOT / '.github/workflows/ci.yml').read_text()
        self.text = '\n'.join(line for line in source.splitlines() if not line.lstrip().startswith('#'))

    def test_validation_only_workflow(self):
        validate(self.text)

    def test_rejects_remote_deploy_or_prune_reintroduction(self):
        for command in ('ssh host command', 'docker system prune -f', 'rsync source host:dest',
                        'docker-compose up -d spending', 'docker push example', '${{ secrets.EC2_SSH_KEY }}'):
            with self.subTest(command=command), self.assertRaises(AssertionError):
                validate(self.text.replace('test "$TESTS_RESULT"', command + ' && test "$TESTS_RESULT"'))

    def test_rejects_privileged_pull_request_event(self):
        with self.assertRaises(AssertionError):
            validate(self.text.replace('  pull_request:', '  pull_request_target:'))

    def test_old_workflow_is_harmless_even_if_reenabled(self):
        text = (ROOT / '.github/workflows/deploy.yml').read_text()
        workflow = yaml.load(text, Loader=yaml.BaseLoader)
        self.assertEqual(set(workflow['on']), {'workflow_dispatch'})
        self.assertEqual(set(workflow['jobs']), {'retired'})
        self.assertEqual(workflow['permissions'], {'contents': 'read'})
        steps = workflow['jobs']['retired']['steps']
        self.assertEqual(len(steps), 1)
        self.assertIn('exit 1', steps[0]['run'])
        self.assertNotRegex(text, r'\b(?:secrets\s*\.|ssh|rsync|prune|docker-compose)\b')


if __name__ == '__main__':
    unittest.main()

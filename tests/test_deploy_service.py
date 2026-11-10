"""Pure-logic guards of scripts/deploy-service.py (no Docker, no host access)."""
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('spending_deploy', ROOT / 'scripts' / 'deploy-service.py')
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)

SECRET_URL = 'postgresql://spending_runtime:synthetic-placeholder@db:5432/spending'


class EnvironmentDrift(unittest.TestCase):
    def setUp(self):
        self.defaults = {'PATH': '/usr/local/bin', 'PYTHON_VERSION': '3.11.9'}
        self.configured = {'DATABASE_URL': SECRET_URL, 'SECRET_KEY': 'synthetic-session-key', 'PORT': '8000'}
        self.live = dict(self.defaults, **self.configured)

    def test_identical_environment_passes_silently(self):
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            deploy.validate_environment(self.live, self.configured, self.defaults)
        self.assertEqual(output.getvalue(), '')

    def test_changed_or_removed_values_fail_without_disclosing_values(self):
        for configured in (dict(self.configured, DATABASE_URL=SECRET_URL.replace('runtime', 'admin')),
                           {k: v for k, v in self.configured.items() if k != 'SECRET_KEY'},
                           dict(self.configured, EXTRA='1')):
            with self.subTest(keys=sorted(configured)), self.assertRaises(deploy.ReleaseError) as error:
                deploy.validate_environment(self.live, configured, self.defaults)
            self.assertNotIn('synthetic', str(error.exception))
            self.assertIn('reconcile', str(error.exception))

    def test_live_override_of_image_default_cannot_disappear(self):
        live = dict(self.live, PATH='/custom/bin')
        with self.assertRaises(deploy.ReleaseError):
            deploy.validate_environment(live, self.configured, self.defaults)

    def test_requires_postgres_url_and_no_migration_url(self):
        for configured in (dict(self.configured, DATABASE_URL='sqlite:///x'),
                           {k: v for k, v in self.configured.items() if k != 'DATABASE_URL'},
                           dict(self.configured, MIGRATION_DATABASE_URL=SECRET_URL)):
            live = dict(self.defaults, **configured)
            with self.subTest(keys=sorted(configured)), self.assertRaises(deploy.ReleaseError):
                deploy.validate_environment(live, configured, self.defaults)

    def test_configured_environment_normalises_list_and_null(self):
        self.assertEqual(deploy.configured_environment({'environment': ['A=1', 'B=x=y']}), {'A': '1', 'B': 'x=y'})
        self.assertEqual(deploy.configured_environment({'environment': {'A': 1, 'B': None}}), {'A': '1', 'B': None})


class Labels(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name).resolve()
        self.base = self.directory / 'docker-compose.yml'
        self.base.write_text('services: {}\n')

    def tearDown(self):
        self.tmp.cleanup()

    def container(self, **labels):
        values = {'com.docker.compose.project': 'deploy', 'com.docker.compose.service': 'spending',
                  'com.docker.compose.project.working_dir': str(self.directory),
                  'com.docker.compose.project.config_files': str(self.base)}
        values.update(labels)
        return {'Config': {'Labels': {k: v for k, v in values.items() if v is not None}}}

    def test_command_comes_from_recorded_labels(self):
        overlay = self.directory / 'db-runtime-spending-0123456789abcdef.json'
        overlay.write_text('{}')
        command, paths = deploy.compose_for(self.container(**{
            'com.docker.compose.project.config_files': '{},{}'.format(self.base, overlay)}), self.directory)
        self.assertEqual(command, ['docker-compose', '--project-name', 'deploy', '--project-directory',
                                   str(self.directory), '-f', str(self.base), '-f', str(overlay)])
        self.assertEqual(paths, [self.base, overlay])

    def test_rejects_wrong_project_service_or_directory(self):
        for labels in ({'com.docker.compose.project': 'other'}, {'com.docker.compose.service': 'daily'},
                       {'com.docker.compose.service': None},
                       {'com.docker.compose.project.working_dir': '/elsewhere'}):
            with self.subTest(labels=labels), self.assertRaises(deploy.ReleaseError):
                deploy.compose_for(self.container(**labels), self.directory)

    def test_rejects_missing_or_outside_compose_files(self):
        with tempfile.NamedTemporaryFile() as outside:
            for files in ('', str(self.directory / 'missing.yml'), outside.name, '{},'.format(self.base)):
                with self.subTest(files=files), self.assertRaises(deploy.ReleaseError):
                    deploy.compose_for(self.container(**{'com.docker.compose.project.config_files': files}),
                                       self.directory)

    def test_compose_version_must_match_live_label(self):
        live = {'Config': {'Labels': {'com.docker.compose.version': '2.26.1'}}}
        with patch.object(deploy, 'run', return_value='v2.26.1'):
            deploy.validate_compose_version(live)
        with patch.object(deploy, 'run', return_value='2.27.0'), self.assertRaises(deploy.ReleaseError):
            deploy.validate_compose_version(live)
        with patch.object(deploy, 'run', return_value='2.26.1'), self.assertRaises(deploy.ReleaseError):
            deploy.validate_compose_version({'Config': {'Labels': {}}})


class ComposeHash(unittest.TestCase):
    def fixture(self):
        live = {'Config': {'Labels': {'com.docker.compose.config-hash': 'b' * 64,
                                       'com.docker.compose.depends_on': '',
                                       'com.docker.compose.version': '2.26.1'}}}
        compose = ['docker-compose', '--project-name', 'deploy', '--project-directory', '/deploy',
                   '-f', '/deploy/docker-compose.yml']
        config = {'services': {'spending': {'depends_on': {'db': {'condition': 'service_started'}},
                                             'environment': {'DATABASE_URL': SECRET_URL}},
                               'db': {'image': 'postgres:16'}}}
        return live, compose, config

    def test_exact_hash_passes(self):
        live, compose, config = self.fixture()
        with patch.object(deploy, 'run', return_value='spending ' + 'b' * 64) as run:
            deploy.validate_service_hash(live, compose, config)
            run.assert_called_once_with(*compose, 'config', '--hash', 'spending')

    def test_no_deps_hash_accepted_only_after_lossless_roundtrip(self):
        live, compose, config = self.fixture()
        responses = ['spending ' + 'a' * 64, 'spending ' + 'a' * 64, 'spending ' + 'b' * 64]
        with patch.object(deploy, 'run', side_effect=responses) as run:
            deploy.validate_service_hash(live, compose, config)
            second = json.loads(run.call_args_list[2].kwargs['input_text'])
            self.assertNotIn('depends_on', second['services']['spending'])
            self.assertEqual(second['services']['db'], config['services']['db'])
            self.assertNotIn('/deploy/docker-compose.yml', run.call_args_list[2].args)
            self.assertNotIn(SECRET_URL, ' '.join(run.call_args_list[2].args))
            self.assertIn('depends_on', config['services']['spending'])

    def test_unrelated_drift_fails(self):
        live, compose, config = self.fixture()
        for responses in (['spending ' + 'a' * 64, 'spending ' + 'a' * 64, 'spending ' + 'c' * 64],
                          ['spending ' + 'a' * 64, 'spending ' + 'c' * 64]):
            with self.subTest(responses=len(responses)), patch.object(deploy, 'run', side_effect=responses):
                with self.assertRaises(deploy.ReleaseError):
                    deploy.validate_service_hash(live, compose, config)

    def test_no_deps_alternative_requires_known_labels(self):
        for changed in ({'com.docker.compose.depends_on': 'db:service_started:false'},
                        {'com.docker.compose.version': '2.27.0'}):
            live, compose, config = self.fixture()
            live['Config']['Labels'].update(changed)
            with patch.object(deploy, 'run', return_value='spending ' + 'a' * 64) as run:
                with self.assertRaises(deploy.ReleaseError):
                    deploy.validate_service_hash(live, compose, config)
                self.assertEqual(run.call_count, 1)

    def test_service_hash_rejects_malformed_output(self):
        for value in ('', 'spending nothex', 'daily ' + 'a' * 64, 'spending ' + 'a' * 64 + '\ndb ' + 'b' * 64):
            with self.subTest(value=value), self.assertRaises(deploy.ReleaseError):
                deploy.service_hash(value)


class Artifacts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name).resolve()

    def tearDown(self):
        self.tmp.cleanup()

    def test_build_section_required_and_context_must_match(self):
        webapp = self.directory / 'webapp'
        deploy.validate_build_source({'build': {'context': str(webapp)}, 'image': 'sha256:' + 'a' * 64}, webapp)
        for service in ({'image': 'sha256:' + 'a' * 64}, {'build': {'context': '/other'}}, {'build': {}}):
            with self.subTest(service=service), self.assertRaises(deploy.ReleaseError):
                deploy.validate_build_source(service, webapp)

    def test_cutover_window_blocks_recent_db_runtime_overlay(self):
        overlay = self.directory / 'db-runtime-spending-0123456789abcdef.json'
        overlay.write_text('{}')
        other = self.directory / 'db-runtime-daily-0123456789abcdef.json'
        other.write_text('{}')
        deploy.check_cutover_window([other], time.time())
        with self.assertRaises(deploy.ReleaseError):
            deploy.check_cutover_window([overlay], time.time())
        old = time.time() - 25 * 3600
        os.utime(overlay, (old, old))
        deploy.check_cutover_window([overlay], time.time())

    def test_retained_files_drop_only_own_image_only_overrides(self):
        base = self.directory / 'docker-compose.yml'
        cutover = self.directory / 'db-runtime-spending-0123456789abcdef.json'
        own = self.directory / ('spending-image-' + 'a' * 40 + '.json')
        tampered = self.directory / ('spending-image-' + 'b' * 40 + '.json')
        own.write_text(json.dumps({'services': {'spending': {'image': 'sha256:' + 'c' * 64}}}))
        tampered.write_text(json.dumps({'services': {'spending': {'image': 'x', 'environment': {}}}}))
        compose = ['docker-compose', '--project-name', 'deploy', '--project-directory', str(self.directory)]
        command = deploy.retained_files(compose, [base, own, cutover, tampered])
        self.assertEqual(command, compose + ['-f', str(base), '-f', str(cutover), '-f', str(tampered)])

    def test_release_metadata_must_match_commit_and_version(self):
        webapp = self.directory
        (webapp / 'VERSION').write_text('1.3.0\n')
        sha = 'abcdef0' + '1' * 33
        (webapp / 'version.json').write_text(json.dumps({'sha': sha[:7], 'timestamp': 't', 'version': '1.3.0'}))
        self.assertEqual(deploy.release_metadata(webapp, sha)['sha'], sha[:7])
        for data in ({'sha': '0000000', 'timestamp': 't', 'version': '1.3.0'},
                     {'sha': sha[:7], 'timestamp': 't', 'version': '1.2.0'},
                     {'sha': sha[:7], 'version': '1.3.0'}):
            (webapp / 'version.json').write_text(json.dumps(data))
            with self.subTest(data=data), self.assertRaises(deploy.ReleaseError):
                deploy.release_metadata(webapp, sha)

    def test_image_only_override_rejects_any_other_change(self):
        original = {'services': {'spending': {'image': 'old', 'environment': {'DATABASE_URL': SECRET_URL}}}}
        good = {'services': {'spending': {'image': 'new', 'environment': {'DATABASE_URL': SECRET_URL}}}}
        bad = {'services': {'spending': {'image': 'new', 'environment': {'DATABASE_URL': 'other'}}}}
        with patch.object(deploy, 'run', side_effect=[original, good]):
            deploy.validate_image_only_override(['a'], ['b'], 'new')
        with patch.object(deploy, 'run', side_effect=[original, bad]), self.assertRaises(deploy.ReleaseError):
            deploy.validate_image_only_override(['a'], ['b'], 'new')


class Execution(unittest.TestCase):
    def test_failed_command_withholds_output(self):
        with self.assertRaises(deploy.ReleaseError) as error:
            deploy.run('python3', '-c', 'import sys; print("' + SECRET_URL + '"); sys.exit(3)')
        self.assertNotIn(SECRET_URL, str(error.exception))

    def test_preflight_requires_exactly_one_container(self):
        for ids in ('', 'a\nb'):
            with self.subTest(ids=ids), patch.object(deploy, 'run', return_value=ids):
                with self.assertRaises(deploy.ReleaseError):
                    deploy.preflight(Path('/unused'), Path('/unused'))


if __name__ == '__main__':
    unittest.main()

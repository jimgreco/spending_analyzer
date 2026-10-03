"""Synthetic API contract tests for the review page and new-import display names."""
import json
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
import app as spending


class FakeImportDb:
    def __init__(self, *, new_upload_id=7, existing_new=False, old_count=1):
        self.job_result = {'status': 'ok', 'filename': 'original.csv',
                           'file_hash': 'synthetic-hash', 'new_upload_id': new_upload_id}
        self.filename = 'original.csv'
        self.existing_new = existing_new
        self.old_count = old_count
        self.fetchone_result = None
        self.rowcount = 0
        self.updated_transactions = 0

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self

    def execute(self, sql, params):
        self.rowcount = 0
        if 'SELECT pg_advisory_xact_lock' in sql:
            return
        if 'SELECT status, result_json FROM upload_jobs' in sql:
            self.fetchone_result = ('done', json.dumps(self.job_result))
        elif 'SELECT filename FROM uploaded_files WHERE id=' in sql:
            upload_id, owner, file_hash = params
            self.fetchone_result = (self.filename,) if (upload_id, owner, file_hash) == (7, 1, 'synthetic-hash') else None
        elif 'SELECT COUNT(*) FROM uploaded_files' in sql:
            self.fetchone_result = (self.old_count,)
        elif 'SELECT 1 FROM uploaded_files' in sql:
            self.fetchone_result = (1,) if self.existing_new else None
        elif 'UPDATE uploaded_files SET filename=' in sql:
            self.filename = params[0]
            self.rowcount = 1
        elif 'UPDATE transactions SET import_file=' in sql:
            self.updated_transactions += 1
            self.rowcount = 2
        elif 'UPDATE upload_jobs SET result_json=' in sql:
            self.job_result = json.loads(params[0])
            self.rowcount = 1
        else:
            raise AssertionError(f'Unexpected SQL: {sql}')

    def fetchone(self):
        return self.fetchone_result


class UiApiContractTests(unittest.TestCase):
    def setUp(self):
        self.user = {'id': 1, 'role': 'edit', 'is_owner': False}
        spending.app.dependency_overrides[spending.get_current_user] = lambda: self.user
        self.client = TestClient(spending.app)

    def tearDown(self):
        self.client.close()
        spending.app.dependency_overrides.clear()

    def request_name(self, name='Chase Checking 2026-09.csv'):
        return self.client.patch('/api/uploads/display-name', json={'job_id': 'job-1', 'new_name': name})

    def test_transaction_pagination_rejects_unbounded_values(self):
        for query in ('page=0', 'per_page=0', 'per_page=101', 'page=1000001'):
            self.assertEqual(self.client.get('/api/transactions?' + query).status_code, 422)

    def test_new_import_name_is_idempotent_and_does_not_change_account(self):
        fake = FakeImportDb()
        with patch.object(spending, 'db', return_value=fake):
            self.assertEqual(self.request_name().status_code, 200)
            self.assertEqual(fake.filename, 'Chase Checking 2026-09.csv')
            self.assertEqual(fake.updated_transactions, 1)
            self.assertEqual(self.request_name().status_code, 200)
            self.assertEqual(fake.updated_transactions, 1)
            self.assertEqual(self.request_name('Different.csv').status_code, 409)

    def test_old_or_ambiguous_import_cannot_be_named(self):
        for fake in (FakeImportDb(new_upload_id=None), FakeImportDb(existing_new=True), FakeImportDb(old_count=2)):
            with self.subTest(fake=fake), patch.object(spending, 'db', return_value=fake):
                self.assertEqual(self.request_name().status_code, 409)
                self.assertEqual(fake.filename, 'original.csv')

    def test_job_cannot_name_another_upload_or_change_extension(self):
        fake = FakeImportDb(new_upload_id=8)
        with patch.object(spending, 'db', return_value=fake):
            self.assertEqual(self.request_name().status_code, 404)
            self.assertEqual(fake.filename, 'original.csv')
        fake = FakeImportDb()
        with patch.object(spending, 'db', return_value=fake):
            self.assertEqual(self.request_name('Statement.pdf').status_code, 400)
            self.assertEqual(fake.filename, 'original.csv')

    def test_viewer_cannot_name_import(self):
        self.user['role'] = 'read'
        with patch.object(spending, 'db', side_effect=AssertionError('DB should not be called')):
            self.assertEqual(self.request_name().status_code, 403)


if __name__ == '__main__':
    unittest.main()

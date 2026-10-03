"""Database-free checks for rules-manager validation and stale actions."""
from contextlib import contextmanager
import unittest
from unittest.mock import patch

from fastapi import HTTPException
from fastapi.testclient import TestClient
from pydantic import ValidationError

from test_tag_model import app


class Cursor:
    def __init__(self, rows=(), one=None):
        self.rows = rows
        self.one = one
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        pass

    def execute(self, sql, params):
        self.statements.append((sql, params))

    def fetchone(self):
        return self.one

    def fetchall(self):
        return self.rows


class Connection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self, **_):
        return self._cursor


def fake_db(cursor):
    @contextmanager
    def use():
        yield Connection(cursor)
    return use


class RulesManagerUnitTests(unittest.TestCase):
    def test_stale_and_archived_manager_edits_stop_before_any_write(self):
        for revision, archived in ((2, False), (1, True)):
            with self.subTest(revision=revision, archived=archived):
                # id, primary_id, revision, manually_corrected, archived, status
                cursor = Cursor(one=(7, 11, revision, True, archived, 'active'))
                body = app.PrimaryTagUpdate(primary_tag='Home', expected_correction_revision=1)
                with patch.object(app, 'db', fake_db(cursor)):
                    with self.assertRaises(HTTPException) as error:
                        app.set_primary_tag(7, body, user={'id': 3})
                self.assertEqual(error.exception.status_code, 409)
                self.assertEqual(len(cursor.statements), 1)
                self.assertIn('FOR UPDATE', cursor.statements[0][0])
                self.assertEqual(cursor.statements[0][1], (7, 3))

    def test_manager_revision_validation(self):
        with self.assertRaises(ValidationError):
            app.PrimaryTagUpdate(primary_tag='Home', expected_correction_revision=-1)
        self.assertEqual(app.PrimaryTagUpdate(primary_tag='Home').expected_correction_revision, None)

    def test_archive_restore_sql_guards_owner_state_and_revision(self):
        for action in (app.archive_categorization_correction, app.restore_categorization_correction):
            with self.subTest(action=action.__name__):
                cursor = Cursor(one=(7,))
                with patch.object(app, 'db', fake_db(cursor)):
                    self.assertEqual(action(7, expected_revision=4, user={'id': 3}), {'ok': True, 'id': 7})
                sql, params = cursor.statements[0]
                self.assertIn('user_id=%s', sql)
                self.assertIn("status='active'", sql)
                self.assertIn('manually_corrected=TRUE', sql)
                self.assertIn('correction_revision=%s', sql)
                self.assertIn('correction_revision=correction_revision+1', sql)
                self.assertEqual(params, (7, 3, 4))

    def test_search_treats_sql_wildcards_as_literal_characters(self):
        cursor = Cursor(one={'reusable': 0, 'one_time': 0, 'archived': 0})
        with patch.object(app, 'db', fake_db(cursor)):
            result = app.list_categorization_corrections(
                kind='reusable', search='100%_!', limit=50, offset=0, user={'id': 3})
        self.assertEqual(result['total'], 0)
        self.assertEqual(len(cursor.statements), 2)
        for sql, params in cursor.statements:
            self.assertIn("ILIKE %s ESCAPE '!'", sql)
            self.assertEqual(params[:5], [3] + ['%100!%!_!!%'] * 4)

    def test_viewer_mutations_rejected_without_database_access(self):
        def viewer():
            return {'id': 3, 'role': 'read', 'is_owner': False}
        app.app.dependency_overrides[app.get_current_user] = viewer
        try:
            with patch.object(app, 'db', side_effect=AssertionError('database must not be used')):
                client = TestClient(app.app)
                try:
                    self.assertEqual(client.delete('/api/categorization-corrections/7?expected_revision=0').status_code, 403)
                    self.assertEqual(client.post('/api/categorization-corrections/7/restore?expected_revision=0').status_code, 403)
                    self.assertEqual(client.put('/api/transactions/7/primary-tag', json={
                        'primary_tag': 'Home', 'expected_correction_revision': 0}).status_code, 403)
                finally:
                    client.close()
        finally:
            app.app.dependency_overrides.clear()

    def test_api_query_validation_and_anonymous_access(self):
        client = TestClient(app.app)
        try:
            self.assertEqual(client.get('/api/categorization-corrections').status_code, 401)
            app.app.dependency_overrides[app.get_current_user] = lambda: {
                'id': 3, 'role': 'edit', 'is_owner': False}
            with patch.object(app, 'db', side_effect=AssertionError('invalid input must not query')):
                self.assertEqual(client.get('/api/categorization-corrections?kind=invalid').status_code, 422)
                self.assertEqual(client.get('/api/categorization-corrections?limit=101').status_code, 422)
                self.assertEqual(client.get('/api/categorization-corrections?offset=-1').status_code, 422)
                self.assertEqual(client.delete('/api/categorization-corrections/7?expected_revision=-1').status_code, 422)
        finally:
            app.app.dependency_overrides.clear()
            client.close()


if __name__ == '__main__':
    unittest.main()

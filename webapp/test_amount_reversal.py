"""Synthetic sign correction checks on an explicitly disposable local *_test DB."""

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from uuid import uuid4

import psycopg2.extensions
from fastapi import HTTPException
from fastapi.testclient import TestClient
from test_tag_model import app


TEST_DSN = os.getenv('SPENDING_TEST_DATABASE_URL', '')


@unittest.skipUnless(TEST_DSN, 'Set SPENDING_TEST_DATABASE_URL to a disposable local test DB')
class AmountReversalTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = psycopg2.extensions.parse_dsn(TEST_DSN)
        host = config.get('host', '')
        if not config.get('dbname', '').endswith('_test') or not (
            host in ('localhost', '127.0.0.1') or host.startswith('/tmp/')
        ):
            raise RuntimeError('Integration tests require an explicitly named local _test database')
        cls.patches = [patch.object(app, 'DATABASE_URL', TEST_DSN),
                       patch.object(app, '_pool', None), patch.object(app, 'LOCAL_DEV', False)]
        for item in cls.patches:
            item.start()
        app.init_db()
        app.init_db()
        cls.client = TestClient(app.app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        app.app.dependency_overrides.clear()
        if app._pool:
            app._pool.closeall()
        for item in reversed(cls.patches):
            item.stop()

    def setUp(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE users, upload_jobs RESTART IDENTITY CASCADE')
                cur.execute("""INSERT INTO users(email,name) VALUES
                    ('sign-owner@example.test','Owner'),
                    ('sign-editor@example.test','Editor'),
                    ('sign-other@example.test','Other') RETURNING id""")
                self.uid, self.editor_uid, self.other_uid = [row[0] for row in cur.fetchall()]
                cur.execute("INSERT INTO tags(user_id,name) VALUES(%s,'Purpose') RETURNING id",
                            (self.uid,))
                self.tag_id = cur.fetchone()[0]
        self.user = {'id': self.uid, 'role': 'owner', 'is_owner': True,
                     'email': 'sign-owner@example.test'}
        app.app.dependency_overrides[app.get_current_user] = lambda: self.user

    def insert(self, amount='25.00', *, owner=None, status='active', kind='expense',
               tag=True, key=None, archived=False):
        key = key or f'synthetic-{uuid4()}'
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO transactions
                    (user_id,date,description,amount,source,dedup_key,status,
                     transaction_type,primary_tag_id,correction_archived)
                    VALUES(%s,'2026-09-01','SYNTHETIC SIGN',%s,'Example',%s,%s,%s,%s,%s)
                    RETURNING id""", (owner or self.uid, amount, key, status, kind,
                                        self.tag_id if tag else None, archived))
                return cur.fetchone()[0]

    def preview(self, tx_id):
        response = self.client.get(f'/api/transactions/{tx_id}/amount')
        self.assertEqual(response.status_code, 200, response.text)
        return response.json()

    def change(self, tx_id, preview, action='reverse', change_id=None, operation_id=None):
        body = {'operation_id': str(operation_id or uuid4()),
                'expected_revision': preview['amount_revision'],
                'expected_amount': preview['amount']}
        if change_id:
            body['change_id'] = change_id
        return self.client.post(f'/api/transactions/{tx_id}/amount/{action}', json=body), body

    def test_preview_reverse_undo_provenance_analytics_and_type_warning(self):
        tx_id = self.insert(archived=True)
        before = self.preview(tx_id)
        self.assertEqual((before['amount'], before['original_amount'], before['reverse_preview']),
                         ('25.00', '25.00', '-25.00'))
        self.assertEqual(before['history'], [])
        response, _ = self.change(tx_id, before)
        self.assertEqual(response.status_code, 200, response.text)
        reversal = response.json()
        self.assertEqual((reversal['before_amount'], reversal['after_amount'],
                          reversal['original_amount'], reversal['type_sign_issue']),
                         ('25.00', '-25.00', '25.00', 'expense_credit'))
        corrected = self.preview(tx_id)
        self.assertTrue(corrected['can_undo'])
        self.assertEqual((corrected['amount'], corrected['original_amount'],
                          corrected['amount_revision'], corrected['history'][0]['id']),
                         ('-25.00', '25.00', 1, reversal['id']))
        self.assertEqual(corrected['history'][0]['actor_email'], 'sign-owner@example.test')
        listing = self.client.get('/api/transactions').json()['transactions'][0]
        self.assertEqual((listing['amount'], listing['original_amount'],
                          listing['type_sign_issue'], listing['transaction_type']),
                         (-25.0, 25.0, 'expense_credit', 'expense'))
        self.assertEqual(self.client.get('/api/stats').json()['total'], -25.0)
        self.assertEqual(self.client.get('/api/analytics').json()['ambiguous_sign_count'], 1)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT dedup_key,primary_tag_id,correction_archived FROM transactions WHERE id=%s',
                            (tx_id,))
                self.assertEqual(cur.fetchone()[1:], (self.tag_id, True))
        undo, _ = self.change(tx_id, corrected, 'undo', reversal['id'])
        self.assertEqual(undo.status_code, 200, undo.text)
        self.assertEqual((undo.json()['before_amount'], undo.json()['after_amount'],
                          undo.json()['undo_of']), ('-25.00', '25.00', reversal['id']))
        restored = self.preview(tx_id)
        self.assertFalse(restored['can_undo'])
        self.assertEqual((restored['amount'], restored['amount_revision'], len(restored['history'])),
                         ('25.00', 2, 2))
        self.assertEqual(self.client.get('/api/stats').json()['total'], 25.0)

    def test_response_retry_double_click_and_stale_conflict(self):
        tx_id = self.insert()
        first = self.preview(tx_id)
        response, body = self.change(tx_id, first)
        self.assertEqual(response.status_code, 200, response.text)
        again = self.client.post(f'/api/transactions/{tx_id}/amount/reverse', json=body)
        self.assertEqual(again.status_code, 200, again.text)
        self.assertTrue(again.json()['replayed'])
        self.assertEqual(again.json()['id'], response.json()['id'])
        stale, _ = self.change(tx_id, first)
        self.assertEqual(stale.status_code, 409)
        body['expected_amount'] = '-25.00'
        self.assertEqual(self.client.post(f'/api/transactions/{tx_id}/amount/reverse',
                                          json=body).status_code, 409)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM transaction_amount_changes WHERE transaction_id=%s',
                            (tx_id,))
                self.assertEqual(cur.fetchone()[0], 1)
        corrected = self.preview(tx_id)
        self.assertEqual(self.change(tx_id, corrected, 'undo', response.json()['id'])[0].status_code, 200)
        after_undo = self.client.post(f'/api/transactions/{tx_id}/amount/reverse',
                                      json={**body, 'expected_amount': '25.00'})
        self.assertEqual(after_undo.status_code, 200)
        self.assertTrue(after_undo.json()['replayed'])
        self.assertEqual(self.preview(tx_id)['amount'], '25.00')

    def test_undo_requires_exact_last_reversal_and_retries_are_safe(self):
        tx_id = self.insert()
        first, _ = self.change(tx_id, self.preview(tx_id))
        after_first = self.preview(tx_id)
        second, _ = self.change(tx_id, after_first)
        after_second = self.preview(tx_id)
        stale, _ = self.change(tx_id, after_second, 'undo', first.json()['id'])
        self.assertEqual(stale.status_code, 409)
        undo, body = self.change(tx_id, after_second, 'undo', second.json()['id'])
        self.assertEqual(undo.status_code, 200, undo.text)
        retry = self.client.post(f'/api/transactions/{tx_id}/amount/undo', json=body)
        self.assertEqual(retry.status_code, 200, retry.text)
        self.assertTrue(retry.json()['replayed'])
        self.assertEqual(self.preview(tx_id)['amount'], '-25.00')
        # A later type edit remains intact when an exact amount reversal is undone.
        self.assertEqual(self.client.put(f'/api/transactions/{tx_id}/type', json={
            'transaction_type': 'refund', 'expected_revision': 0}).status_code, 200)
        self.assertEqual(self.preview(tx_id)['transaction_type'], 'refund')

    def test_permissions_zero_and_inactive_status(self):
        own = self.insert()
        foreign = self.insert(owner=self.other_uid, tag=False)
        zero = self.insert('0.00')
        deleted = self.insert(status='deleted')
        deduped = self.insert(status='deduped')
        self.user['role'] = 'read'
        self.assertEqual(self.preview(own)['amount'], '25.00')
        forbidden, _ = self.change(own, self.preview(own))
        self.assertEqual(forbidden.status_code, 403)
        self.user.update(role='edit', auth_id=self.editor_uid)
        self.assertEqual(self.client.get(f'/api/transactions/{foreign}/amount').status_code, 404)
        self.assertEqual(self.change(foreign, {'amount':'25.00','amount_revision':0})[0].status_code, 404)
        for tx_id, expected in ((zero, 422), (deleted, 409), (deduped, 409)):
            self.assertEqual(self.change(tx_id, self.preview(tx_id))[0].status_code, expected)
        edited, _ = self.change(own, self.preview(own))
        self.assertEqual(edited.status_code, 200, edited.text)
        self.assertEqual(self.client.delete(f'/api/transactions/{own}').status_code, 200)
        self.assertEqual(self.change(own, self.preview(own), 'undo', edited.json()['id'])[0].status_code, 409)
        self.assertEqual(self.client.post(f'/api/transactions/{own}/restore').status_code, 200)
        self.assertEqual(self.change(own, self.preview(own), 'undo', edited.json()['id'])[0].status_code, 200)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT actor_user_id FROM transaction_amount_changes WHERE id=%s',
                            (edited.json()['id'],))
                self.assertEqual(cur.fetchone()[0], self.editor_uid)

    def test_undo_preserves_later_type_and_category_edits(self):
        tx_id = self.insert()
        reverse, _ = self.change(tx_id, self.preview(tx_id))
        self.assertEqual(reverse.status_code, 200)
        self.assertEqual(self.client.put(f'/api/transactions/{tx_id}/type', json={
            'transaction_type': 'refund', 'expected_revision': 0}).status_code, 200)
        self.assertEqual(self.client.put(f'/api/transactions/{tx_id}/primary-tag', json={
            'primary_tag': 'Purpose', 'correction_scope': 'transaction'}).status_code, 200)
        undo, _ = self.change(tx_id, self.preview(tx_id), 'undo', reverse.json()['id'])
        self.assertEqual(undo.status_code, 200, undo.text)
        row = self.client.get('/api/transactions').json()['transactions'][0]
        self.assertEqual((row['amount'], row['transaction_type'], row['primary_tag']),
                         (25.0, 'refund', 'Purpose'))

    def test_forced_reimport_preserves_sign_and_single_raw_row(self):
        rows = [{'date':'2026-09-01', 'description':'SYNTHETIC SIGN',
                 'amount':30.0, 'source':'Example', 'dedup_key':'raw-30'}]
        with patch.object(app, 'parse_file_bytes', return_value=(rows, 'Example', None)), \
             patch.object(app, 'assign_tags_with_gpt',
                          return_value=[app.review('Choose a category.')]):
            for job_id, force in (('sign-first', False), ('sign-force', True)):
                with app.db() as conn:
                    with conn.cursor() as cur:
                        cur.execute('INSERT INTO upload_jobs(id,user_id,filename) VALUES(%s,%s,%s)',
                                    (job_id, self.uid, 'sign.csv'))
                if force:
                    tx_id = self.client.get('/api/transactions').json()['transactions'][0]['id']
                    reversal, _ = self.change(tx_id, self.preview(tx_id))
                    self.assertEqual(reversal.status_code, 200, reversal.text)
                app._process_upload_job(job_id, self.uid, 'sign.csv', b'raw file bytes', force)
        result = self.client.get('/api/upload/status/sign-force').json()['result']
        self.assertEqual((result['new'], result['dupes'], result['skipped']), (0, 0, 1))
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT id, amount, original_amount, dedup_key, amount_revision
                               FROM transactions WHERE user_id=%s''', (self.uid,))
                self.assertEqual(cur.fetchall(), [(tx_id, -30, 30, 'raw-30', 1)])

    def test_concurrent_distinct_requests_only_one_applies(self):
        tx_id = self.insert()
        first = self.preview(tx_id)
        user = dict(self.user)
        def attempt():
            body = app.AmountChangeRequest(operation_id=uuid4(),
                expected_revision=first['amount_revision'], expected_amount=first['amount'])
            try:
                return app.reverse_transaction_amount(tx_id, body, user)['after_amount']
            except HTTPException as exc:
                return exc.status_code
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: attempt(), range(2)))
        self.assertCountEqual(results, ['-25.00', 409])


if __name__ == '__main__':
    unittest.main()

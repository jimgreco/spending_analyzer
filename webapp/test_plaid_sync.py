"""Bank sync checks. Synthetic data only: Plaid and Claude are never called.

The database half needs SPENDING_TEST_DATABASE_URL (a disposable local _test DB).
"""
import os
import unittest
from datetime import date
from unittest.mock import patch

import psycopg2.extensions
from cryptography.fernet import Fernet
from fastapi.testclient import TestClient

import plaid_client
from categorization import review
from test_tag_model import app

TEST_DSN = os.getenv('SPENDING_TEST_DATABASE_URL', '')
PLAID_ENV = {'PLAID_CLIENT_ID': 'synthetic-client', 'PLAID_SECRET': 'synthetic-secret',
             'PLAID_TOKEN_KEY': Fernet.generate_key().decode(), 'PLAID_ENV': 'sandbox'}


def plaid_tx(tx_id, amount=12.5, day='2026-08-10', name='EXAMPLE GROCERY', account='acct-card', **extra):
    return {'transaction_id': tx_id, 'account_id': account, 'amount': amount, 'date': day,
            'authorized_date': None, 'name': name, 'pending': False, **extra}


class PlaidClientTests(unittest.TestCase):
    def test_disabled_until_all_three_settings_exist(self):
        with patch.dict(os.environ, {'PLAID_CLIENT_ID': '', 'PLAID_SECRET': '', 'PLAID_TOKEN_KEY': ''}):
            self.assertFalse(plaid_client.enabled())
        with patch.dict(os.environ, {**PLAID_ENV, 'PLAID_TOKEN_KEY': ''}):
            self.assertFalse(plaid_client.enabled())
        with patch.dict(os.environ, {**PLAID_ENV, 'PLAID_ENV': 'development'}):
            self.assertFalse(plaid_client.enabled())
        with patch.dict(os.environ, PLAID_ENV):
            self.assertTrue(plaid_client.enabled())

    def test_token_is_encrypted_at_rest_and_key_mismatch_is_reported(self):
        with patch.dict(os.environ, PLAID_ENV):
            stored = plaid_client.encrypt_token('access-sandbox-synthetic')
            self.assertNotIn('access-sandbox', stored)
            self.assertEqual(plaid_client.decrypt_token(stored), 'access-sandbox-synthetic')
        with patch.dict(os.environ, {**PLAID_ENV, 'PLAID_TOKEN_KEY': Fernet.generate_key().decode()}):
            with self.assertRaises(plaid_client.PlaidError) as caught:
                plaid_client.decrypt_token(stored)
            self.assertEqual(caught.exception.code, 'TOKEN_KEY_MISMATCH')

    def test_row_mapping_keeps_plaid_sign_and_skips_unusable_rows(self):
        row = plaid_client.to_row(plaid_tx('t1', amount=42.129, authorized_date='2026-08-08'))
        self.assertEqual(row, {'date': '2026-08-08', 'description': 'EXAMPLE GROCERY', 'amount': 42.13,
                               'dedup_key': 'plaid:t1', 'account_id': 'acct-card'})
        self.assertEqual(plaid_client.to_row(plaid_tx('t2', amount=-20))['amount'], -20)   # refund stays negative
        self.assertEqual(plaid_client.to_row(plaid_tx('t3'))['date'], '2026-08-10')        # falls back to posted date
        self.assertIsNone(plaid_client.to_row(plaid_tx('t4', pending=True)))
        self.assertIsNone(plaid_client.to_row(plaid_tx('t5', amount=0)))
        self.assertIsNone(plaid_client.to_row(plaid_tx('t6', name='')))
        self.assertIsNone(plaid_client.to_row(plaid_tx('t7', day='not-a-date')))
        self.assertIsNone(plaid_client.to_row({'transaction_id': 't8'}))

    def test_default_start_is_the_day_after_the_last_statement_row(self):
        self.assertEqual(plaid_client.default_sync_from('2026-08-31'), date(2026, 9, 1))
        self.assertEqual(plaid_client.default_sync_from(None), date.today())

    def test_matching_pairs_the_same_charge_one_to_one(self):
        def row(key, day, amount=12.5, text='EXAMPLE GROCERY #12', source='Example card'):
            return {'dedup_key': key, 'date': day, 'amount': amount, 'description': text, 'source': source}
        match = plaid_client.match_rows
        self.assertEqual(match([row('b', '2026-08-10')], [row('s', '2026-08-10', text='UNRELATED NAME')]), ({'b': 's'}, set()))
        self.assertEqual(match([row('b', '2026-08-12', text='Example Grocery')], [row('s', '2026-08-10')]), ({'b': 's'}, set()))
        # Two equal charges on one day pair off separately; a third stays new.
        matches, possible = match([row('b1', '2026-08-10'), row('b2', '2026-08-10'), row('b3', '2026-08-10')],
                                  [row('s1', '2026-08-10'), row('s2', '2026-08-10')])
        self.assertEqual((sorted(matches.values()), len(matches), possible), (['s1', 's2'], 2, set()))
        # The nearer date wins, and the later charge is not swallowed by the same statement row.
        self.assertEqual(match([row('later', '2026-08-11'), row('same', '2026-08-10')], [row('s', '2026-08-10')]),
                         ({'same': 's'}, set()))
        # Different day and nothing in common: imported, but flagged.
        self.assertEqual(match([row('b', '2026-08-12', text='COFFEE BAR')], [row('s', '2026-08-10')]), ({}, {'b'}))
        for other in (row('s', '2026-08-10', source='Other card'), row('s', '2026-08-10', amount=12.51),
                      row('s', '2026-08-14')):
            self.assertEqual(match([row('b', '2026-08-10')], [other]), ({}, set()))
        self.assertEqual(match([row('b', '2026-08-11', text='DEBIT CARD PURCHASE 1234')],
                               [row('s', '2026-08-10', text='DEBIT CARD PURCHASE 9999')]), ({}, {'b'}))


    def test_sync_restarts_from_the_original_cursor_when_plaid_mutates_mid_page(self):
        calls = []
        def post(path, body, timeout=30):
            calls.append(body.get('cursor'))
            if len(calls) == 2:
                raise plaid_client.PlaidError('TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION')
            last = len(calls) in (4,)
            return {'added': [plaid_tx(f't{len(calls)}')], 'modified': [], 'removed': [{'transaction_id': 'gone'}] if last else [],
                    'next_cursor': f'c{len(calls)}', 'has_more': not last}
        with patch.object(plaid_client, 'post', post):
            added, modified, removed, cursor = plaid_client.sync_transactions('token', 'start')
        self.assertEqual(calls, ['start', 'c1', 'start', 'c3'])
        self.assertEqual([t['transaction_id'] for t in added], ['t3', 't4'])
        self.assertEqual((modified, removed, cursor), ([], ['gone'], 'c4'))

    def test_errors_carry_plaid_code_and_never_the_request(self):
        class Response:
            status_code = 400
            def json(self): return {'error_code': 'ITEM_LOGIN_REQUIRED', 'display_message': None}
        with patch.dict(os.environ, PLAID_ENV), patch.object(plaid_client.httpx, 'post', return_value=Response()) as sent:
            with self.assertRaises(plaid_client.PlaidError) as caught:
                plaid_client.post('/transactions/sync', {'access_token': 'access-synthetic'})
        self.assertEqual(caught.exception.code, 'ITEM_LOGIN_REQUIRED')
        self.assertNotIn('synthetic', str(caught.exception))
        self.assertEqual(sent.call_args.args[0], 'https://sandbox.plaid.com/transactions/sync')


@unittest.skipUnless(TEST_DSN, 'Set SPENDING_TEST_DATABASE_URL to a disposable local test DB')
class PlaidSyncApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = psycopg2.extensions.parse_dsn(TEST_DSN)
        host = config.get('host', '')
        if not config.get('dbname', '').endswith('_test') or not (host in ('localhost', '127.0.0.1') or host.startswith('/tmp/')):
            raise RuntimeError('Integration tests require an explicitly named local _test database')
        cls.patches = [patch.object(app, 'DATABASE_URL', TEST_DSN), patch.object(app, '_pool', None),
                       patch.object(app, 'LOCAL_DEV', False), patch.dict(os.environ, PLAID_ENV)]
        for p in cls.patches: p.start()
        app.init_db()
        app.init_db()  # upgrades must be idempotent
        cls.client = TestClient(app.app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        app.app.dependency_overrides.clear()
        if app._pool: app._pool.closeall()
        for p in reversed(cls.patches): p.stop()

    def setUp(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE users, upload_jobs RESTART IDENTITY CASCADE')
                cur.execute("INSERT INTO users(email,name) VALUES('owner@example.test','Test owner'),('other@example.test','Other owner') RETURNING id")
                self.uid, self.other = [r[0] for r in cur.fetchall()]
                cur.execute("INSERT INTO tags(user_id,name) VALUES(%s,'Groceries')", (self.uid,))
                # A statement row for the card already exists through 31 Aug.
                cur.execute("""INSERT INTO transactions(user_id,date,description,amount,source,dedup_key,primary_migration_status)
                    VALUES(%s,'2026-08-31','STATEMENT ROW',9,'Example card','statement-key','auto')""", (self.uid,))
        self.user = {'id': self.uid, 'role': 'owner', 'is_owner': True, 'name': 'Test owner', 'email': 'owner@example.test'}
        app.app.dependency_overrides[app.get_current_user] = lambda: self.user
        self.decisions = patch.object(app, 'assign_tags_with_ai', side_effect=lambda rows, *a: [
            dict(primary_tag='Groceries', suggested_tag=None, needs_review=False, confidence='high',
                 reason='Synthetic.', example_ids=[]) if 'GROCERY' in r['description'] else review('Choose a category.')
            for r in rows])
        self.decisions.start()
        self.addCleanup(self.decisions.stop)

    def connect(self):
        accounts = [{'account_id': 'acct-card', 'name': 'Example Visa', 'mask': '1234', 'subtype': 'credit card'},
                    {'account_id': 'acct-savings', 'name': 'Savings', 'mask': '9999', 'subtype': 'savings'}]
        with patch.object(plaid_client, 'exchange_public_token', return_value=('access-synthetic', 'item-synthetic')), \
             patch.object(plaid_client, 'get_accounts', return_value=accounts):
            response = self.client.post('/api/plaid/exchange', json={'public_token': 'public-synthetic', 'institution_name': 'Example card'})
        self.assertEqual(response.status_code, 201)
        return response.json()['item_id']

    def status(self):
        return self.client.get('/api/plaid/status').json()

    def sync(self, item_id, added=(), modified=(), removed=(), cursor='c1'):
        with patch.object(plaid_client, 'sync_transactions',
                          return_value=(list(added), list(modified), list(removed), cursor)) as called, \
             patch.object(app, '_start_plaid_sync') as started:
            self.assertEqual(self.client.post(f'/api/plaid/items/{item_id}/sync').json(), {'started': True})
            started.assert_called_once_with(item_id)
            # The endpoint hands the job to a thread; run it inline for a deterministic result.
            app._run_plaid_sync(item_id)
        return called

    def rows(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT t.dedup_key, t.date::text, t.amount::float, t.source, t.status, t.import_file,
                        t.needs_review, tg.name FROM transactions t LEFT JOIN tags tg ON tg.id=t.primary_tag_id
                    WHERE t.user_id=%s AND left(t.dedup_key,6)='plaid:' ORDER BY t.dedup_key""", (self.uid,))
                return cur.fetchall()

    def enable_card(self, item_id, **changes):
        card = next(a for a in self.status()['items'][0]['accounts'] if a['mask'] == '1234')
        body = {'source': card['source'], 'sync_from': card['sync_from'], 'enabled': True, **changes}
        self.assertEqual(self.client.put(f"/api/plaid/accounts/{card['id']}", json=body).status_code, 200)

    def test_feature_is_absent_without_configuration(self):
        with patch.dict(os.environ, {'PLAID_SECRET': ''}):
            self.assertEqual(self.client.get('/api/plaid/status').json(), {'enabled': False, 'items': []})
            self.assertEqual(self.client.post('/api/plaid/link-token', json={}).status_code, 404)
            self.assertEqual(self.client.post('/api/plaid/exchange', json={'public_token': 'x'}).status_code, 404)
            self.assertEqual(self.client.post('/api/plaid/items/1/sync').status_code, 404)

    def test_connect_stores_encrypted_token_and_accounts_start_off_after_last_statement(self):
        item_id = self.connect()
        status = self.status()
        self.assertTrue(status['enabled'] and status['can_manage'])
        self.assertEqual(status['statement_latest'], {'Example card': '2026-08-31'})
        item = status['items'][0]
        self.assertEqual((item['id'], item['institution_name'], item['status']), (item_id, 'Example card', 'ok'))
        self.assertEqual([(a['name'], a['enabled'], a['sync_from']) for a in item['accounts']],
                         [('Example Visa', False, '2026-09-01'), ('Savings', False, '2026-09-01')])
        self.assertNotIn('access', str(status))
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT access_token FROM plaid_items')
                stored = cur.fetchone()[0]
        self.assertNotIn('access-synthetic', stored)
        self.assertEqual(plaid_client.decrypt_token(stored), 'access-synthetic')

    def test_sync_imports_only_enabled_accounts_from_their_start_date_and_is_repeatable(self):
        item_id = self.connect()
        self.sync(item_id, added=[plaid_tx('early')])
        self.assertEqual(self.rows(), [])                      # nothing switched on yet
        self.assertEqual(self.status()['items'][0]['last_result']['message'], 'No accounts are switched on')

        self.enable_card(item_id)
        batch = [plaid_tx('a', day='2026-08-31'),              # before the start date: left to statements
                 plaid_tx('b', day='2026-09-01', amount=30),
                 plaid_tx('c', day='2026-09-02', amount=-5, name='SP REFUND SHOP'),
                 plaid_tx('d', day='2026-09-02', pending=True),
                 plaid_tx('e', day='2026-09-03', account='acct-savings')]
        called = self.sync(item_id, added=batch)
        self.assertEqual(called.call_args.args, ('access-synthetic', None))   # saving settings reset the cursor
        self.assertEqual(self.rows(), [
            ('plaid:b', '2026-09-01', 30.0, 'Example card', 'active', 'Bank sync: Example card', False, 'Groceries'),
            ('plaid:c', '2026-09-02', -5.0, 'Example card', 'active', 'Bank sync: Example card', True, None)])
        item = self.status()['items'][0]
        self.assertEqual(item['last_result'], {'status': 'ok', 'new': 2, 'matched': 0, 'needs_review': 1, 'updated': 0, 'removed': 0})
        self.assertFalse(item['syncing'])
        self.assertIsNotNone(item['last_synced_at'])

        called = self.sync(item_id, added=batch, cursor='c2')  # a full re-read adds nothing
        self.assertEqual(called.call_args.args, ('access-synthetic', 'c1'))
        self.assertEqual(len(self.rows()), 2)
        self.assertEqual(self.status()['items'][0]['last_result']['new'], 0)

    def test_bank_corrections_and_removals_follow_through_but_user_edits_win(self):
        item_id = self.connect()
        self.enable_card(item_id)
        self.sync(item_id, added=[plaid_tx('b', day='2026-09-01', amount=30), plaid_tx('c', day='2026-09-02', amount=8),
                                  plaid_tx('d', day='2026-09-03', amount=4)])
        with app.db() as conn:
            with conn.cursor() as cur:   # the user corrected one amount and deleted another row
                cur.execute("UPDATE transactions SET amount=7, amount_revision=1 WHERE dedup_key='plaid:c'")
                cur.execute("UPDATE transactions SET status='deleted' WHERE dedup_key='plaid:d'")
        self.sync(item_id, modified=[plaid_tx('b', day='2026-09-01', amount=31), plaid_tx('c', day='2026-09-02', amount=9),
                                     plaid_tx('new-in-modified', day='2026-09-04', amount=2)],
                  added=[plaid_tx('d', day='2026-09-03', amount=4)], removed=['b-pending', 'statement-key'], cursor='c2')
        by_key = {r[0]: r for r in self.rows()}
        self.assertEqual(by_key['plaid:b'][2], 31.0)
        self.assertEqual(by_key['plaid:c'][2], 7.0)
        self.assertEqual(by_key['plaid:d'][4], 'deleted')      # not resurrected
        self.assertIn('plaid:new-in-modified', by_key)
        self.sync(item_id, removed=['b'], cursor='c3')
        self.assertEqual({r[0]: r[4] for r in self.rows()}['plaid:b'], 'deleted')
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT status FROM transactions WHERE dedup_key='statement-key'")
                self.assertEqual(cur.fetchone()[0], 'active')  # only bank-synced rows can be removed by a sync

    def test_login_required_is_recorded_and_cleared_by_the_next_good_sync(self):
        item_id = self.connect()
        self.enable_card(item_id)
        with patch.object(plaid_client, 'sync_transactions', side_effect=plaid_client.PlaidError('ITEM_LOGIN_REQUIRED')):
            app._run_plaid_sync(item_id)
        item = self.status()['items'][0]
        self.assertEqual((item['status'], item['error_code']), ('login_required', 'ITEM_LOGIN_REQUIRED'))
        self.assertIsNone(app._claim_due_plaid_item())         # the scheduler leaves it for the owner to reconnect
        with patch.object(plaid_client, 'sync_transactions', side_effect=RuntimeError('private text')):
            app._run_plaid_sync(item_id)
        item = self.status()['items'][0]
        self.assertEqual((item['status'], item['error_code']), ('error', 'SYNC_FAILED'))
        self.assertNotIn('private text', str(item))
        self.sync(item_id, added=[plaid_tx('b', day='2026-09-01')])
        item = self.status()['items'][0]
        self.assertEqual((item['status'], item['error_code']), ('ok', None))

    def test_one_sync_at_a_time_and_scheduler_claims_only_due_items(self):
        item_id = self.connect()
        self.assertEqual(app._claim_due_plaid_item(), item_id)  # never attempted: due
        self.assertIsNone(app._claim_due_plaid_item())
        with patch.object(app, '_start_plaid_sync') as started:
            self.assertEqual(self.client.post(f'/api/plaid/items/{item_id}/sync').json(), {'started': False})
        started.assert_not_called()
        self.assertTrue(self.status()['items'][0]['syncing'])
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE plaid_items SET last_result='{}', last_attempt_at=NOW() - INTERVAL '13 hours'")
        self.assertEqual(app._claim_due_plaid_item(), item_id)

    def test_permissions_and_owner_scope(self):
        item_id = self.connect()
        account_id = self.status()['items'][0]['accounts'][0]['id']
        body = {'source': 'Example card', 'sync_from': '2026-09-01', 'enabled': True}
        self.user.update(role='edit', is_owner=False)
        self.assertEqual(self.client.post('/api/plaid/link-token', json={}).status_code, 403)
        self.assertEqual(self.client.post('/api/plaid/exchange', json={'public_token': 'x'}).status_code, 403)
        self.assertEqual(self.client.put(f'/api/plaid/accounts/{account_id}', json=body).status_code, 403)
        self.assertEqual(self.client.delete(f'/api/plaid/items/{item_id}').status_code, 403)
        self.assertFalse(self.status()['can_manage'])
        self.user.update(role='read')
        self.assertEqual(self.client.post(f'/api/plaid/items/{item_id}/sync').status_code, 403)
        self.user.update(id=self.other, role='owner', is_owner=True)      # a different owner's ledger
        self.assertEqual(self.status()['items'], [])
        self.assertEqual(self.client.put(f'/api/plaid/accounts/{account_id}', json=body).status_code, 404)
        self.assertEqual(self.client.post(f'/api/plaid/items/{item_id}/sync').status_code, 404)
        self.assertEqual(self.client.post('/api/plaid/link-token', json={'item_id': item_id}).status_code, 404)
        self.assertEqual(self.client.delete(f'/api/plaid/items/{item_id}').status_code, 404)

    def test_reconnect_uses_update_mode_and_disconnect_keeps_transactions(self):
        item_id = self.connect()
        self.enable_card(item_id)
        self.sync(item_id, added=[plaid_tx('b', day='2026-09-01')])
        with patch.object(plaid_client, 'post', return_value={'link_token': 'link-synthetic'}) as sent:
            self.assertEqual(self.client.post('/api/plaid/link-token', json={'item_id': item_id}).json(), {'link_token': 'link-synthetic'})
            update_body = sent.call_args.args[1]
            self.client.post('/api/plaid/link-token', json={})
            new_body = sent.call_args.args[1]
        self.assertEqual(update_body['access_token'], 'access-synthetic')
        self.assertNotIn('products', update_body)
        self.assertEqual((new_body['products'], new_body['transactions']), (['transactions'], {'days_requested': 730}))
        with patch.object(plaid_client, 'remove_item', side_effect=plaid_client.PlaidError('INTERNAL_SERVER_ERROR')):
            self.assertEqual(self.client.delete(f'/api/plaid/items/{item_id}').status_code, 502)
        self.assertEqual(len(self.status()['items']), 1)       # still connected when Plaid could not revoke
        with patch.object(plaid_client, 'remove_item') as removed:
            self.assertEqual(self.client.delete(f'/api/plaid/items/{item_id}').status_code, 200)
        removed.assert_called_once_with('access-synthetic')
        self.assertEqual(self.status()['items'], [])
        self.assertEqual(len(self.rows()), 1)

    def statement(self, key, day, amount, text='EXAMPLE GROCERY', status='active', **extra):
        columns = {'user_id': self.uid, 'date': day, 'description': text, 'amount': amount, 'source': 'Example card',
                   'dedup_key': key, 'status': status, 'primary_migration_status': 'auto', **extra}
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute(f"INSERT INTO transactions({','.join(columns)}) VALUES({','.join(['%s'] * len(columns))})",
                            list(columns.values()))

    def ledger(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT dedup_key, status, dedup_of, needs_review FROM transactions
                    WHERE user_id=%s AND dedup_key <> 'statement-key'""", (self.uid,))
                return {r[0]: r[1:] for r in cur.fetchall()}

    def upload(self, job_id, rows):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('INSERT INTO upload_jobs(id,user_id,filename) VALUES(%s,%s,%s)', (job_id, self.uid, f'{job_id}.csv'))
        with patch.object(app, 'parse_file_bytes', return_value=(rows, 'Example card', None)):
            app._process_upload_job(job_id, self.uid, f'{job_id}.csv', job_id.encode(), False)
        return self.client.get(f'/api/upload/status/{job_id}').json()['result']

    def test_bank_rows_already_on_a_statement_are_stored_as_duplicates(self):
        item_id = self.connect()
        self.enable_card(item_id, sync_from='2026-08-01')
        self.statement('s-same', '2026-08-10', 20)
        self.statement('s-shift', '2026-08-12', 31, text='EXAMPLE GROCERY 44')
        self.statement('s-signed', '2026-08-14', -40, original_amount=40, amount_revision=1)   # re-signed by the user
        self.statement('s-gone', '2026-08-16', 50, status='deleted')
        self.statement('s-weak', '2026-08-18', 60, text='COFFEE BAR')
        self.sync(item_id, added=[plaid_tx('same', day='2026-08-10', amount=20, name='OTHER WORDING'),
                                  plaid_tx('shift', day='2026-08-14', amount=31, name='Example Grocery'),
                                  plaid_tx('signed', day='2026-08-14', amount=40),
                                  plaid_tx('gone', day='2026-08-16', amount=50),
                                  plaid_tx('weak', day='2026-08-20', amount=60),
                                  plaid_tx('new', day='2026-08-21', amount=70)])
        rows = self.ledger()
        for name in ('same', 'shift', 'signed', 'gone'):
            self.assertEqual(rows[f'plaid:{name}'][:2], ('deduped', f's-{name}'))
        self.assertEqual(rows['plaid:weak'], ('active', None, True))      # imported, flagged as a possible duplicate
        self.assertEqual(rows['plaid:new'], ('active', None, False))
        self.assertEqual(rows['s-gone'][0], 'deleted')                     # a deleted charge stays deleted
        self.assertEqual(rows['s-same'][0], 'active')
        result = self.status()['items'][0]['last_result']
        self.assertEqual((result['new'], result['matched'], result['needs_review']), (2, 4, 1))
        # Matched rows were never sent for categorization, and a re-read of history changes nothing.
        self.assertEqual(sorted(r['dedup_key'] for r in app.assign_tags_with_ai.call_args_list[0].args[0]),
                         ['plaid:new', 'plaid:weak'])
        self.enable_card(item_id, sync_from='2026-08-01')
        self.sync(item_id, added=[plaid_tx('same', day='2026-08-10', amount=20), plaid_tx('again', day='2026-08-10', amount=20)])
        self.assertEqual(self.ledger()['plaid:again'][0], 'active')        # s-same is already paired: a second charge is new
        self.assertEqual(len(self.ledger()), len(rows) + 1)

    def test_statement_rows_already_synced_from_the_bank_are_stored_as_duplicates(self):
        item_id = self.connect()
        self.enable_card(item_id)
        self.sync(item_id, added=[plaid_tx('b', day='2026-09-01', amount=30), plaid_tx('c', day='2026-09-02', amount=8),
                                  plaid_tx('w', day='2026-09-05', amount=15, name='COFFEE BAR')])
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE transactions SET status='deleted' WHERE dedup_key='plaid:c'")
        def row(key, day, amount, text='EXAMPLE GROCERY'):
            return {'date': day, 'description': text, 'amount': amount, 'source': 'Example card', 'dedup_key': key}
        result = self.upload('september', [row('u-b', '2026-09-01', 30), row('u-c', '2026-09-03', 8),
                                           row('u-w', '2026-09-07', 15), row('u-new', '2026-09-08', 99)])
        self.assertEqual((result['new'], result['dupes'], result['bank_matched'], result['needs_review']), (2, 2, 2, 1))
        rows = self.ledger()
        self.assertEqual(rows['u-b'][:2], ('deduped', 'plaid:b'))
        self.assertEqual(rows['u-c'][:2], ('deduped', 'plaid:c'))          # the deleted bank row is not replaced
        self.assertEqual(rows['u-w'], ('active', None, True))
        self.assertEqual(rows['u-new'], ('active', None, False))
        self.assertEqual(rows['plaid:b'][0], 'active')
        # The bank later withdraws its row: the statement's copy counts again.
        self.sync(item_id, removed=['b'], cursor='c2')
        rows = self.ledger()
        self.assertEqual((rows['plaid:b'][0], rows['u-b'][:2]), ('deleted', ('active', None)))
        self.assertEqual(rows['u-c'][0], 'deduped')

if __name__ == '__main__':
    unittest.main()

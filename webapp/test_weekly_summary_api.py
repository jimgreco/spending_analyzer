"""GET /api/weekly-summary: token auth, contract shape and weekly-review parity.

Auth/validation tests need no database. The parity tests reproduce weekly-review's
test/fixtures/seed.mjs spending data on a disposable local *_test PostgreSQL DB.
"""
import hashlib
import json
import os
import unittest
from unittest.mock import patch

import psycopg2.extensions
from fastapi.testclient import TestClient
from test_tag_model import app


TEST_DSN = os.getenv('SPENDING_TEST_DATABASE_URL', '')
TOKEN = 'synthetic-weekly-summary-token'
TOKEN_SHA256 = hashlib.sha256(TOKEN.encode()).hexdigest()
EMAIL = 'tester@example.test'
WEEK = '2026-10-26'
AUTH = {'Authorization': f'Bearer {TOKEN}'}


def weekly_env(token_sha=TOKEN_SHA256, email=EMAIL):
    return patch.dict('os.environ', {'WEEKLY_SUMMARY_TOKEN_SHA256': token_sha,
                                     'WEEKLY_SUMMARY_EMAIL': email})


class RoundHalfUpTests(unittest.TestCase):
    def test_matches_javascript_math_round(self):
        cases = [((2.5,), 3), ((-2.5,), -2), ((0.125, 2), 0.13), ((-0.125, 2), -0.12),
                 ((1.005, 2), 1), ((72.25, 2), 72.25), ((372.65 - 178, 2), 194.65),
                 ((-0.004, 2), 0), ((None, 2), None), ((float('nan'), 2), None)]
        for args, expected in cases:
            with self.subTest(args=args):
                self.assertEqual(app.round_half_up(*args), expected)
        self.assertEqual(json.dumps(app.round_half_up(-0.004, 2)), '0')
        self.assertEqual(json.dumps(app.round_half_up(178.0, 2)), '178')


class WeeklySummaryAuthTests(unittest.TestCase):
    """Every rejection happens before any database access."""

    @classmethod
    def setUpClass(cls):
        cls.client = TestClient(app.app)

    def setUp(self):
        guard = patch.object(app, 'db', side_effect=AssertionError('no database access expected'))
        guard.start()
        self.addCleanup(guard.stop)
        self.client.cookies.clear()

    def get(self, headers=None, week=WEEK):
        return self.client.get('/api/weekly-summary', params={'week': week} if week else None,
                               headers=headers or {})

    def test_unset_or_empty_configuration_is_404(self):
        with patch.dict('os.environ', {}, clear=False):
            os.environ.pop('WEEKLY_SUMMARY_TOKEN_SHA256', None)
            os.environ.pop('WEEKLY_SUMMARY_EMAIL', None)
            self.assertEqual(self.get(AUTH).status_code, 404)
        for sha, email in [('', EMAIL), (TOKEN_SHA256, ''), ('  ', EMAIL)]:
            with self.subTest(sha=sha, email=email), weekly_env(sha, email):
                self.assertEqual(self.get(AUTH).status_code, 404)

    def test_missing_wrong_or_malformed_token_is_404(self):
        with weekly_env():
            for headers in [{}, {'Authorization': 'Bearer wrong'}, {'Authorization': TOKEN},
                            {'Authorization': f'Basic {TOKEN}'}, {'Authorization': 'Bearer '},
                            {'Authorization': f'Bearer {TOKEN_SHA256}'},
                            {'Authorization': f'Bearer {TOKEN}x'}]:
                with self.subTest(headers=headers):
                    response = self.get(headers)
                    self.assertEqual(response.status_code, 404)
                    self.assertEqual(response.json(), {'detail': 'Not Found'})

    def test_session_cookie_and_local_dev_are_not_accepted(self):
        with weekly_env(), patch.object(app, 'LOCAL_DEV', True):
            self.client.cookies.set('session', app._sign_session(1))
            self.assertEqual(self.get().status_code, 404)
        self.client.cookies.clear()

    def test_weekly_token_is_not_accepted_on_other_routes(self):
        with weekly_env(), patch.object(app, 'LOCAL_DEV', False):
            self.assertEqual(self.client.get('/api/stats', headers=AUTH).status_code, 401)

    def test_week_must_be_a_monday(self):
        with weekly_env():
            for week in [None, '', '2026-10-27', '2026-02-30', '2026-1-26', '26-10-2026',
                         '2026-10-26T00:00', ' 2026-10-26', 'monday']:
                with self.subTest(week=week):
                    response = self.get(AUTH, week=week)
                    self.assertEqual(response.status_code, 400)
                    self.assertEqual(response.json(), {'error': 'week must be a Monday (YYYY-MM-DD)'})
                    self.assertEqual(response.headers['Cache-Control'], 'no-store')

    def test_internal_errors_hide_details(self):
        with weekly_env():
            response = self.get(AUTH)
        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json(), {'error': 'internal'})
        self.assertEqual(response.headers['Cache-Control'], 'no-store')


@unittest.skipUnless(TEST_DSN, 'Set SPENDING_TEST_DATABASE_URL to a disposable local test DB')
class WeeklySummaryApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = psycopg2.extensions.parse_dsn(TEST_DSN)
        host = config.get('host', '')
        if not config.get('dbname', '').endswith('_test') or not (
                host in ('localhost', '127.0.0.1') or host.startswith('/tmp/')):
            raise RuntimeError('Integration tests require an explicitly named local _test database')
        cls.patches = [patch.object(app, 'DATABASE_URL', TEST_DSN),
                       patch.object(app, '_pool', None), patch.object(app, 'LOCAL_DEV', False),
                       patch.object(app, 'OWNER_EMAIL', '')]
        for item in cls.patches:
            item.start()
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
        self.client.cookies.clear()
        env = weekly_env()
        env.start()
        self.addCleanup(env.stop)
        # Mirrors weekly-review test/fixtures/seed.mjs (spending block).
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE users, upload_jobs, invited_users RESTART IDENTITY CASCADE')
                cur.execute("INSERT INTO users(email,google_id) VALUES(%s,'g1'),('other@example.test','g2') "
                            "RETURNING id", (EMAIL,))
                self.uid, self.other_uid = [r[0] for r in cur.fetchall()]

                def tag(name, parent=None, excluded=False):
                    cur.execute('INSERT INTO tags(user_id,name,group_tag_id,excluded_from_spending) '
                                'VALUES(%s,%s,%s,%s) RETURNING id', (self.uid, name, parent, excluded))
                    return cur.fetchone()[0]
                food = tag('Food')
                self.groceries = tag('Groceries', food)
                dining = tag('Dining', food)
                home = tag('Home')
                transfers = tag('Transfers', None, True)
                cc = tag('Card payment', transfers)

                def tx(day, description, amount, tag_id, status='active', owner=None):
                    cur.execute("""INSERT INTO transactions(user_id,date,description,amount,original_amount,
                        dedup_key,source,primary_tag_id,status,transaction_type)
                        VALUES(%s,%s,%s,%s,%s,%s,'visa',%s,%s,'expense')""",
                        (owner or self.uid, day, description, amount, amount,
                         f'{day}|{description}|{amount}|{status}|{owner}', tag_id, status))
                tx('2026-10-27', 'Whole Foods', '182.40', self.groceries)
                tx('2026-10-29', 'Via Carota', '146.00', dining)
                tx('2026-10-30', 'Hardware store', '64.25', home)
                tx('2026-10-31', 'Refund: hardware', '-20.00', home)
                tx('2026-10-31', 'Card autopay', '2500.00', cc)  # excluded via parent tag
                tx('2026-10-28', 'Duplicate', '182.40', self.groceries, 'deduped')
                tx('2026-10-21', 'Whole Foods', '140.00', self.groceries)
                tx('2026-10-23', 'Pizza', '38.00', dining)
                tx('2026-09-15', 'Old groceries', '400.00', self.groceries)
                tx('2026-10-28', 'Someone else', '999.00', None, owner=self.other_uid)

    def get(self, week=WEEK, headers=AUTH):
        return self.client.get('/api/weekly-summary', params={'week': week}, headers=headers)

    def snapshot(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                rows = []
                for table in ('users', 'transactions', 'tags', 'invited_users', 'transaction_tags'):
                    cur.execute(f'SELECT row_to_json(x)::text FROM {table} x ORDER BY 1')
                    rows.append(cur.fetchall())
                return rows

    def test_matches_weekly_review_seed_expectations_without_writes(self):
        before = self.snapshot()
        response = self.get()
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.assertTrue(response.headers['content-type'].startswith('application/json'))
        body = response.json()
        self.assertEqual(body['version'], 1)
        self.assertEqual(body['app'], 'spending')
        self.assertEqual(body['week'], {'start': '2026-10-26', 'end': '2026-11-01'})
        self.assertRegex(body['generatedAt'], r'^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$')
        self.assertEqual(body['data'], {
            'status': 'ok',
            'latestTransactionDate': '2026-10-31',
            'importsLagging': False,
            'current': {'total': 372.65, 'count': 4},
            'previous': {'total': 178, 'count': 2},
            'weeklyBaseline': 72.25,
            'byCategory': [
                {'category': 'Food', 'total': 328.4, 'weeklyBaseline': 72.25},
                {'category': 'Home', 'total': 44.25, 'weeklyBaseline': 0},
            ],
            'largest': [
                {'date': '2026-10-27', 'description': 'Whole Foods', 'amount': 182.4, 'category': 'Food'},
                {'date': '2026-10-29', 'description': 'Via Carota', 'amount': 146, 'category': 'Food'},
                {'date': '2026-10-30', 'description': 'Hardware store', 'amount': 64.25, 'category': 'Home'},
            ],
            'needsReview': 0,
            'change': {'total': 194.65},
        })
        # JSON numbers, integral values serialised like JSON.stringify.
        self.assertIn('"previous":{"total":178,"count":2}', response.text.replace(' ', ''))
        self.assertEqual(self.snapshot(), before)

    def test_week_totals_agree_with_api_stats(self):
        user = {'id': self.uid, 'role': 'owner', 'is_owner': True, 'email': EMAIL}
        app.app.dependency_overrides[app.get_current_user] = lambda: user
        try:
            stats = self.client.get('/api/stats', params={'date_from': '2026-10-26',
                                                          'date_to': '2026-11-01'}).json()
        finally:
            app.app.dependency_overrides.clear()
        self.assertEqual((round(stats['total'], 2), stats['count']), (372.65, 4))
        self.assertEqual(self.get().json()['data']['current'], {'total': 372.65, 'count': 4})

    def test_review_lag_and_empty_weeks(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE transactions SET needs_review=TRUE WHERE description='Via Carota'")
                cur.execute("UPDATE transactions SET transaction_type=NULL WHERE description='Card autopay'")
        data = self.get().json()['data']
        self.assertEqual(data['needsReview'], 2)  # review counts ignore spending exclusion
        later = self.get('2026-11-09').json()['data']
        self.assertTrue(later['importsLagging'])
        self.assertEqual(later['current'], {'total': 0, 'count': 0})
        self.assertEqual(later['byCategory'], [])
        self.assertEqual(later['largest'], [])

    def test_unknown_email_is_no_account(self):
        with weekly_env(email='nobody@example.test'):
            response = self.get()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()['data'], {'status': 'no-account'})

    def test_email_match_is_case_insensitive(self):
        with weekly_env(email=EMAIL.upper()):
            self.assertEqual(self.get().json()['data']['current']['total'], 372.65)

    def test_invited_reader_reports_owner_ledger_without_touching_last_seen(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO invited_users(email,role) VALUES('Reader@example.test','read')")
        with patch.object(app, 'OWNER_EMAIL', EMAIL):
            with weekly_env(email='reader@example.test'):
                before = self.snapshot()
                self.assertEqual(self.get().json()['data']['current']['total'], 372.65)
                self.assertEqual(self.snapshot(), before)
            with weekly_env(email='other@example.test'):  # neither owner nor invited
                self.assertEqual(self.get().json()['data'], {'status': 'no-account'})

    def test_valid_session_cookie_alone_is_rejected(self):
        self.client.cookies.set('session', app._sign_session(self.uid))
        try:
            self.assertEqual(self.get(headers={}).status_code, 404)
        finally:
            self.client.cookies.clear()


if __name__ == '__main__':
    unittest.main()

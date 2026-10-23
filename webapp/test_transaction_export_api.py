"""Export/list parity on an explicitly disposable local *_test PostgreSQL DB."""
import csv
import io
import os
import unittest
from unittest.mock import patch

import psycopg2.extensions
from fastapi.testclient import TestClient
from test_tag_model import app


TEST_DSN = os.getenv('SPENDING_TEST_DATABASE_URL', '')


@unittest.skipUnless(TEST_DSN, 'Set SPENDING_TEST_DATABASE_URL to a disposable local test DB')
class TransactionExportApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = psycopg2.extensions.parse_dsn(TEST_DSN)
        host = config.get('host', '')
        if not config.get('dbname', '').endswith('_test') or not (
                host in ('localhost', '127.0.0.1') or host.startswith('/tmp/')):
            raise RuntimeError('Integration tests require an explicitly named local _test database')
        cls.patches = [patch.object(app, 'DATABASE_URL', TEST_DSN),
                       patch.object(app, '_pool', None), patch.object(app, 'LOCAL_DEV', False)]
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
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE users, upload_jobs, invited_users RESTART IDENTITY CASCADE')
                cur.execute("INSERT INTO users(email,name) VALUES('export@example.test','Owner'),"
                            "('foreign@example.test','Foreign'),('viewer@example.test','Viewer') RETURNING id")
                self.uid, self.foreign_uid, self.viewer_uid = [r[0] for r in cur.fetchall()]
                cur.execute("INSERT INTO tags(user_id,name,excluded_from_spending) "
                            "VALUES(%s,'Food',FALSE),(%s,'Travel',FALSE),(%s,'Excluded',TRUE) RETURNING id",
                            (self.uid, self.uid, self.uid))
                self.food, self.travel, self.excluded = [r[0] for r in cur.fetchall()]
                cur.execute("INSERT INTO uploaded_files(user_id,filename,file_hash,source,card_last4) "
                            "VALUES(%s,'October.csv','export-oct','Card A','0012'),"
                            "(%s,'Other.csv','export-other','Card B','9876')", (self.uid, self.uid))
        self.user = {'id':self.uid, 'role':'owner', 'is_owner':True, 'email':'export@example.test'}
        app.app.dependency_overrides[app.get_current_user] = lambda: self.user

    def insert(self, number, *, status='active', owner=None, kind='expense', review=False,
               source='Card A', filename='October.csv', day='2026-10-23', primary=None, tags=()):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO transactions(user_id,date,description,amount,source,dedup_key,
                    status,transaction_type,needs_review,import_file,primary_tag_id)
                    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                    (owner or self.uid, day, f'SYNTHETIC purchase {number}', str(number) + '.25',
                     source, f'export-{number}-{owner}-{status}', status, kind, review, filename, primary))
                tx_id = cur.fetchone()[0]
                for tag_id in tags:
                    cur.execute('INSERT INTO transaction_tags(transaction_id,tag_id) VALUES(%s,%s)', (tx_id,tag_id))
        return tx_id

    def export(self, params=None):
        values = [('expected_dataset_id',self.uid), ('expected_actor_id',self.user.get('auth_id',self.uid))]
        return self.client.get('/api/transactions/export.csv', params=values + (params or []))

    @staticmethod
    def rows(response):
        return list(csv.DictReader(io.StringIO(response.content.decode('utf-8-sig'))))

    def test_all_pages_exact_amounts_card_metadata_and_no_financial_writes(self):
        expected = [self.insert(i, primary=self.excluded if i == 1 else self.food) for i in range(1,106)]
        self.insert(999, owner=self.foreign_uid)
        self.assertEqual(len(self.client.get('/api/transactions').json()['transactions']), 100)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT row_to_json(t)::text FROM transactions t ORDER BY id')
                before = cur.fetchall()
        result = self.export([('sort_by','amount'),('sort_dir','asc'),('page',2),('per_page',1)])
        self.assertEqual(result.status_code, 200, result.text)
        rows = self.rows(result)
        self.assertEqual([int(r['ID']) for r in rows], expected)
        self.assertEqual(rows[0]['Amount'], '1.25')
        self.assertEqual(rows[0]['Primary category'], 'Excluded')
        self.assertEqual(rows[0]['Card last 4'], '0012')
        self.assertEqual(result.headers['X-Export-Row-Count'], '105')
        self.assertEqual(result.headers['Cache-Control'], 'no-store')
        self.assertIn('attachment;', result.headers['Content-Disposition'])
        repeated = self.export([('sort_by','amount'),('sort_dir','asc')])
        self.assertEqual(repeated.content, result.content)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT row_to_json(t)::text FROM transactions t ORDER BY id')
                self.assertEqual(cur.fetchall(), before)
                cur.execute('SELECT count(*) FROM transaction_amount_changes')
                self.assertEqual(cur.fetchone()[0], 0)

    def test_every_filter_and_sort_matches_the_transaction_list(self):
        self.insert(1, primary=self.food, tags=(self.travel,), review=True)
        self.insert(-2, kind='refund', primary=self.food, day='2026-10-01')
        self.insert(3, kind=None, source='Card B', filename='Other.csv', tags=(self.travel,))
        self.insert(4, status='deleted')
        self.insert(5, status='deduped')
        self.insert(6, owner=self.foreign_uid)
        cases = [[], [('source','Card B')], [('card_last4','0012')], [('import_file','Other.csv')],
                 [('search','purchase 1')], [('date_from','2026-10-02'),('date_to','2026-10-31')],
                 [('transaction_type','refund')], [('transaction_type','unreviewed')],
                 [('tag','Food')], [('tag','Travel')], [('tag','__none__')],
                 [('tag','Food'),('tag','Travel'),('tag_match','all')],
                 [('tag','Food'),('tag','Travel'),('tag_match','any')],
                 [('status','review')], [('status','deleted')], [('status','deduped')],
                 [('status','review'),('source','Card A'),('card_last4','0012'),('tag','Food')]]
        for filters in cases:
            for sort in ('date','amount','description','source'):
                with self.subTest(filters=filters,sort=sort):
                    params = filters + [('sort_by',sort),('sort_dir','asc')]
                    visible = self.client.get('/api/transactions',params=params).json()['transactions']
                    exported = self.export(params)
                    self.assertEqual(exported.status_code, 200, exported.text)
                    self.assertEqual([int(r['ID']) for r in self.rows(exported)], [r['id'] for r in visible])
        refunded = self.rows(self.export([('transaction_type','refund')]))[0]
        self.assertEqual(refunded['Amount'], '-2.25')

    def test_limit_empty_invalid_filters_and_account_change_fail_closed(self):
        self.insert(1)
        self.insert(2)
        with patch.object(app,'TRANSACTION_EXPORT_MAX_ROWS',1):
            limited = self.export()
            self.assertEqual(limited.status_code,413)
            self.assertNotIn('Content-Disposition',limited.headers)
            self.assertIn('Narrow the filters',limited.json()['detail'])
        empty = self.export([('search','no match')])
        self.assertEqual(self.rows(empty),[])
        self.assertEqual(empty.headers['X-Export-Row-Count'],'0')
        for params in [[('date_from','bad')], [('date_from','2026-10-24'),('date_to','2026-10-23')],
                       [('status','unknown')], [('transaction_type','unknown')], [('tag_match','unknown')]]:
            self.assertEqual(self.export(params).status_code,422)
        with patch.object(app,'db',side_effect=AssertionError('No database query for stale account')):
            response = self.client.get('/api/transactions/export.csv',params={
                'expected_dataset_id':self.foreign_uid,'expected_actor_id':self.uid})
            self.assertEqual(response.status_code,409)
        # The connection remains usable after an oversized result aborted its cursor.
        self.assertEqual(self.export().headers['X-Export-Row-Count'],'2')

    def test_real_auth_allows_viewer_and_denies_signed_out_or_revoked_invitee(self):
        self.insert(1)
        self.insert(2,owner=self.foreign_uid)
        app.app.dependency_overrides.clear()
        with patch.object(app,'OWNER_EMAIL','export@example.test'):
            self.client.cookies.clear()
            self.assertEqual(self.export().status_code,401)
            with app.db() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO invited_users(email,role) VALUES('viewer@example.test','read')")
            self.client.cookies.set('session',app._sign_session(self.viewer_uid))
            params = {'expected_dataset_id':self.uid,'expected_actor_id':self.viewer_uid}
            viewed = self.client.get('/api/transactions/export.csv',params=params)
            self.assertEqual(viewed.status_code,200,viewed.text)
            self.assertEqual(len(self.rows(viewed)),1)
            with app.db() as conn:
                with conn.cursor() as cur:
                    cur.execute("DELETE FROM invited_users WHERE email='viewer@example.test'")
            self.assertEqual(self.client.get('/api/transactions/export.csv',params=params).status_code,403)
        self.client.cookies.clear()

    def test_ambiguous_card_metadata_never_duplicates_or_guesses_rows(self):
        self.insert(1)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO uploaded_files(user_id,filename,file_hash,source,card_last4) "
                            "VALUES(%s,'October.csv','export-ambiguous','Card A','9999')", (self.uid,))
        rows = self.rows(self.export())
        self.assertEqual(len(rows),1)
        self.assertEqual(rows[0]['Card last 4'],'')


if __name__ == '__main__':
    unittest.main()

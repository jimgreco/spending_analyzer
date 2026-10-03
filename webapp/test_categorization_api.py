"""Integration checks against an explicitly supplied, disposable PostgreSQL DB.
Set SPENDING_TEST_DATABASE_URL to a local database whose name ends in _test.
Never point this suite at a database containing real data: it clears test tables.
"""
import os
import unittest
from unittest.mock import patch

import psycopg2.extensions
from fastapi.testclient import TestClient
import test_tag_model as model_tests
from test_tag_model import app, transaction, decision

TEST_DSN=os.getenv('SPENDING_TEST_DATABASE_URL','')


@unittest.skipUnless(TEST_DSN, 'Set SPENDING_TEST_DATABASE_URL to a disposable local test DB')
class CategorizationApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config=psycopg2.extensions.parse_dsn(TEST_DSN)
        host=config.get('host','')
        if not config.get('dbname','').endswith('_test') or not (host in ('localhost','127.0.0.1') or host.startswith('/tmp/')):
            raise RuntimeError('Integration tests require an explicitly named local _test database')
        cls.patches=[patch.object(app,'DATABASE_URL',TEST_DSN),patch.object(app,'_pool',None),patch.object(app,'LOCAL_DEV',False)]
        for p in cls.patches:p.start()
        app.init_db()
        app.init_db()  # new columns/table/index must be idempotent
        cls.client=TestClient(app.app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        app.app.dependency_overrides.clear()
        if app._pool:app._pool.closeall()
        for p in reversed(cls.patches):p.stop()

    def setUp(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('TRUNCATE users, upload_jobs RESTART IDENTITY CASCADE')
                cur.execute("INSERT INTO users(email,name) VALUES('owner@example.test','Test owner'),('other@example.test','Other owner') RETURNING id")
                self.uid,self.other=[r[0] for r in cur.fetchall()]
                cur.execute("INSERT INTO tags(user_id,name) VALUES(%s,'Groceries'),(%s,'Home') RETURNING id",(self.uid,self.uid))
                self.grocery,self.home=[r[0] for r in cur.fetchall()]
        self.user={'id':self.uid,'role':'owner','is_owner':True,'name':'Test owner','email':'owner@example.test'}
        app.app.dependency_overrides[app.get_current_user]=lambda:self.user

    def insert(self, owner=None, manual=False, scope=None, review=False, tag=None, status='active', key='synthetic'):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO transactions(user_id,date,description,amount,source,dedup_key,
                    primary_tag_id,manually_corrected,correction_scope,needs_review,status,primary_migration_status)
                    VALUES(%s,'2026-07-25','EXAMPLE GROCERY',30,'Example card',%s,%s,%s,%s,%s,%s,'auto') RETURNING id""",
                    (owner or self.uid,key,tag,manual,scope,review,status))
                return cur.fetchone()[0]

    def test_guide_persistence_owner_scope_and_read_only_permissions(self):
        self.assertEqual(self.client.get('/api/categorization-guide').json(),{'guide':''})
        self.assertEqual(self.client.put('/api/categorization-guide',json={'guide':' Food for home. '}).status_code,200)
        self.assertEqual(self.client.get('/api/categorization-guide').json()['guide'],'Food for home.')
        self.user['id']=self.other
        self.assertEqual(self.client.get('/api/categorization-guide').json()['guide'],'')
        self.user.update(id=self.uid,role='read')
        self.assertEqual(self.client.put('/api/categorization-guide',json={'guide':'changed'}).status_code,403)
        self.assertEqual(self.client.put('/api/transactions/1/primary-tag',json={'primary_tag':'Home'}).status_code,403)
        self.user['role']='owner'
        self.assertEqual(self.client.put('/api/categorization-guide',json={'guide':'x'*12001}).status_code,422)

    def test_manual_correction_scope_note_review_resolution_and_null(self):
        tx=self.insert(review=True,tag=self.grocery)
        response=self.client.put(f'/api/transactions/{tx}/primary-tag',json={
            'primary_tag':'Home','correction_scope':'similar','correction_note':'Home supplies only'})
        self.assertEqual(response.status_code,200,response.text)
        row=self.client.get('/api/transactions').json()['transactions'][0]
        self.assertEqual(row['primary_tag'],'Home')
        self.assertIn('Groceries',row['tags'])
        self.assertFalse(row['needs_review'])
        self.assertEqual(row['correction_note'],'Home supplies only')
        self.assertEqual(self.client.get('/api/transactions?status=review').json()['total'],0)
        _,history=app.load_categorization_context(self.uid)
        self.assertEqual([r['id'] for r in history],[tx])
        self.client.put(f'/api/transactions/{tx}/primary-tag',json={'primary_tag':None,'correction_scope':'similar'})
        self.assertIsNone(app.load_categorization_context(self.uid)[1][0]['primary_tag'])
        self.client.put(f'/api/transactions/{tx}/primary-tag',json={'primary_tag':'Groceries'})
        self.assertEqual(app.load_categorization_context(self.uid)[1],[])
        self.assertEqual(self.client.put(f'/api/transactions/{tx}/primary-tag',json={'correction_scope':'always'}).status_code,422)
        self.assertEqual(self.client.put(f'/api/transactions/{tx}/primary-tag',json={'correction_note':'x'*1001}).status_code,422)

    def test_history_excludes_automatic_oneoff_deleted_and_other_owner(self):
        legacy=self.insert(manual=True,tag=self.grocery)
        preference=self.insert(manual=True,scope='similar',tag=self.home)
        self.insert(manual=False,tag=self.home)
        self.insert(manual=True,scope='transaction',tag=self.home)
        self.insert(manual=True,status='deleted',tag=self.home)
        self.insert(owner=self.other,manual=True)
        _,history=app.load_categorization_context(self.uid)
        self.assertEqual({r['id'] for r in history},{legacy,preference})

    def test_bulk_correction_and_ownership(self):
        own=self.insert(review=True)
        foreign=self.insert(owner=self.other,manual=True)
        response=self.client.post('/api/transactions/bulk-tag',json={'ids':[own,foreign],'tag':'Home',
            'action':'set-primary','correction_scope':'similar','correction_note':'Equipment'})
        self.assertEqual(response.json()['updated'],1)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT primary_tag_id FROM transactions WHERE id=%s',(foreign,))
                self.assertIsNone(cur.fetchone()[0])
        self.assertEqual(self.client.put(f'/api/transactions/{foreign}/primary-tag',json={'primary_tag':'Home'}).status_code,404)
        self.assertEqual(self.client.get('/api/transactions?status=review').json()['total'],0)
        self.assertEqual(app.load_categorization_context(self.uid)[1][0]['correction_note'],'Equipment')

    def test_clear_all_removes_reusable_preference_and_review(self):
        tx=self.insert(manual=True,scope='similar',review=True,tag=self.home)
        self.assertEqual(self.client.delete(f'/api/transactions/{tx}/tags').status_code,200)
        self.assertEqual(app.load_categorization_context(self.uid)[1],[])
        self.assertEqual(self.client.get('/api/transactions?status=review').json()['total'],0)

    def test_correction_manager_scopes_search_archive_restore_and_edit(self):
        reusable=self.insert(manual=True,scope='similar',tag=self.home,key='reusable')
        legacy=self.insert(manual=True,tag=self.grocery,key='legacy')
        one_time=self.insert(manual=True,scope='transaction',tag=self.grocery,key='one-time')
        self.insert(manual=False,tag=self.home,key='automatic')
        self.insert(manual=True,scope='similar',status='deleted',key='deleted')
        self.insert(owner=self.other,manual=True,scope='similar',key='foreign')
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE transactions SET description='SPECIAL SAMPLE', correction_note='only tools' WHERE id=%s",(reusable,))
        result=self.client.get('/api/categorization-corrections').json()
        self.assertEqual(result['counts'],{'reusable':2,'one_time':1,'archived':0})
        self.assertEqual({r['id'] for r in result['corrections']},{reusable,legacy})
        self.assertEqual(self.client.get('/api/categorization-corrections?kind=one-time').json()['corrections'][0]['id'],one_time)
        self.assertEqual(self.client.get('/api/categorization-corrections?search=tools').json()['corrections'][0]['id'],reusable)
        self.assertEqual(self.client.get('/api/categorization-corrections?kind=invalid').status_code,422)
        self.assertEqual(self.client.delete(f'/api/categorization-corrections/{reusable}').status_code,200)
        self.assertEqual({r['id'] for r in app.load_categorization_context(self.uid)[1]},{legacy})
        self.assertEqual(self.client.get('/api/categorization-corrections?kind=archived').json()['corrections'][0]['id'],reusable)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT primary_tag_id, status FROM transactions WHERE id=%s',(reusable,))
                self.assertEqual(cur.fetchone(),(self.home,'active'))
        self.assertEqual(self.client.post(f'/api/categorization-corrections/{reusable}/restore').status_code,200)
        self.assertEqual({r['id'] for r in app.load_categorization_context(self.uid)[1]},{reusable,legacy})
        self.assertEqual(self.client.put(f'/api/transactions/{legacy}/primary-tag',json={
            'primary_tag':'Home','correction_scope':'transaction','correction_note':'specific item'}).status_code,200)
        self.assertEqual(self.client.get('/api/categorization-corrections?kind=one-time').json()['total'],2)
        self.assertEqual({r['id'] for r in app.load_categorization_context(self.uid)[1]},{reusable})

    def test_correction_manager_authorization_and_one_time_archive(self):
        own=self.insert(manual=True,scope='transaction',tag=self.home)
        foreign=self.insert(owner=self.other,manual=True,scope='similar',tag=self.home)
        self.assertEqual(self.client.delete(f'/api/categorization-corrections/{foreign}').status_code,404)
        self.assertEqual(self.client.post(f'/api/categorization-corrections/{foreign}/restore').status_code,404)
        self.user['role']='read'
        self.assertEqual(self.client.delete(f'/api/categorization-corrections/{own}').status_code,403)
        self.assertEqual(self.client.post(f'/api/categorization-corrections/{own}/restore').status_code,403)
        self.assertEqual(self.client.get('/api/categorization-corrections?kind=one-time').json()['total'],1)
        self.user['role']='owner'
        self.assertEqual(self.client.delete(f'/api/categorization-corrections/{own}').status_code,200)
        self.assertEqual(self.client.get('/api/categorization-corrections?kind=one-time').json()['total'],0)
        self.assertEqual(self.client.get('/api/categorization-corrections?kind=archived').json()['total'],1)
        self.assertEqual(self.client.post(f'/api/categorization-corrections/{own}/restore').status_code,200)
        self.assertEqual(self.client.get('/api/categorization-corrections?kind=one-time').json()['total'],1)

    def test_correction_manager_rejects_stale_edit_archive_and_restore(self):
        tx=self.insert(manual=True,scope='similar',tag=self.grocery)
        row=self.client.get('/api/categorization-corrections').json()['corrections'][0]
        self.assertEqual(row['correction_revision'],0)
        self.user['role']='edit'
        self.assertEqual(self.client.put(f'/api/transactions/{tx}/primary-tag',json={
            'primary_tag':'Home','correction_scope':'similar',
            'expected_correction_revision':0}).status_code,200)
        self.assertEqual(self.client.delete(f'/api/categorization-corrections/{tx}?expected_revision=0').status_code,409)
        self.assertEqual(self.client.put(f'/api/transactions/{tx}/primary-tag',json={
            'primary_tag':'Groceries','expected_correction_revision':0}).status_code,409)
        latest=self.client.get('/api/categorization-corrections').json()['corrections'][0]
        self.assertEqual((latest['primary_tag'],latest['correction_revision']),('Home',1))
        self.assertIn('Groceries',latest['secondary_tags'])
        self.assertEqual(self.client.delete(f'/api/categorization-corrections/{tx}?expected_revision=1').status_code,200)
        self.assertEqual(self.client.delete(f'/api/categorization-corrections/{tx}?expected_revision=1').status_code,409)
        self.assertEqual(self.client.put(f'/api/transactions/{tx}/primary-tag',json={
            'primary_tag':'Groceries','expected_correction_revision':1}).status_code,409)
        archived=self.client.get('/api/categorization-corrections?kind=archived').json()['corrections'][0]
        self.assertEqual((archived['primary_tag'],archived['correction_revision']),('Home',2))
        self.assertEqual(self.client.post(f'/api/categorization-corrections/{tx}/restore?expected_revision=1').status_code,409)
        self.assertEqual(self.client.post(f'/api/categorization-corrections/{tx}/restore?expected_revision=2').status_code,200)
        self.assertEqual(self.client.post(f'/api/categorization-corrections/{tx}/restore?expected_revision=2').status_code,409)
        self.assertEqual(self.client.delete(f'/api/transactions/{tx}').status_code,200)
        self.assertEqual(self.client.post(f'/api/transactions/{tx}/restore').status_code,200)
        self.assertEqual(self.client.put(f'/api/transactions/{tx}/primary-tag',json={
            'primary_tag':'Groceries','expected_correction_revision':3}).status_code,409)
        self.assertEqual(self.client.delete(f'/api/categorization-corrections/{tx}?expected_revision=-1').status_code,422)

    def test_correction_manager_literal_search_and_pagination(self):
        ids=[self.insert(manual=True,scope='similar',tag=self.grocery,key=f'page-{i}') for i in range(53)]
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE transactions SET description='BUDGET_100%' WHERE id=%s",(ids[0],))
        self.assertEqual(self.client.get('/api/categorization-corrections?search=%25').json()['total'],1)
        self.assertEqual(self.client.get('/api/categorization-corrections?search=_').json()['total'],1)
        page=self.client.get('/api/categorization-corrections?limit=50&offset=50').json()
        self.assertEqual((page['total'],len(page['corrections'])),(53,3))
        self.assertEqual([row['id'] for row in page['corrections']],list(reversed(ids[:3])))
        self.assertEqual(self.client.get('/api/categorization-corrections?offset=-1').status_code,422)
        self.assertEqual(self.client.get('/api/categorization-corrections?limit=101').status_code,422)

    def test_import_preserves_row_identity_review_and_dedup(self):
        self.insert(key='existing',tag=self.home,manual=True,scope='transaction')
        rows=[dict(transaction(amount=30),dedup_key='new-30'),dict(transaction(amount=300),dedup_key='new-300'),
              dict(transaction(amount=20),dedup_key='existing')]
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO upload_jobs(id,user_id,filename) VALUES('test-job',%s,'synthetic.csv')",(self.uid,))
        helper=model_tests.TagModelTests()
        client=helper.make_client([decision(0),decision(1,'Home','medium'),decision(2)])
        self.addCleanup(client.close)
        with patch.object(app,'parse_file_bytes',return_value=(rows,'Example card',None)),patch.object(app,'OpenAI',return_value=client):
            app._process_upload_job('test-job',self.uid,'synthetic.csv',b'fake statement',False)
        job=self.client.get('/api/upload/status/test-job').json()
        self.assertEqual(job['status'],'done',job)
        self.assertEqual((job['result']['new'],job['result']['dupes'],job['result']['needs_review']),(2,1,1))
        review_rows=self.client.get('/api/transactions?status=review').json()['transactions']
        self.assertEqual(len(review_rows),1)
        self.assertEqual(review_rows[0]['amount'],300)
        self.assertIsNone(review_rows[0]['primary_tag'])
        self.assertEqual(review_rows[0]['suggested_tag'],'Home')
        self.assertEqual(self.client.get('/api/transactions?status=deduped').json()['total'],1)
        stats=self.client.get('/api/stats')
        self.assertEqual(stats.status_code,200,stats.text)
        # Duplicate upload short-circuits before parsing or an AI call.
        with patch.object(app,'parse_file_bytes',side_effect=AssertionError('must not parse duplicate')):
            app._process_upload_job('test-job',self.uid,'synthetic.csv',b'fake statement',False)
        self.assertEqual(self.client.get('/api/upload/status/test-job').json()['result']['status'],'already_imported')


if __name__=='__main__':unittest.main()

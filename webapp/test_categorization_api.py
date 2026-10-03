"""Integration checks against an explicitly supplied, disposable PostgreSQL DB.
Set SPENDING_TEST_DATABASE_URL to a local database whose name ends in _test.
Never point this suite at a database containing real data: it clears test tables.
"""
import os
import hashlib
import threading
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
        wrong=self.client.post('/api/upload?force=true&expected_file_hash='+('0'*32),
            files={'files':('synthetic.csv',b'not the same file','text/csv')})
        self.assertEqual(wrong.status_code,400)
        self.assertEqual(len(self.client.get('/api/upload/jobs').json()['jobs']),1)
        # Duplicate upload short-circuits before parsing or an AI call.
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO upload_jobs(id,user_id,filename) VALUES('test-job-repeat',%s,'synthetic.csv')",(self.uid,))
        with patch.object(app,'parse_file_bytes',side_effect=AssertionError('must not parse duplicate')):
            app._process_upload_job('test-job-repeat',self.uid,'synthetic.csv',b'fake statement',False)
        self.assertEqual(self.client.get('/api/upload/status/test-job-repeat').json()['result']['status'],'already_imported')

        # A forced reimport repairs one missing refund without touching category
        # corrections or manufacturing duplicate copies of existing rows.
        original=self.client.get('/api/transactions?search=EXAMPLE%20GROCERY').json()['transactions']
        original_id=next(r['id'] for r in original if r['amount']==30)
        self.client.put(f'/api/transactions/{original_id}/primary-tag',json={
            'primary_tag':'Home','correction_scope':'transaction'})
        refund=dict(transaction(description='REFUND',amount=-104.04),dedup_key='refund')
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO upload_jobs(id,user_id,filename) VALUES('test-job-force',%s,'synthetic.csv')",(self.uid,))
        with patch.object(app,'parse_file_bytes',return_value=(rows+[refund],'Example card',None)), \
             patch.object(app,'assign_tags_with_gpt',return_value=[app.review('Choose a category.')]*4):
            app._process_upload_job('test-job-force',self.uid,'synthetic.csv',b'fake statement',True)
        forced=self.client.get('/api/upload/status/test-job-force').json()
        self.assertEqual(forced['status'],'done',forced)
        self.assertEqual((forced['result']['new'],forced['result']['skipped']),(1,3))
        after=self.client.get('/api/transactions?search=EXAMPLE%20GROCERY').json()['transactions']
        self.assertEqual(next(r for r in after if r['id']==original_id)['primary_tag'],'Home')
        self.assertEqual(self.client.get('/api/transactions?status=deduped').json()['total'],1)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO upload_jobs(id,user_id,filename) VALUES('test-job-force-again',%s,'synthetic.csv')",(self.uid,))
        with patch.object(app,'parse_file_bytes',return_value=(rows+[refund],'Example card',None)), \
             patch.object(app,'assign_tags_with_gpt',return_value=[app.review('Choose a category.')]*4):
            app._process_upload_job('test-job-force-again',self.uid,'synthetic.csv',b'fake statement',True)
        repeated=self.client.get('/api/upload/status/test-job-force-again').json()['result']
        self.assertEqual((repeated['new'],repeated['dupes'],repeated['skipped']),(0,0,4))
        self.assertEqual(self.client.get('/api/transactions?status=deduped').json()['total'],1)

    def test_jobs_are_user_scoped_and_stale_work_is_recoverable(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO upload_jobs(id,user_id,filename,status,updated_at)
                    VALUES('stale-job',%s,'synthetic.csv','categorizing',NOW()-INTERVAL '5 minutes')""",(self.uid,))
        jobs=self.client.get('/api/upload/jobs').json()['jobs']
        self.assertEqual(jobs[0]['status'],'interrupted')
        self.assertIn('Re-upload',jobs[0]['result']['message'])
        self.assertEqual(self.client.get('/api/upload/jobs?filename=synthetic.csv').json()['jobs'][0]['job_id'],'stale-job')
        self.assertEqual(self.client.get('/api/upload/jobs?filename=other.csv').json()['jobs'],[])
        self.user['id']=self.other
        self.assertEqual(self.client.get('/api/upload/status/stale-job').status_code,404)
        self.assertEqual(self.client.get('/api/upload/jobs').json()['jobs'],[])

    def test_concurrent_uploads_of_same_file_insert_once(self):
        rows=[dict(transaction(),dedup_key='single')]
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO upload_jobs(id,user_id,filename)
                    VALUES('race-one',%s,'race.csv'),('race-two',%s,'race.csv')""",(self.uid,self.uid))
        errors=[]
        def run(job_id):
            try: app._process_upload_job(job_id,self.uid,'race.csv',b'race bytes',False)
            except Exception as exc: errors.append(exc)
        with patch.object(app,'parse_file_bytes',return_value=(rows,'Example card',None)), \
             patch.object(app,'assign_tags_with_gpt',return_value=[app.review('Choose a category.')]):
            workers=[threading.Thread(target=run,args=(job_id,)) for job_id in ('race-one','race-two')]
            for worker in workers: worker.start()
            for worker in workers: worker.join()
        self.assertEqual(errors,[])
        results=[self.client.get(f'/api/upload/status/{job_id}').json()['result']['status']
                 for job_id in ('race-one','race-two')]
        self.assertCountEqual(results,['ok','already_imported'])
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM transactions WHERE user_id=%s AND import_file='race.csv'",(self.uid,))
                self.assertEqual(cur.fetchone()[0],1)

    def test_same_transfer_on_distinct_bofa_accounts_stays_active(self):
        description='Online Banking transfer from CHK'
        args=('2026-03-30','Bank of America',650000,description)
        legacy=app.make_dedup_key(*args)
        def account_row(last4):
            return dict(transaction(description=description,amount=650000,source='Bank of America'),
                date='2026-03-30',account_key='bofa:'+last4,legacy_dedup_key=legacy,
                dedup_key=app.make_dedup_key(*args,account_key='bofa:'+last4))
        for name,last4 in [('joint.csv','5090'),('jim.csv','5191')]:
            job_id='account-'+last4
            with app.db() as conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO upload_jobs(id,user_id,filename) VALUES(%s,%s,%s)',
                                (job_id,self.uid,name))
            with patch.object(app,'parse_file_bytes',return_value=([account_row(last4)],'Bank of America',None)), \
                 patch.object(app,'assign_tags_with_gpt',return_value=[app.review('Synthetic review.')]):
                app._process_upload_job(job_id,self.uid,name,name.encode(),False)
            self.assertEqual(self.client.get(f'/api/upload/status/{job_id}').json()['result']['new'],1)
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM transactions WHERE user_id=%s AND status='active' AND amount=650000",(self.uid,))
                self.assertEqual(cur.fetchone()[0],2)
                cur.execute("SELECT card_last4 FROM uploaded_files WHERE user_id=%s ORDER BY filename",(self.uid,))
                self.assertEqual({r[0] for r in cur.fetchall()},{'5090','5191'})

        # A legacy collision with no verified account is kept active for review.
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO transactions(user_id,date,description,amount,source,dedup_key,
                    status,import_file) VALUES(%s,'2026-03-30',%s,650000,'Bank of America',%s,
                    'active','old-unknown.csv')""",(self.uid,description,legacy))
                cur.execute("INSERT INTO upload_jobs(id,user_id,filename) VALUES('account-9142',%s,'trust.csv')",(self.uid,))
        with patch.object(app,'parse_file_bytes',return_value=([account_row('9142')],'Bank of America',None)), \
             patch.object(app,'assign_tags_with_gpt',return_value=[app.review('Synthetic review.')]):
            app._process_upload_job('account-9142',self.uid,'trust.csv',b'trust bytes',False)
        result=self.client.get('/api/upload/status/account-9142').json()['result']
        self.assertEqual((result['new'],result['dupes'],result['possible_overlap']),(1,0,1))
        self.assertEqual(self.client.get('/api/transactions?status=review').json()['total'],3)

    def test_forced_legacy_bofa_reimport_skips_existing_row(self):
        content=b'old statement bytes'
        file_hash=hashlib.md5(content).hexdigest()
        args=('2026-03-30','Bank of America',650000,'Online Banking transfer from CHK')
        old_key=app.make_dedup_key(args[0],'BofA',args[2],args[3])
        new_key=app.make_dedup_key(*args,account_key='bofa:5090')
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO uploaded_files(user_id,filename,file_hash,source,tx_new)
                    VALUES(%s,'old-bofa.pdf',%s,'BofA',1)""",(self.uid,file_hash))
                cur.execute("""INSERT INTO transactions(user_id,date,description,amount,source,
                    dedup_key,status,import_file,manually_corrected) VALUES
                    (%s,'2026-03-30',%s,650000,'BofA',%s,'active','old-bofa.pdf',TRUE)""",
                    (self.uid,args[3],old_key))
                cur.execute("INSERT INTO upload_jobs(id,user_id,filename) VALUES('legacy-force',%s,'old-bofa.pdf')",(self.uid,))
        row=dict(transaction(description=args[3],amount=650000,source='Bank of America'),
                 date=args[0],account_key='bofa:5090',legacy_dedup_key=old_key,
                 legacy_dedup_keys=[app.make_dedup_key(*args),old_key],dedup_key=new_key)
        with patch.object(app,'parse_file_bytes',return_value=([row],'Bank of America',None)), \
             patch.object(app,'assign_tags_with_gpt',return_value=[app.review('Synthetic review.')]):
            app._process_upload_job('legacy-force',self.uid,'old-bofa.pdf',content,True)
        result=self.client.get('/api/upload/status/legacy-force').json()['result']
        self.assertEqual((result['new'],result['dupes'],result['skipped']),(0,0,1))
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*),BOOL_AND(manually_corrected) FROM transactions WHERE user_id=%s AND import_file='old-bofa.pdf'",(self.uid,))
                self.assertEqual(cur.fetchone(),(1,True))


if __name__=='__main__':unittest.main()

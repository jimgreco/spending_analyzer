"""Synthetic statement fixtures; no real statements, database, or API traffic."""
import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import test_tag_model as model_tests
from import_checks import reconcile_card_statement, detect_account_key

app = model_tests.app


APPLE_PAGE = """Apple Card Goldman Sachs Daily Cash
Payments
Date Description Amount
08/31/2026 ACH PAYMENT -$50.00
Total payments for this period -$50.00
Transactions
Date Description Daily Cash Amount
08/15/2026 BLUE HILL 2% $2.08 $104.04
08/15/2026 BLUE HILL -$104.04
(RETURN)
Daily Cash Adjustment -2% $2.08
08/16/2026 COFFEE 1% $0.10 $10.00
Total charges, credits and returns $12.08
"""
APPLE_ROWS = [
    {'date':'2026-08-31','description':'ACH PAYMENT','amount':-50},
    {'date':'2026-08-15','description':'BLUE HILL','amount':104.04},
    {'date':'2026-08-15','description':'BLUE HILL RETURN','amount':-104.04},
    {'date':'2026-08-16','description':'COFFEE','amount':10},
]
COINBASE_PAGE = """Coinbase One Card
Payments and credits
Date Description Amount
Sep 23, 2026 ACH PAYMENT -$8,412.03
Total payments and credits in this period -$8,412.03
Transactions
Date Description Amount
Sep 15, 2026 SUBWAY $3.00
Sep 15, 2026 SUBWAY $3.00
Sep 16, 2026 SHOP $8,416.59
Total new charges in this period $8,422.59
"""
COINBASE_ROWS = [
    {'date':'2026-09-23','description':'ACH PAYMENT','amount':-8412.03},
    {'date':'2026-09-15','description':'SUBWAY','amount':3},
    {'date':'2026-09-15','description':'SUBWAY','amount':3},
    {'date':'2026-09-16','description':'SHOP','amount':8416.59},
]


class ImportCheckTests(unittest.TestCase):
    def test_apple_refund_payment_and_undated_reward_adjustment(self):
        summary = reconcile_card_statement([APPLE_PAGE], 'Apple Card', APPLE_ROWS)
        self.assertEqual(summary, {'rows':4,'net':-40.0,'transactions_net':10.0,
                                   'payments_net':-50.0,'undated_adjustments':2.08})
        with self.assertRaisesRegex(ValueError, '1 missing'):
            reconcile_card_statement([APPLE_PAGE], 'Apple Card',
                                     [r for r in APPLE_ROWS if r['amount'] != -104.04])
        with self.assertRaisesRegex(ValueError, '1 missing, 1 unexpected'):
            reconcile_card_statement([APPLE_PAGE], 'Apple Card',
                                     [{**r, 'amount':104.04} if r['amount'] == -104.04 else r
                                      for r in APPLE_ROWS])

    def test_coinbase_repeated_charge_and_large_payment(self):
        self.assertEqual(reconcile_card_statement([COINBASE_PAGE], 'Coinbase', COINBASE_ROWS)['net'], 10.56)
        with self.assertRaisesRegex(ValueError, '1 missing'):
            reconcile_card_statement([COINBASE_PAGE], 'Coinbase', COINBASE_ROWS[:-2] + COINBASE_ROWS[-1:])
        with self.assertRaisesRegex(ValueError, '1 missing, 1 unexpected'):
            reconcile_card_statement([COINBASE_PAGE], 'Coinbase',
                [{**r, 'amount':8412.03} if r['amount'] == -8412.03 else r for r in COINBASE_ROWS])

    def test_bofa_account_aware_keys_and_checking_signs(self):
        first = ('Bank of America\nAccount number: 4830 7761 5090\n'
                 'Beginning balance on January 1, 2026 $1,000.00\n'
                 'Ending balance on January 31, 2026 $950.00')
        pages = [first, 'not activity',
                 'Date Description Amount\n01/02/26 DEPOSIT 100.00\n01/03/26 WITHDRAWAL -150.00']
        rows = [{'date':'2026-01-02','amount':-100}, {'date':'2026-01-03','amount':150}]
        self.assertEqual(detect_account_key(pages,'Bank of America'),'bofa:5090')
        self.assertEqual(reconcile_card_statement(pages,'Bank of America',rows,'checking.pdf')['net'],50)
        with self.assertRaisesRegex(ValueError, '1 missing, 1 unexpected'):
            reconcile_card_statement(pages,'Bank of America',
                                     [rows[0],{'date':'2026-01-03','amount':-150}], 'checking.pdf')
        with self.assertRaisesRegex(ValueError, '0 missing, 1 unexpected'):
            reconcile_card_statement(pages,'Bank of America',rows+[rows[-1]],'checking.pdf')
        same = ('2026-03-30','Bank of America',650000,
                'Online Banking transfer from CHK',1)
        self.assertNotEqual(app.make_dedup_key(*same,account_key='bofa:5090'),
                            app.make_dedup_key(*same,account_key='bofa:5191'))
        self.assertEqual(app.make_dedup_key(*same),
                         app.make_dedup_key(*same,account_key=''))

    def test_bofa_card_refund_yearend_credit_and_posting_date(self):
        pages = ['Bank of America\nAccount# 4400 6671 6839 5188\n'
                 'Previous Balance $100.00\nNew Balance Total $110.00', '',
                 'Transactions\n09/01 09/02 SHOP 20.00\n09/03 09/04 REFUND 10.00CR']
        rows = [{'date':'2026-09-02','amount':20}, {'date':'2026-09-03','amount':-10}]
        self.assertEqual(reconcile_card_statement(pages,'Bank of America',rows,'2026-09-17.pdf')['net'],10)
        with self.assertRaisesRegex(ValueError, '1 missing'):
            reconcile_card_statement(pages,'Bank of America',rows[:1],'2026-09-17.pdf')
        yearend = ['Bank of America\n2025 year-end summary of credit card transactions\n'
                   'credit card ending in 5188\nTotal spent Total interest\n$20.00',
                   '07/04/25 PLAYSTATION 10.00CR\n07/05/25 PLAYSTATION 30.00']
        self.assertEqual(reconcile_card_statement(yearend,'Bank of America',[
            {'date':'2025-07-04','amount':-10},{'date':'2025-07-05','amount':30}],
            'BofA Personal 2025.pdf')['rows'],2)
        with self.assertRaisesRegex(ValueError, '1 missing'):
            reconcile_card_statement(yearend,'Bank of America',[
                {'date':'2025-07-05','amount':30}], 'BofA Personal 2025.pdf')

    def test_chase_reward_section_is_not_account_activity(self):
        pages = ['Prime Visa', '',
                 'Transaction Merchant Name or Transaction Description $ Amount\n'
                 'PAYMENTS AND OTHER CREDITS\n02/09 AMAZON REFUND -645.33\n'
                 'PURCHASE\n02/10 AMAZON PURCHASE 700.00\n'
                 'Transaction Merchant Name or Transaction Description $ Amount Rewards\n'
                 'SHOP WITH POINTS ACTIVITY\n02/11 CHASE SHOP WITH POINTS 3.00']
        rows = [{'date':'2026-02-09','amount':-645.33},
                {'date':'2026-02-10','amount':700}]
        self.assertEqual(reconcile_card_statement(pages,'Amazon',rows,'Amazon Feb26.pdf')['rows'],2)
        with self.assertRaisesRegex(ValueError, '1 missing'):
            reconcile_card_statement(pages,'Amazon',rows[-1:],'Amazon Feb26.pdf')
        with self.assertRaisesRegex(ValueError, 'unexpected dated rows'):
            reconcile_card_statement(pages,'Amazon',rows+[
                {'date':'2026-02-11','amount':3}], 'Amazon Feb26.pdf')

    def test_citi_credit_sign_and_duplicate_row_multiplicity(self):
        pages = ['Citi Double Cash\nBilling Period: 01/01/26-02/25/26\n'
                 'Previous balance $1,000.00\nNew balance $1,005.00', '',
                 '02/10 REWARD CREDIT -$5.00\n02/11 PURCHASE $10.00']
        rows = [{'date':'2026-02-10','amount':-5},
                {'date':'2026-02-11','amount':10}]
        self.assertEqual(reconcile_card_statement(pages,'Citi',rows,'Citi 2025-02-25.pdf')['net'],5)
        with self.assertRaisesRegex(ValueError, 'unexpected dated rows'):
            reconcile_card_statement(pages,'Citi',rows+[rows[-1]],'Citi 2025-02-25.pdf')

    def test_printed_total_disagreement_blocks_import(self):
        with self.assertRaisesRegex(ValueError, 'transactions total disagrees'):
            reconcile_card_statement([APPLE_PAGE.replace('$12.08', '$13.08')],
                                     'Apple Card', APPLE_ROWS)

    def test_omitted_apple_refund_gets_focused_recovery_then_recheck(self):
        pdf = MagicMock()
        pdf.__enter__.return_value.pages = [SimpleNamespace(extract_text=lambda: APPLE_PAGE)]
        missing_refund = [r for r in APPLE_ROWS if r['amount'] != -104.04]
        refund = [r for r in APPLE_ROWS if r['amount'] == -104.04]
        with patch.object(app.pdfplumber, 'open', return_value=pdf), \
             patch.object(app, 'parse_with_gpt', side_effect=[
                 (missing_refund,None,''), (refund,None,'')]) as extract:
            rows, source, error = app.parse_file_bytes(b'synthetic pdf', 'apple.pdf')
        self.assertEqual(error, '')
        self.assertEqual(source, 'Apple Card')
        self.assertEqual(len(rows), 4)
        self.assertEqual([r['amount'] for r in rows if r['amount'] < 0], [-50,-104.04])
        self.assertIn('BLUE HILL -$104.04', extract.call_args_list[1].args[0])

    def test_unrepaired_refund_blocks_all_rows(self):
        pdf = MagicMock()
        pdf.__enter__.return_value.pages = [SimpleNamespace(extract_text=lambda: APPLE_PAGE)]
        missing_refund = [r for r in APPLE_ROWS if r['amount'] != -104.04]
        with patch.object(app.pdfplumber, 'open', return_value=pdf), \
             patch.object(app, 'parse_with_gpt', side_effect=[
                 (missing_refund,None,''), ([],None,'')]):
            rows, source, error = app.parse_file_bytes(b'synthetic pdf', 'apple.pdf')
        self.assertEqual(rows, [])
        self.assertIsNone(source)
        self.assertIn('1 missing', error)

    def test_chunk_failure_does_not_return_partial_rows(self):
        content = ('Date,Description,Amount\n' + '2026-08-01,X,1\n' * 3000).encode()
        first = ([{'date':'2026-08-01','description':'X','amount':1}], None, '')
        with patch.object(app, 'parse_with_gpt', side_effect=[first, ([],None,'timeout')]):
            rows, source, error = app.parse_file_bytes(content, 'synthetic.csv')
        self.assertEqual(rows, [])
        self.assertIsNone(source)
        self.assertIn('Chunk 2/', error)

    def test_ambiguous_dedup_identity_blocks_statement(self):
        collision = [
            {'date':'2026-08-01','description':'ABCDEFGHIJKL FIRST','amount':3},
            {'date':'2026-08-01','description':'ABCDEFGHIJKL SECOND','amount':3},
        ]
        with patch.object(app, 'parse_with_gpt', return_value=(collision,None,'')):
            rows, source, error = app.parse_file_bytes(b'synthetic', 'synthetic.csv')
        self.assertEqual(rows, [])
        self.assertIn('ambiguous duplicate', error)

    def test_truncated_response_and_invalid_rows_fail_closed(self):
        class Choice:
            finish_reason = 'length'
            message = type('Message', (), {'content':'{"transactions":[]}'})()
        class Client:
            chat = type('Chat', (), {'completions':type('Completions', (), {
                'create':lambda self, **kw:type('Response', (), {'choices':[Choice()]})()})()})()
        with patch.object(app, 'OpenAI', return_value=Client()):
            self.assertIn('ended early', app.parse_with_gpt('synthetic','x.csv')[2])
        Choice.finish_reason = 'stop'
        Choice.message = type('Message', (), {'content':json.dumps({'transactions':[
            {'date':'2026-08-01','description':'OK','amount':2},
            {'date':'2026-08-02','description':'BAD','amount':'NaN'}]})})()
        with patch.object(app, 'OpenAI', return_value=Client()):
            self.assertIn('invalid row', app.parse_with_gpt('synthetic','x.csv')[2])

    def test_citi_bofa_section_sign_contract(self):
        # Keep payments made separate from payments received and rewards credits.
        prompt = app.GPT_PARSE_PROMPT
        self.assertIn('payments made/outflows', prompt)
        self.assertIn('payments received/inflows', prompt)
        self.assertIn('refunds/credits', prompt)
        self.assertIn('section\'s meaning', prompt)
        fixtures = [
            {'date':'2026-08-01','description':'CITI PAYMENT RECEIVED','amount':-250.00},
            {'date':'2026-08-02','description':'CITI REWARD CREDIT','amount':-5.25},
            {'date':'2026-08-03','description':'BOFA CHECKING WITHDRAWAL','amount':42.00},
            {'date':'2026-08-04','description':'BOFA CHECKING DEPOSIT','amount':-100.00},
        ]
        response = SimpleNamespace(choices=[SimpleNamespace(finish_reason='stop',
            message=SimpleNamespace(content=json.dumps({'transactions':fixtures})))])
        client = SimpleNamespace(chat=SimpleNamespace(completions=SimpleNamespace(
            create=lambda **kwargs: response)))
        with patch.object(app, 'OpenAI', return_value=client):
            parsed, _, error = app.parse_with_gpt('synthetic Citi and BofA sections', 'signs.csv')
        self.assertEqual(error, '')
        self.assertEqual([r['amount'] for r in parsed],[-250.00,-5.25,42.00,-100.00])
        self.assertEqual(reconcile_card_statement(None, 'Citi', APPLE_ROWS), None)
        self.assertEqual(reconcile_card_statement(None, 'Bank of America', APPLE_ROWS), None)


if __name__ == '__main__':
    unittest.main()

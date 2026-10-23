"""Synthetic CSV and client-flow checks; no financial records or external API."""
import csv
from decimal import Decimal
import io
from pathlib import Path
import shutil
import subprocess
import unittest

from transaction_export import ExportTooLarge, transactions_csv


def transaction(**changes):
    return dict(id=7, date='2026-10-23', description='Coffee', amount=Decimal('-10.25'),
                source='Example card', card_last4='0012', primary_tag='Food', tags=['Travel'],
                transaction_type='refund', status='active', import_file='October.csv',
                needs_review=False, **changes)


class TransactionExportTests(unittest.TestCase):
    def test_csv_quotes_text_preserves_unicode_and_exact_signed_amounts(self):
        row = transaction()
        row.update(description='Café, "quoted"\nsecond line', amount=Decimal('9999999999.99'))
        result, count = transactions_csv([row, transaction()])
        self.assertTrue(result.startswith(b'\xef\xbb\xbf'))
        self.assertEqual(count, 2)
        rows = list(csv.DictReader(io.StringIO(result.decode('utf-8-sig'))))
        self.assertEqual(rows[0]['Description'], row['description'])
        self.assertEqual(rows[0]['Amount'], '9999999999.99')
        self.assertEqual(rows[1]['Amount'], '-10.25')
        self.assertEqual(rows[1]['Card last 4'], '0012')

    def test_every_text_column_neutralizes_spreadsheet_formulas(self):
        for prefix in ('=', '+', '-', '@', '  =', '\t', '\r', '\n'):
            with self.subTest(prefix=repr(prefix)):
                text = prefix + 'SYNTHETIC()'
                row = transaction()
                row.update(description=text, source=text, card_last4=text, primary_tag=text,
                           tags=[text], transaction_type=text, status=text, import_file=text)
                result, _ = transactions_csv([row])
                exported = next(csv.DictReader(io.StringIO(result.decode('utf-8-sig'))))
                for name in ('Description', 'Source', 'Card last 4', 'Primary category',
                             'Tags', 'Type', 'Status', 'Import file'):
                    self.assertEqual(exported[name], "'" + text)
                self.assertEqual(exported['Amount'], '-10.25')

    def test_row_and_byte_limits_fail_without_returning_partial_csv(self):
        consumed = []
        def rows():
            for number in range(10):
                consumed.append(number)
                yield transaction()
        with self.assertRaisesRegex(ExportTooLarge, 'limited to 2'):
            transactions_csv(rows(), max_rows=2)
        self.assertEqual(consumed, [0, 1, 2])
        with self.assertRaisesRegex(ExportTooLarge, 'too large'):
            transactions_csv([transaction()], max_bytes=200)

    def test_empty_export_has_header_and_no_placeholder_rows(self):
        result, count = transactions_csv([])
        self.assertEqual(count, 0)
        self.assertEqual(len(list(csv.reader(io.StringIO(result.decode('utf-8-sig'))))), 1)

    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_browser_cancellation_retry_account_and_filter_guards(self):
        result = subprocess.run(['node', '--test', str(Path(__file__).with_name('check_transaction_export.js'))],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()

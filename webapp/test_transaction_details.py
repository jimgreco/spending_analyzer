"""Run read-only transaction detail checks against the shipped browser code."""
from pathlib import Path
import shutil
import subprocess
import unittest


class TransactionDetailsTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_read_only_transaction_details(self):
        script = Path(__file__).with_name('check_transaction_details.js')
        result = subprocess.run(['node', '--test', str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

"""Check the selected-transaction amount shown by the browser UI."""
from pathlib import Path
import shutil
import subprocess
import unittest


class SelectionSummaryTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_selected_transaction_amount(self):
        script = Path(__file__).with_name('check_selection_summary.js')
        result = subprocess.run(['node', '--test', str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

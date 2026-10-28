"""Exercise selected account breakdowns using the shipped browser code."""
from pathlib import Path
import shutil
import subprocess
import unittest


class SelectionAccountTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_selected_account_breakdown(self):
        script = Path(__file__).with_name('check_selection_accounts.js')
        result = subprocess.run(['node', '--test', str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

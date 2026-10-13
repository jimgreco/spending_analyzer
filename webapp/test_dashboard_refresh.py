"""Run synthetic browser behavior checks using the shipped inline JavaScript."""
from pathlib import Path
import shutil
import subprocess
import unittest


class DashboardRefreshTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_mutations_keep_spending_summaries_current(self):
        script = Path(__file__).with_name('check_dashboard_refresh.js')
        result = subprocess.run(['node', '--test', str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

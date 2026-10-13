"""Check browser sign corrections with synthetic deferred responses."""
from pathlib import Path
import shutil
import subprocess
import unittest


class SignDialogTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_sign_preview_and_save_races(self):
        script = Path(__file__).with_name('check_sign_dialog.js')
        result = subprocess.run(['node', '--test', str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

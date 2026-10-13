"""Check guide editor draft protection with synthetic deferred responses."""
from pathlib import Path
import shutil
import subprocess
import unittest


class GuideDialogTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_guide_load_and_save_races(self):
        script = Path(__file__).with_name('check_guide_dialog.js')
        result = subprocess.run(['node', '--test', str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

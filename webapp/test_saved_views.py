from pathlib import Path
import shutil
import subprocess
import unittest

class SavedViewsTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed for browser behavior')
    def test_saved_views(self):
        result = subprocess.run(['node', '--test', str(Path(__file__).with_name('check_saved_views.js'))], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

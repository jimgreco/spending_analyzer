"""Exercise asynchronous preview navigation in the actual browser script."""
from pathlib import Path
import shutil
import subprocess
import unittest


class TypePreviewTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_preview_navigation_races_and_retry(self):
        script = Path(__file__).with_name('check_type_preview.js')
        result = subprocess.run(['node', '--test', str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

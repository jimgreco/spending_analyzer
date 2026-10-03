"""Run the real browser-template malicious-name check when Node is installed."""
from pathlib import Path
import shutil
import subprocess
import unittest


class NamingHandlerTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate the inline UI templates')
    def test_hostile_names_remain_data_across_controls(self):
        script = Path(__file__).with_name('check_naming_handlers.js')
        result = subprocess.run(['node', str(script)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()

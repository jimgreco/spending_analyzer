"""Run synthetic period comparison regressions across date/time-zone boundaries."""
import os
from pathlib import Path
import shutil
import subprocess
import unittest


class PeriodComparisonTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'Node is needed to evaluate browser behavior')
    def test_comparison_across_timezones(self):
        script = Path(__file__).with_name('check_period_comparison.js')
        for zone in ('UTC', 'America/Los_Angeles', 'Pacific/Auckland'):
            with self.subTest(timezone=zone):
                result = subprocess.run(['node', '--test', str(script)],
                                        env={**os.environ, 'TZ': zone},
                                        capture_output=True, text=True)
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

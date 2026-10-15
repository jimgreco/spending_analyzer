"""Release version remains available independently of image SHA metadata."""
import unittest
from fastapi.testclient import TestClient
from test_tag_model import app


class VersionTests(unittest.TestCase):
    def test_api_and_openapi_expose_marketing_version_with_build_metadata(self):
        client = TestClient(app.app)
        version = client.get('/api/version').json()
        self.assertEqual(version['version'], '1.3.0')
        self.assertIn('sha', version)
        self.assertIn('timestamp', version)
        self.assertEqual(app.app.version, version['version'])
        client.close()

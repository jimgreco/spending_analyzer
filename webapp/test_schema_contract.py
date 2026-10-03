"""Schema control must fail closed without running historical repairs."""
import unittest
from unittest.mock import patch
from test_tag_model import app
from schema_contract import assert_schema_compatible, CONTRACT

class Cursor:
    def __init__(self, drift=False): self.sql = []; self.drift = drift
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def execute(self, sql, params=None): self.sql.append(sql)
    def fetchone(self): return (1,)
    def fetchall(self):
        if self.sql[-1].startswith('SELECT version'): return [(CONTRACT['version'],)]
        rows = CONTRACT['objects'][1:] if self.drift else CONTRACT['objects']
        return [(r['kind'], r['name'], r['definition']) for r in rows]

class Connection:
    def __init__(self, drift=False): self.c = Cursor(drift)
    def cursor(self): return self.c

class SchemaContractTests(unittest.TestCase):
    def test_runtime_only_reads(self):
        conn = Connection()
        assert_schema_compatible(conn)
        self.assertTrue(all(q.lstrip().startswith('SELECT') for q in conn.c.sql))
        with self.assertRaisesRegex(RuntimeError, 'incompatible'):
            assert_schema_compatible(Connection(drift=True))

    def test_startup_and_adoption_never_run_historical_repairs(self):
        from contextlib import contextmanager
        conn = Connection()
        @contextmanager
        def db(): yield conn
        with patch.object(app, 'db', db), patch.object(app, '_initialize_legacy_schema_and_data') as legacy:
            app.startup()
            app.init_db(adopt_existing=True)
            legacy.assert_not_called()
        writes = [q for q in conn.c.sql if q.lstrip().startswith(('UPDATE', 'DELETE', 'INSERT'))]
        self.assertEqual(len(writes), 1)
        self.assertTrue(writes[0].startswith('INSERT INTO public.app_schema_versions'))

    def test_invalid_adoption_does_not_record_compatibility(self):
        from contextlib import contextmanager
        conn = Connection(drift=True)
        @contextmanager
        def db(): yield conn
        with patch.object(app, 'db', db), patch.object(app, '_initialize_legacy_schema_and_data') as legacy:
            with self.assertRaisesRegex(RuntimeError, 'incompatible'):
                app.init_db(adopt_existing=True)
            legacy.assert_not_called()
        self.assertFalse(any('INSERT INTO' in q for q in conn.c.sql))

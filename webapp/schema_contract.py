"""Read-only schema compatibility contract. Never reads application rows."""
import json
from pathlib import Path
CONTRACT = json.loads(Path(__file__).with_name('schema-contract.json').read_text())
CATALOG_SQL = Path(__file__).with_name('schema-catalog.sql').read_text()

def assert_schema_compatible(conn, require_version=True):
    try:
        with conn.cursor() as cur:
            if require_version:
                cur.execute('SELECT version FROM public.app_schema_versions WHERE app=%s AND version=%s',
                            (CONTRACT['app'], CONTRACT['version']))
                if len(cur.fetchall()) != 1:
                    raise RuntimeError('Missing compatibility version')
            cur.execute(CATALOG_SQL)
            actual = {(kind, name): definition for kind, name, definition in cur.fetchall()}
            if any(actual.get((row['kind'], row['name'])) != row['definition'] for row in CONTRACT['objects']):
                raise RuntimeError('Schema structure mismatch')
    except Exception:
        raise RuntimeError('Database schema is incompatible. Run the reviewed migration/adoption command before starting this release.') from None

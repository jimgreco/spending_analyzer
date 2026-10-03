"""One-shot schema preparation; no fallback to the runtime database URL."""
import argparse
import os
import sys

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--adopt-existing', action='store_true')
    args = parser.parse_args()
    migration_url = os.environ.get('MIGRATION_DATABASE_URL')
    if not migration_url:
        raise RuntimeError('MIGRATION_DATABASE_URL is required.')
    os.environ['DATABASE_URL'] = migration_url
    # No HTTP is served; avoid generating an unused session key on import.
    os.environ.setdefault('SECRET_KEY', 'schema-command-no-http')
    os.environ['LOCAL_DEV'] = 'false'
    import app
    try:
        app.init_db(adopt_existing=args.adopt_existing)
        print('Database schema ready.')
    finally:
        if app._pool:
            app._pool.closeall()

if __name__ == '__main__':
    try:
        main()
    except Exception:
        print('Schema migration failed. Verify migration access and schema compatibility; existing databases require reviewed --adopt-existing. No server started.', file=sys.stderr)
        sys.exit(1)

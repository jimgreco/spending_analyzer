# Spending Dashboard — Gemini Guide

## Deployment (EC2 + Docker)
Migrated from Elastic Beanstalk to a consolidated EC2 instance (`18.219.163.101`).

### Infrastructure Details
- **Runtime**: Python 3.11-slim Docker container.
- **Database**: Local Dockerized PostgreSQL (shared with Macros app).
- **CI/CD**: GitHub Actions auto-deploy on push to `main`.

### Auto-Deployment Workflow
1. GitHub Action checks out code.
2. `rsync` transfers the `webapp/` directory to the server.
3. Server runs `docker-compose build spending && docker-compose up -d spending`.

## Key Patches
- **Schema Initialization**: `webapp/app.py` was patched to wrap `cur.execute(SCHEMA)` in a try-except block to handle `SERIAL` sequence conflicts when starting on an existing database.
- **Internal Networking**: Database URL is `postgresql://admin:${DB_PASSWORD}@db:5432/spending`.

## Environment Variables (Production)
Managed in `~/deploy/.env` on the EC2 instance:
- `SPENDING_OPENAI_API_KEY`
- `SPENDING_GOOGLE_CLIENT_ID / SECRET`
- `SPENDING_SECRET_KEY`
- `DB_PASSWORD`
- `INTERNAL_SYNC_SECRET`

## Performance Tuning
The database is performance-tuned for the `t4g.small` instance (2GB RAM). Ensure `postgresql.conf` in the `/deploy` folder is used.

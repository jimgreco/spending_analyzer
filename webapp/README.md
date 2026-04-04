# Spending Dashboard

A personal spending tracker with drag-and-drop statement import, smart categorization, and PostgreSQL persistence.

**Supports:** Chase (Amazon Prime Visa) · Apple Card · Citi · Coinbase · Bank of America

## Local Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Start a local PostgreSQL (if you have Docker):
docker run -d --name pg -e POSTGRES_DB=spending \
  -e POSTGRES_USER=spending -e POSTGRES_PASSWORD=spending \
  -p 5432:5432 postgres:16-alpine

# Run the app
DATABASE_URL=postgresql://spending:spending@localhost/spending python app.py
```

Open http://localhost:8000

## Deployment (Consolidated EC2)

This app is deployed as part of a consolidated Docker environment on EC2.

### Auto-Deployment
Pushes to the `main` branch automatically deploy to the EC2 instance via GitHub Actions.
- **Workflow**: `.github/workflows/deploy.yml`
- **Mechanism**: `rsync` syncs the `webapp/` folder → `docker-compose up -d --build spending`.

## Usage

1. **Drop files** anywhere on the page (or click "Upload Statements")
2. Supported: PDF exports from Chase, Apple Card, Citi, Coinbase, BofA + their CSV exports
3. Files are **deduplicated** automatically — safe to re-upload
4. **Click any category pill** to recategorize — future similar transactions will match automatically
5. **Click a donut slice** to filter the table to that category

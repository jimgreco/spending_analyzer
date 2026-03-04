# Spending Dashboard

A personal spending tracker with drag-and-drop statement import, smart categorization, and PostgreSQL persistence.

**Supports:** Chase (Amazon Prime Visa) · Apple Card · Citi · Coinbase · Bank of America

---

## Deploy to Railway (recommended)

Railway gives you a free PostgreSQL database and hosting.

### 1 – Push code to GitHub

```bash
cd spending-dashboard       # this folder
git init
git add .
git commit -m "initial commit"
# Create a repo on github.com, then:
git remote add origin https://github.com/YOUR_USER/spending-dashboard.git
git push -u origin main
```

### 2 – Create Railway project

1. Go to [railway.app](https://railway.app) → **New Project**
2. Choose **Deploy from GitHub repo** → select your repo
3. Railway will auto-detect Python and deploy

### 3 – Add PostgreSQL

1. In your Railway project → **+ New** → **Database** → **Add PostgreSQL**
2. Railway automatically injects `DATABASE_URL` into your app — nothing else needed

### 4 – Open your app

Click the generated URL (e.g. `https://your-app.up.railway.app`)

---

## Run locally

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

---

## Other cloud options

The same `Procfile` works on:
- **Heroku** – `heroku create && git push heroku main`, add Heroku Postgres addon
- **Render** – connect GitHub repo, add PostgreSQL service, set `DATABASE_URL`
- **Fly.io** – `fly launch`, add `fly postgres create`

---

## Usage

1. **Drop files** anywhere on the page (or click "Upload Statements")
2. Supported: PDF exports from Chase, Apple Card, Citi, Coinbase, BofA + their CSV exports
3. Files are **deduplicated** automatically — safe to re-upload
4. **Click any category pill** to recategorize — future similar transactions will match automatically
5. **Click a donut slice** to filter the table to that category

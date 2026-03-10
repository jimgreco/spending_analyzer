# Spending Dashboard — CLAUDE.md

## Project Overview

A full-stack personal finance tracker. Import bank statements (PDF/CSV), auto-categorize transactions with AI + fuzzy matching, tag transactions with free-form global tags, view spending by category/tag/month, and share read/edit access with other users.

---

## Directory Structure

```
spending/
├── .env                    # Secrets (DATABASE_URL, API keys, OAuth creds) — not committed
├── samples/                # Sample statement files for testing
└── webapp/
    ├── app.py              # FastAPI backend (~1775 lines, single file)
    ├── index.html          # Single-page frontend — all HTML/CSS/JS inline (~2343 lines)
    ├── requirements.txt
    ├── runtime.txt         # python-3.11.9 (used by EBS)
    ├── run.sh              # Local dev start script (not deployed)
    ├── Procfile            # Deployment: web: uvicorn app:app --host 0.0.0.0 --port $PORT
    └── .elasticbeanstalk/  # EBS CLI config (not committed) — deploy from webapp/ dir
```

---

## Running Locally

```bash
# Postgres is already running in Docker as 'spending-postgres' on port 5434
# (separate from macro_tracker_db which uses 5432)

# .env (at project root)
LOCAL_DEV=true
DATABASE_URL=postgresql://spending:spending@localhost:5434/spending
OPENAI_API_KEY=sk-...   # Optional — falls back to "Other" if absent
OPENAI_MODEL=gpt-4.1-mini

# Run
cd webapp && pip install -r requirements.txt
python app.py           # or: uvicorn app:app --reload --port 8000
```

`LOCAL_DEV=true` bypasses Google OAuth, creates a local test user, and auto-seeds categories. Open `http://localhost:8000`.

**Claude Code launch config** (`.claude/launch.json`):
```json
{
  "configurations": [{
    "name": "spending-webapp",
    "runtimeExecutable": "/bin/bash",
    "runtimeArgs": ["-c", "cd /Users/jgreco/code/spending/webapp && python3 -m uvicorn app:app --reload --port 8000"],
    "port": 8000
  }]
}
```

---

## Deployment

### GitHub
```bash
git add webapp/app.py webapp/index.html CLAUDE.md   # (etc.)
git commit -m "message"
git push origin main   # remote: git@github.com:jimgreco/spending_analyzer.git
```

### Elastic Beanstalk (production)
Deployed from the `webapp/` subdirectory using the EB CLI.

- **App:** `spending-analyzer`
- **Environment:** `spending-analyzer-prod`
- **Region:** `us-east-2`
- **Platform:** Python 3.11 on Amazon Linux 2023
- **URL:** `spending-analyzer-prod.eba-6xkawn4m.us-east-2.elasticbeanstalk.com`

```bash
cd webapp
eb deploy                   # deploy current code to spending-analyzer-prod
eb status                   # check environment health
eb logs                     # tail logs
```

Environment variables (set via EBS console or `eb setenv`):
- `DATABASE_URL` — RDS PostgreSQL connection string
- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `GOOGLE_CALLBACK_URL`
- `OWNER_EMAIL` — the single account owner; invited users share this owner's data
- `SECRET_KEY` — itsdangerous session signing key

---

## Tech Stack

| Layer | Tech |
|-------|------|
| Backend | FastAPI + Uvicorn |
| Database | PostgreSQL via psycopg2 (connection pool) |
| Auth | Google OAuth 2.0 (or local dev bypass) |
| Sessions | itsdangerous signed cookies (30-day) |
| PDF parsing | pdfplumber |
| CSV parsing | pandas |
| AI categorization | OpenAI API (gpt-4.1-mini) |
| Frontend | Vanilla JS + inline CSS, no frameworks |
| Charts | Chart.js 4.4.1 |

---

## Database Schema

| Table | Key Columns |
|-------|-------------|
| `users` | `id`, `google_id` UNIQUE, `email`, `name` |
| `transactions` | `id`, `user_id`, `date`, `description`, `category`, `amount`, `source`, `status` ('active'/'deleted'/'deduped'), `dedup_key`, `manually_corrected`, `import_file` |
| `categories` | `id`, `user_id`, `name`, `excluded_from_spending` |
| `tags` | `id`, `user_id`, `name` — UNIQUE(user_id, name) |
| `transaction_tags` | `transaction_id`, `tag_id` — PK(transaction_id, tag_id) |
| `uploaded_files` | `id`, `user_id`, `filename`, `file_hash`, `source`, `card_last4`, `tx_new`, `tx_dupes` |
| `invited_users` | `id`, `email` UNIQUE, `role` ('read'/'edit') |

**Migrations** live at the bottom of `app.py` as a list of `(name, sql)` tuples. Each runs in its own transaction on startup — failures are logged and skipped, never blocking.

**Tags** are global per user (not per-category). A transaction can have zero or more tags (many-to-many via `transaction_tags`). Tag names are unique per user.

**Multi-user model:** `OWNER_EMAIL` identifies the owner. Invited users (`invited_users`) always query the owner's `user_id` — they see and edit the same dataset.

---

## API Endpoints

### Transactions
| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/transactions` | Paginated. Params: `page`, `per_page`, `source`, `category`, `tag`, `search`, `date_from`, `date_to`, `card_last4`, `import_file`, `sort_by`, `sort_dir`, `status`. Returns `tags` array per transaction. |
| PATCH | `/api/transactions/{id}` | Update category/source on a single tx |
| PATCH | `/api/transactions` | Bulk update — body: `{ids, category?}` |
| DELETE | `/api/transactions/{id}` | Soft-delete (status → 'deleted') |
| POST | `/api/transactions/bulk-delete` | Bulk soft-delete |
| POST | `/api/transactions/{id}/restore` | Restore soft-deleted |
| POST | `/api/transactions/bulk-restore` | Bulk restore |
| PUT | `/api/transactions/{id}/tags` | Replace all tags on a transaction — body: `{tags: [name, ...]}` |
| POST | `/api/transactions/bulk-tag` | Add or remove a tag from many transactions — body: `{ids, tag, action: "add"\|"remove"}` |

### Stats
| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/stats` | Returns `total`, `count`, `by_category`, `by_month`, `by_source`, `by_tag`. Same filter params as transactions. |

### Categories
| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/categories` | List all for user |
| POST | `/api/categories` | Create — body: `{name}` |
| PATCH | `/api/categories` | Rename — body: `{old_name, new_name}` (cascades to transactions) |
| PATCH | `/api/categories/exclusion` | Toggle excluded_from_spending — body: `{name, excluded}` |
| DELETE | `/api/categories?name=` | Delete (reassigns transactions to 'Other') |

### Tags
| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/tags` | List all tag names for user |
| POST | `/api/tags` | Create — body: `{name}` |
| PATCH | `/api/tags` | Rename — body: `{old_name, new_name}` |
| DELETE | `/api/tags?name=` | Delete tag (cascades — removed from all transactions) |

### Uploads
| Method | Path | Notes |
|--------|------|-------|
| POST | `/api/upload` | Multipart — parses PDF/CSV, dedupes, categorizes, returns `{new, dupes}` |
| GET | `/api/uploads` | List uploaded files |
| DELETE | `/api/uploads?filename=` | Remove upload record |
| PATCH | `/api/uploads` | Rename display name |
| PATCH | `/api/uploads/source` | Change source label |
| PATCH | `/api/uploads/card-last4` | Set card last 4 digits |

---

## Frontend Architecture

**Single-file SPA** (`index.html`). No build step. All state is global.

### Key State Variables
```js
let categories   = [];          // [{name, excluded_from_spending}]
let tags         = [];          // [name, ...] — global tag names
let currentTxs   = [];          // transactions currently rendered in table
let selectedIds  = new Set();   // bulk-selected transaction IDs
let catFilter    = '';          // set by category donut click
let tagFilter    = '';          // set by tag bar chart click
let monthFilter  = '';          // set by monthly bar chart click
let viewStatus   = 'active';    // 'active' | 'deleted' | 'deduped'
let donutChart, barChart, tagChart;  // Chart.js instances (destroyed/recreated on each render)
```

### Data Flow
```
checkAuth() → loadCategories() + loadTags() + loadStats() + loadTransactions()
                                    ↓                              ↓
                            renderCharts(stats)            renderTable(txs)
```

Any filter change calls `applyFilters()` → `loadStats()` + `loadTransactions()`.

`buildFilterParams()` assembles all active filters into URL params. It merges chart-driven state (`catFilter`, `tagFilter`, `monthFilter`) with form dropdown values.

### Charts (3-column grid, left → right)
1. **By Category** — doughnut chart; click slice → sets `catFilter`
2. **By Tag** — horizontal bar chart (total spent per tag); click bar → sets `tagFilter`; shows "No tags assigned yet" when empty
3. **Monthly Spending** — vertical bar chart; click bar → sets `monthFilter` + fills date range inputs

Each chart shows a "(filtered – click to clear)" link in its header when its own filter is active (`catFilter`, `tagFilter`, `monthFilter` respectively). `updateFilterBadges()` sets all three in one call.

### Inline Editing
- **Category pill** click → `startEdit(pill)` → `<input list="cat-datalist">` with autocomplete. Typing a new name shows "✦ New category will be created" hint. On commit: creates category if new, then PATCHes transaction.
- **Tags cell** — shows tag chips with `×` to remove. `+` button → `startTagAdd()` → inline text input with custom autocomplete dropdown (`attachTagAutocomplete()`). Existing tags on the transaction are excluded from suggestions.

### Tag Autocomplete
`attachTagAutocomplete(inp, { onPick, exclude })` — shared helper that attaches a custom `position:fixed` styled dropdown to any input. Uses the global `tags[]` array. Suppresses browser autocomplete via `autocomplete="off"` + randomized `name` attribute. Supports keyboard navigation (↑↓ arrows, Enter, Escape). Used on:
- Inline `+` tag inputs on transaction rows
- `#bulk-tag-input` in the bulk action bar

### Bulk Action Bar
Fixed to bottom of viewport, visible when `selectedIds.size > 0`. Contains:
- **Category select + Apply** → `applyBulkCategory()`
- **Tag input + `+ Tag` / `− Tag` buttons** → `applyBulkTag('add'|'remove')` — calls `POST /api/transactions/bulk-tag`
- **Delete / Restore / Clear** buttons

### Modals
- **⚙ Categories** — `openCatModal()` — add, rename, delete, toggle exclusion
- **⚙ Tags** — `openTagModal()` — add, rename (`PATCH /api/tags`), delete tags

---

## Key Patterns & Conventions

### Auth / Permissions
Three dependency levels in `app.py`:
- `get_current_user` — any authenticated user
- `require_edit` — must have role 'edit' (or be owner)
- `require_owner` — must be the OWNER_EMAIL account

### Categorization Pipeline (on upload)
1. Exact description match in historical transactions
2. Fuzzy match (SequenceMatcher ≥0.75) on `manually_corrected=TRUE` transactions
3. Fuzzy match (≥0.85) on all active transactions
4. Batch GPT call (gpt-4.1-mini) for remaining uncategorized
5. Falls back to "Other" if GPT unavailable

### Deduplication
`dedup_key = MD5(date|source|amount|normalized_description|seq)`. On upload, any transaction whose key already exists in the DB is inserted with `status='deduped'` rather than 'active'.

### Tag Full-Replace Pattern
`PUT /api/transactions/{id}/tags` uses a full-replace: delete all existing `transaction_tags` for the tx, then re-insert. Tag names are upserted into `tags` with `ON CONFLICT DO NOTHING` to get their IDs.

### CSS Variables (dark theme)
`--bg`, `--bg-soft`, `--surface`, `--border`, `--text`, `--muted`, `--accent` (#6366f1 indigo), `--accent2` (#22d3ee cyan), `--red`, `--green`

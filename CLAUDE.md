# Spending Dashboard — CLAUDE.md

## Project Overview

A full-stack personal finance tracker. Import bank statements (PDF/CSV), auto-tag transactions with AI, tag transactions with free-form global tags, view spending by tag/month, and share read/edit access with other users.

---

## Directory Structure

```
spending/
├── .env                    # Secrets (DATABASE_URL, API keys, OAuth creds) — not committed
├── samples/                # Sample statement files for testing
└── webapp/
    ├── app.py              # FastAPI backend (~1000 lines, single file)
    ├── index.html          # Single-page frontend — all HTML/CSS/JS inline (~2000 lines)
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
OPENAI_API_KEY=sk-...   # Optional — falls back to no tags if absent
OPENAI_MODEL=gpt-4.1-mini

# Run
cd webapp && pip install -r requirements.txt
python app.py           # or: uvicorn app:app --reload --port 8000
```

`LOCAL_DEV=true` bypasses Google OAuth and creates a local test user. Open `http://localhost:8000`.

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
| PDF/CSV parsing | GPT-only (pdfplumber for text extraction, no hand-coded parsers) |
| AI tag assignment | OpenAI API (gpt-4.1-mini) |
| Frontend | Vanilla JS + inline CSS, no frameworks |
| Charts | Chart.js 4.4.1 |

---

## Database Schema

| Table | Key Columns |
|-------|-------------|
| `users` | `id`, `google_id` UNIQUE, `email`, `name` |
| `transactions` | `id`, `user_id`, `date`, `description`, `amount`, `source`, `status` ('active'/'deleted'/'deduped'), `dedup_key`, `import_file` |
| `tags` | `id`, `user_id`, `name`, `excluded_from_spending` — UNIQUE(user_id, name) |
| `transaction_tags` | `transaction_id`, `tag_id` — PK(transaction_id, tag_id) |
| `uploaded_files` | `id`, `user_id`, `filename`, `file_hash`, `source`, `card_last4`, `tx_new`, `tx_dupes` |
| `invited_users` | `id`, `email` UNIQUE, `role` ('read'/'edit') |

**Migrations** live at the bottom of `app.py` as a list of `(name, sql)` tuples. Each runs in its own transaction on startup — failures are logged and skipped, never blocking.

**Tags** are global per user. A transaction can have zero or more tags (many-to-many via `transaction_tags`). Tag names are unique per user. Tags have an `excluded_from_spending` boolean — transactions are excluded from spending totals if any of their tags are marked excluded; excluded tags are also hidden from the "By Tag" chips.

**Multi-user model:** `OWNER_EMAIL` identifies the owner. Invited users (`invited_users`) always query the owner's `user_id` — they see and edit the same dataset.

---

## API Endpoints

### Transactions
| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/transactions` | Paginated. Params: `page`, `per_page`, `source`, `tag`, `tag_match` ('any'/'all'), `search`, `date_from`, `date_to`, `card_last4`, `import_file`, `sort_by`, `sort_dir`, `status`. Returns `tags` array per transaction. |
| PATCH | `/api/transactions/{id}` | Update `source` on a single tx |
| DELETE | `/api/transactions/{id}` | Soft-delete (status → 'deleted') |
| POST | `/api/transactions/bulk-delete` | Bulk soft-delete |
| POST | `/api/transactions/{id}/restore` | Restore soft-deleted |
| POST | `/api/transactions/bulk-restore` | Bulk restore |
| PUT | `/api/transactions/{id}/tags` | Replace all tags on a transaction — body: `{tags: [name, ...]}` |
| POST | `/api/transactions/bulk-tag` | Add or remove a tag from many transactions — body: `{ids, tag, action: "add"\|"remove"}` |

### Stats
| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/stats` | Returns `total`, `count`, `by_month`, `by_source`, `by_tag`. Same filter params as transactions. Transactions with any excluded tag are omitted from totals. `by_tag` only includes non-excluded tags. |

### Tags
| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/tags` | Returns `{tags: [{name, excluded_from_spending}]}` |
| POST | `/api/tags` | Create — body: `{name}` |
| PATCH | `/api/tags` | Rename — body: `{old_name, new_name}` |
| PATCH | `/api/tags/exclusion` | Toggle excluded_from_spending — body: `{name, excluded}` |
| DELETE | `/api/tags?name=` | Delete tag (cascades — removed from all transactions) |

### Uploads
| Method | Path | Notes |
|--------|------|-------|
| POST | `/api/upload` | Multipart — parses PDF/CSV, dedupes, assigns tags, returns `{new, dupes}` |
| GET | `/api/uploads` | List uploaded files |
| DELETE | `/api/uploads?filename=` | Remove upload record |
| PATCH | `/api/uploads` | Rename display name |
| PATCH | `/api/uploads/source` | Change source label |
| PATCH | `/api/uploads/card-last4` | Set card last 4 digits |

### Misc
| Method | Path | Notes |
|--------|------|-------|
| GET | `/api/version` | Returns `{sha, timestamp}` from git at startup |

---

## Frontend Architecture

**Single-file SPA** (`index.html`). No build step. All state is global.

### Key State Variables
```js
let tags         = [];          // [{name, excluded_from_spending}] — global tag list
let allSources   = [];          // sorted alphabetically; drives sourceColor()
let currentTxs   = [];          // transactions currently rendered in table
let selectedIds  = new Set();   // bulk-selected transaction IDs
let tagFilters   = new Set();   // active tag filters (multi-select)
let tagMatchMode = 'any';       // 'any' (OR) or 'all' (AND)
let monthFilter  = '';          // set by monthly bar chart click
let viewStatus   = 'active';    // 'active' | 'deleted' | 'deduped'
let barChart     = null;        // Chart.js instance (destroyed/recreated on each render)
```

### Data Flow
```
checkAuth() → loadTags() + loadStats() + loadTransactions()
                   ↓                           ↓
          renderCharts(stats)           renderTable(txs)
```

Any filter change calls `applyFilters()` → `loadStats()` + `loadTransactions()`.

`buildFilterParams()` assembles all active filters into URL params. It merges `tagFilters` (Set), `tagMatchMode`, and `monthFilter` with form dropdown values.

### Charts (2-column grid, left → right)
1. **By Tag** — clickable tag chips (total spent per non-excluded tag); clicking toggles that tag in `tagFilters`. Shows a "(filtered – click to clear)" link when any tag filter is active.
2. **Monthly Spending** — vertical bar chart; click bar → sets `monthFilter` + fills date range inputs. Shows "(filtered – click to clear)" when active.

`updateFilterBadges()` syncs both filter indicators and the ANY/ALL toggle button visibility in one call.

### Inline Editing
- **Tags cell** — shows tag chips with `×` to remove. `+` button → `startTagAdd()` → inline text input with custom autocomplete dropdown (`attachTagAutocomplete()`). Existing tags on the transaction are excluded from suggestions.

### Tag Autocomplete
`attachTagAutocomplete(inp, { onPick, exclude })` — shared helper that attaches a custom `position:fixed` styled dropdown to any input. Uses `tags[].name` from the global `tags` array. Suppresses browser autocomplete via `autocomplete="off"` + randomized `name` attribute. Supports keyboard navigation (↑↓ arrows, Enter, Escape). Used on:
- Inline `+` tag inputs on transaction rows
- `#bulk-tag-input` in the bulk action bar

### Bulk Action Bar
Fixed to bottom of viewport, visible when `selectedIds.size > 0`. Contains:
- **Tag input + `+ Tag` / `− Tag` buttons** → `applyBulkTag('add'|'remove')` — calls `POST /api/transactions/bulk-tag`
- **Delete / Restore / Clear** buttons

### Modals
- **⚙ Tags** — `openTagModal()` — add, rename, delete, toggle `excluded_from_spending` per tag

### Version Footer
`GET /api/version` is fetched on boot; populates `#version-footer` with `<sha> — <timestamp>`.

---

## Key Patterns & Conventions

### Auth / Permissions
Three dependency levels in `app.py`:
- `get_current_user` — any authenticated user
- `require_edit` — must have role 'edit' (or be owner)
- `require_owner` — must be the OWNER_EMAIL account

### Description Cleaning
`clean_description(desc)` is applied to every parsed row before storing. Strips payment-method prefixes that add no useful information:
- `AplPay` (Apple Pay) — anywhere in the description
- `SP ` (Square) — leading prefix
- `*TST*` / `TST*` / `*TST` (Toast POS) — leading prefix

DB migrations run on startup to retroactively clean existing records.

### File Parsing Pipeline
`parse_file_bytes(content, filename)` — GPT-only, no hand-coded parsers:
1. PDF: extract text with pdfplumber page by page
2. CSV: decode as UTF-8
3. If text > 30,000 chars: chunk by pages (PDF) or rows (CSV, repeating header); each chunk sent separately
4. `parse_with_gpt()` sends each chunk to gpt-4.1-mini with `max_tokens=32000`, `response_format=json_object`
5. Returns `{date, description, amount}` rows; source detected via `detect_source(text)` regex
6. `clean_description()` applied to all rows before GPT tag assignment and DB insert

### Tag Assignment Pipeline (on upload)
GPT assigns zero or more tags per transaction from the user's existing tag list:
1. `assign_tags_with_gpt(descriptions, tag_list)` — batch GPT call (gpt-4.1-mini); returns `{description: [tag, ...]}` map
2. If no tags exist or GPT is unavailable, transactions are imported with no tags
3. After bulk insert (via `execute_values` with `RETURNING id`), tags are upserted and `transaction_tags` rows inserted

### Tag Exclusion
A transaction is excluded from spending totals if **any** of its tags has `excluded_from_spending = TRUE`. This is enforced via a `NOT IN` subquery on `transaction_tags` in both `/api/stats` and `/api/transactions`. Excluded tags are also filtered out of the `by_tag` stats results.

### Deduplication
`dedup_key = MD5(date|source|amount|normalized_description|seq)`. On upload, any transaction whose key already exists in the DB is inserted with `status='deduped'` rather than 'active'.

### Tag Full-Replace Pattern
`PUT /api/transactions/{id}/tags` uses a full-replace: delete all existing `transaction_tags` for the tx, then re-insert. Tag names are upserted into `tags` with `ON CONFLICT DO NOTHING` to get their IDs.

### Source (Card) Chip Colors
Card/issuer chips are color-coded dynamically — no hard-coded color map. `allSources` is populated (sorted alphabetically) in `renderUploadHistory()`. `sourceColor(src)` returns a color from `CAT_PALETTE` by alphabetical index. `contrastText(hex)` auto-picks black or white text for readability.

### CSS Variables (dark theme)
`--bg`, `--bg-soft`, `--surface`, `--border`, `--text`, `--muted`, `--accent` (#6366f1 indigo), `--accent2` (#22d3ee cyan), `--red`, `--green`

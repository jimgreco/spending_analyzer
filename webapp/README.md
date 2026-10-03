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

## AI model configuration

- `OPENAI_MODEL` controls PDF/CSV transaction extraction (default: `gpt-4o-mini`).
- `OPENAI_TAG_MODEL` controls AI categorization (default: `gpt-6-astra`). A blank
  value also uses the default. It is independent of `OPENAI_MODEL`, so an existing
  extraction-model override does not keep categorization on the older model.
- Categorization uses Chat Completions JSON mode with `reasoning_effort="low"`
  and a 16,384-token completion budget, including reasoning. Model overrides must
  support these options. Unsupported sampling parameters are omitted.

The default follows the current [OpenAI production-model recommendation](https://developers.openai.com/api/docs/models/chat-latest)
and [GPT-6 migration guidance](https://developers.openai.com/api/docs/guides/latest-model).
It is an explicit model ID, so future model releases require a deliberate update.
GPT-6 Astra costs more per token than the prior mini models. Categorization now
sends one decision request per transaction in batches of 20 (up to three concurrent
requests), with the guide and up to six relevant manual examples per row. It uses
no direct merchant-history bypass. The upgrade applies to new imports and does
not recategorize existing transactions.

## Personal categorization

- **Categorization guide** in the header stores category definitions and exceptions
  in plain language. It is shared by the dataset's owner and editors, readable by
  invited viewers, limited to 12,000 characters, and applies to future imports.
- **Edit category / Review** lets you choose a primary category, optionally add a
  note (1,000 characters), and choose **Just this transaction** (the default for a
  new correction) or **Use for similar transactions**. Reusable corrections are
  examples, not unconditional merchant rules. Bulk primary edits offer the same
  scope choice. Clearing all tags is a one-time correction.
- **Category rules** in the header opens a searchable management page for reusable
  examples, legacy manual examples with unknown original scope, one-time
  corrections, and archived corrections. Editing uses the same category, scope,
  and note controls as a transaction correction. An edit changes its source
  transaction and can change which examples are used on future imports; it does
  not recategorize other existing transactions. Archiving stops a reusable or
  legacy example from influencing future imports and hides any correction from
  the active lists, while keeping the source transaction and category intact.
  Archived corrections can be restored. Read-only invitees can view the page.
- Only active, manually corrected primary categories are eligible examples.
  Earlier manual corrections have unknown scope and remain available as legacy
  evidence. One-time corrections and automatic labels are never training examples;
  secondary-tag changes are not primary-category training events.
- Retrieval considers merchant similarity, amount, charge/refund sign, source and
  recency among the most recent 5,000 eligible corrections for the dataset. The
  prompt includes dates, descriptions, amounts, sources, chosen categories and
  notes. The guide and selected examples are sent to OpenAI when importing.
- **Needs review** lists uncertain or failed categorizations with a suggested
  category (when available) and a short reason. Pending suggestions do not become
  primary tags until a human saves the correction. Conflicting close manual
  examples always require review. Broad merchants such as Amazon require a
  supporting explicit reusable example with a note before auto-assignment.
  Confidence is a model label, not a calibrated probability. Charts continue to
  show all active spending; the review tab filters the transaction table.
- Missing API access, invalid/truncated responses and missing categories keep new
  rows visible in review. Imports still succeed if only categorization fails.
  A deployment needs an API key with access to the selected model.

The migrations add `categorization_settings` plus correction scope/note, archive,
and review metadata on `transactions`. They are idempotent and do not rewrite
existing labels.

## Import reliability

- Apple Card and Coinbase One Card PDFs with recognized statement summaries are
  checked against their dated payment and transaction lines before saving. The
  check compares date, signed amount, repeated-row count, and printed section
  totals. Apple's undated Daily Cash Adjustment is included in its printed total
  check but is not imported as a dated transaction.
- If extraction omits dated rows, one focused extraction of those original lines
  runs, followed by the full check again. A truncated model response, failed
  chunk, invalid row, or unresolved mismatch saves no transactions. Other file
  formats still use the general extraction path without this statement-total
  check; verify their signs and totals before relying on an import.
- `GET /api/upload/jobs` lists the latest owner-scoped jobs and their stages;
  `?filename=` filters by the original upload name.
  The dashboard shows these after refresh. A heartbeat marks work interrupted
  after two minutes without an update. The uploaded bytes are not stored, so
  retry by selecting the same file. Errors and interrupted jobs stay visible.
- `Reimport` reparses an existing file and adds only missing rows. Rows already
  attributed to that file keep their categories and other edits. Concurrent
  uploads of the same file serialize their final database write. A normal retry
  of an already imported file reports `already_imported`. The Reimport button
  verifies that the selected bytes match its upload record.
`GET/PUT /api/categorization-guide` manage guidance; the primary-tag and bulk-tag
APIs accept `correction_scope` (`transaction` or `similar`) and `correction_note`.
`GET /api/categorization-corrections` lists active transaction corrections by
`kind` (`reusable`, `one-time`, or `archived`) with search and pagination.
`DELETE /api/categorization-corrections/{id}` archives one correction and
`POST /api/categorization-corrections/{id}/restore` restores it; both require edit
access and enforce dataset ownership.
`GET /api/transactions?status=review` returns active pending rows.

## Verification

From the repository root, run the synthetic unit checks (no database or live API):

```bash
python3 -m unittest discover -s webapp -p 'test_tag_model.py' -v
```

The optional API/import integration suite needs a **disposable local PostgreSQL
DB** whose name ends in `_test`. It clears test tables; never use a real dataset.
It mocks the parser and OpenAI transport and never reads statement files.
The synthetic statement unit tests also exercise the Apple refund, Coinbase
payment, repeat-charge, total mismatch, and failed-recovery paths.

```bash
SPENDING_TEST_DATABASE_URL='postgresql://user@localhost/spending_test' \
  python3 -m unittest discover -s webapp -p 'test_*.py' -v
```

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
4. **Edit category** to correct a primary tag and decide whether future imports should learn from it
5. **Click a donut slice** to filter the table to that category

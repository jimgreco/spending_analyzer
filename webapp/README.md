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
`GET/PUT /api/categorization-guide` manage guidance; the primary-tag and bulk-tag
APIs accept `correction_scope` (`transaction` or `similar`) and `correction_note`.
`GET /api/categorization-corrections` lists active transaction corrections by
`kind` (`reusable`, `one-time`, or `archived`) with search and pagination.
`DELETE /api/categorization-corrections/{id}` archives one correction and
`POST /api/categorization-corrections/{id}/restore` restores it; both require edit
access and enforce dataset ownership.
`GET /api/transactions?status=review` returns active pending rows.

## Transaction types and financial-flow analytics

`transaction_type` is separate from the primary/secondary category tags. It is
nullable and accepts `expense`, `income`, `transfer`, or `refund`. Fees remain
expenses whose category describes the fee. Existing rows and new imports stay
untyped until a person reviews them. The additive migration also adds
`type_revision` (initially 0) and `type_updated_at` (initially null); it does not
classify historical rows, change amounts, or modify `/api/stats`. The current
dashboard total therefore stays on its existing basis until a future reviewed
product change.

- `GET /api/transactions` adds `transaction_type`, `type_revision`,
  `type_updated_at`, and `type_sign_issue` on each row. Its optional
  `transaction_type` filter accepts the four types or `unreviewed` (null).
- `PUT /api/transactions/{id}/type` requires edit access and
  `{ "transaction_type": "refund", "expected_revision": 0 }`. Send null to clear
  a type. It returns the previous and new types, new revision, timestamp, and
  any sign warning. A stale revision returns 409; a missing/foreign transaction
  returns 404. No amount, category, exclusion, or review flag is changed.
- `GET /api/analytics` accepts the same source/tag/search/date/import/card filters
  as `/api/stats`, plus the optional type filter. `legacy_total` is the unchanged
  signed total for active rows with no excluded primary category or ancestor.
  The reviewed subset reports `gross_charges` (positive expenses), `refunds`
  (absolute value of negative refunds), and `net_spend = gross_charges - refunds`.
  It also reports income credits, signed transfer total, untyped signed total,
  sign-conflict signed total, exclusions, and category/type/sign review counts.
  `basis: reviewed_types_only` and `untyped_count` make incomplete coverage
  explicit. For the same filters, `legacy_total = net_spend - income_credits +
  transfer_net + untyped_signed_total + ambiguous_sign_signed_total`.
- `GET /api/transaction-type-preview?limit=50&offset=0` is a read-only historical
  mapping preview, capped at 100 sample rows per request. It reports all active
  untyped rows, candidate counts, ambiguous reasons, and sample suggestions.
  Negative credits, reimbursement tags, zero amounts, and excluded categories
  without a direct transfer hint remain ambiguous. Every suggestion needs human
  review; there is no bulk apply or historical repair endpoint.

Stored amounts keep the existing sign convention: charges positive, credits
negative. A reviewed expense with a negative amount, refund/income with a positive
amount, or any zero amount has a `type_sign_issue`. The explicit type remains
saved, but that row is omitted from reviewed financial-flow totals and counted
for review. Transfers may have either sign. Excluded primary categories and
ancestors follow `/api/stats` semantics; secondary tags alone do not exclude.

**Future backfill/rollback design:** Before any separately approved mapping,
persist a scoped snapshot of transaction ID, owner ID, status, signed amount,
old type, and type revision. Show the preview and reconcile old `/api/stats` totals
against the new typed subset. Apply only reviewed IDs with compare-and-swap
revisions; record each resulting revision. An inverse pass may restore only rows
whose revisions still match the recorded result, leaving later manual edits for
review. No backfill, snapshot table, or inverse pass is executed by this slice.

## Verification

From the repository root, run the synthetic unit checks (no database or live API):

```bash
python3 -m unittest discover -s webapp -p 'test_tag_model.py' -v
```

The optional API/import integration suite needs a **disposable local PostgreSQL
DB** whose name ends in `_test`. It clears test tables; never use a real dataset.
It mocks the parser and OpenAI transport and never reads statement files.

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

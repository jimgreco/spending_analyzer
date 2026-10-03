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
access and enforce dataset ownership. List rows include `correction_revision`.
The rules manager sends that value as `expected_revision` on archive/restore,
or `expected_correction_revision` on primary-category edit. A stale manager
action returns HTTP 409 and the page refreshes the list before another edit.
`GET /api/transactions?status=review` returns active pending rows.

## Import reliability

- Recognized Apple Card, Coinbase One Card, Bank of America, Citi, and Chase
  Amazon PDFs are checked against their dated account-activity lines before
  saving. The check compares date, signed amount, repeated-row count, and a
  printed balance or section total when readable. BofA card transaction and
  posting dates are both accepted. Apple's undated Daily Cash Adjustment is
  included in its printed total check but is not imported as a dated row.
  Chase's informational Shop with Points activity and Apple's installment
  financing summaries are outside the current account-activity ledger.
- If extraction omits dated rows, one focused extraction of those original lines
  runs, followed by the full check again. A truncated model response, failed
  chunk, invalid row, or unresolved mismatch saves no transactions. Other file
  formats still use the general extraction path without this statement-total
  check; verify their signs and totals before relying on an import.
- New BofA PDF imports use the account suffix printed on the statement in their
  deduplication keys and upload metadata. If the suffix cannot be verified,
  import stops. An older matching key without verified account identity is kept
  active and flagged for review; it is not silently collapsed across accounts.
  Existing same-file rows still match their legacy keys on forced reimport.
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

## Correct a transaction sign

An editor can open **Sign…** on one active transaction, inspect its exact
before → after amount, and confirm **Reverse sign**. The dialog shows the imported
amount, reviewed type warning, recent history, and **Undo last reversal** when the
latest amount change is that reversal. Viewers can inspect history but cannot
change it. A deleted or deduped transaction cannot be changed until restored;
zero has no sign to reverse. Archiving a category correction has no effect on the
sign action. A sign change never changes category, reviewed type, or exclusion.

`transactions.amount` remains the effective signed amount used by existing stats,
analytics, filters, and type warnings. `original_amount` stores the signed amount
received at import (backfilled once from existing amounts), and
`amount_revision` increments only for sign corrections. A database trigger fills
and then protects `original_amount` for every insert, including older import
code. The original `dedup_key` is never recomputed by a correction. Normal and
forced reimports use the parsed raw amount and stable import identity, so a
previously corrected row is skipped without losing its correction or creating a
second raw row.

API contract:

- `GET /api/transactions/{id}/amount` returns exact decimal strings for
  `amount`, `original_amount`, and `reverse_preview`, plus `amount_revision`,
  `transaction_type`, `type_sign_issue`, `can_undo`, and the 50 most recent audit
  changes. A foreign or missing ID returns 404.
- `POST /api/transactions/{id}/amount/reverse` accepts
  `{ "operation_id": "<UUID>", "expected_revision": 0, "expected_amount": "25.00" }`.
  It returns the audit change ID and original, before, and after amounts and
  revisions. A successful retry with the same operation ID returns that change
  with `replayed: true`; a new request from a stale preview returns 409.
- `POST /api/transactions/{id}/amount/undo` accepts the same fields plus
  `change_id` from the reversal. It succeeds only if that exact reversal is the
  latest amount change and the expected amount and revision still match. Its
  retry behavior is the same. Both writes require owner or editor access and
  reject zero or inactive rows. A later category or type edit remains intact.

`transaction_amount_changes` records the owner, actor, operation UUID, action,
target reversal for undo, immutable source amount, effective before/after
amounts, revisions, and timestamp. The migration is additive and idempotent;
it does not reverse any historical transaction. To roll back the app code, keep
the added columns and audit table: older code continues to read the effective
`amount`, and its import dedup key stays unchanged. A schema down migration
would remove audit history and is intentionally not automatic. Explicit import
deletion still removes its transactions and their related audit rows.

## Verification

From the repository root, run the synthetic unit checks (no database or live API):

```bash
python3 -m unittest discover -s webapp -p 'test_tag_model.py' -v
```

The optional API/import integration suite needs a **disposable local PostgreSQL
DB** whose name ends in `_test`. It clears test tables; never use a real dataset.
It mocks the parser and OpenAI transport and never reads statement files.
The synthetic statement unit tests exercise Apple, Coinbase, BofA, Citi, and
Chase refund, payment, credit-sign, repeated-row, total-mismatch, section,
and failed-recovery paths.

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

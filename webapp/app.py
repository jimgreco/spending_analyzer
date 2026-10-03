#!/usr/bin/env python3
"""
Spending Dashboard – FastAPI + PostgreSQL backend
Run locally: uvicorn app:app --reload
Deploy:      Railway / Heroku (DATABASE_URL env var auto-injected)

Auth:
  - Production:  Google OAuth 2.0  (GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET required)
  - Local dev:   Set LOCAL_DEV=true in .env to bypass OAuth and auto-login as a
                 local test user.  No Google credentials needed.
"""
import os, re, io, json, hashlib, secrets, uuid, threading, subprocess, math
from collections import Counter
from decimal import Decimal
from datetime import datetime, timedelta
from contextlib import contextmanager
from typing import List, Optional, Literal
from uuid import UUID
from urllib.parse import urlencode

import httpx
import pdfplumber
import pandas as pd
import psycopg2
import psycopg2.extras
import psycopg2.pool
import uvicorn
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from openai import OpenAI
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Response, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from categorization import SYSTEM_PROMPT, prepare_context, request_payload, validate_results, review
from import_checks import reconcile_card_statement, detect_account_key, StatementMismatch
from transaction_types import preview_candidate, sign_issue, summarize_ledger
from dotenv import load_dotenv

# ── Load .env (one level up from this file) ───────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

def _as_oauth_callback_url(value: str) -> str:
    value = value.strip().rstrip("/")
    if not value:
        return ""
    if value.endswith("/auth/google/callback"):
        return value
    return f"{value}/auth/google/callback"

# ── Configuration ────────────────────────────────────────────────────────────────
DATABASE_URL         = os.getenv("DATABASE_URL", "postgresql://spending:spending@localhost/spending")
PORT                 = int(os.getenv("PORT", "8000"))
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL         = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
# Categorization can advance independently of the statement extraction model.
OPENAI_TAG_MODEL     = os.getenv("OPENAI_TAG_MODEL", "").strip() or "gpt-6-astra"
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
SECRET_KEY           = os.getenv("SECRET_KEY", secrets.token_hex(32))
_ENV_APP_URL         = os.getenv("APP_URL", "").strip().rstrip("/")
_ENV_CALLBACK_URL    = os.getenv("GOOGLE_CALLBACK_URL", "").strip().rstrip("/")
APP_URL              = _ENV_APP_URL or _ENV_CALLBACK_URL.removesuffix("/auth/google/callback") or "http://localhost:8000"
GOOGLE_CALLBACK_URL  = _as_oauth_callback_url(_ENV_CALLBACK_URL) or f"{APP_URL}/auth/google/callback"
LOCAL_DEV            = os.getenv("LOCAL_DEV", "false").lower() in ("true", "1", "yes")
OWNER_EMAIL          = os.getenv("OWNER_EMAIL", "")

SESSION_MAX_AGE = 30 * 24 * 3600  # 30 days

if not LOCAL_DEV and os.getenv("SECRET_KEY") is None:
    print("[warn] SECRET_KEY not set — sessions will reset on every restart. Set it in .env.")

# ── Session helpers ───────────────────────────────────────────────────────────────
_signer = URLSafeTimedSerializer(SECRET_KEY)

def _sign_session(user_id: int) -> str:
    return _signer.dumps({"uid": user_id})

def _unsign_session(token: str) -> Optional[int]:
    try:
        data = _signer.loads(token, max_age=SESSION_MAX_AGE)
        return data["uid"]
    except (BadSignature, SignatureExpired, KeyError):
        return None

# ── Connection pool ───────────────────────────────────────────────────────────────
_pool: Optional[psycopg2.pool.SimpleConnectionPool] = None

def _get_pool():
    global _pool
    if _pool is None:
        _pool = psycopg2.pool.ThreadedConnectionPool(1, 10, DATABASE_URL)
    return _pool

@contextmanager
def db():
    conn = _get_pool().getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _get_pool().putconn(conn)

# ── Schema ────────────────────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    id         SERIAL      PRIMARY KEY,
    google_id  TEXT        UNIQUE,
    email      TEXT        UNIQUE NOT NULL,
    name       TEXT        NOT NULL DEFAULT '',
    picture    TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transactions (
    id                 SERIAL        PRIMARY KEY,
    user_id            INTEGER       REFERENCES users(id) ON DELETE CASCADE,
    date               DATE          NOT NULL,
    description        TEXT          NOT NULL,
    category           TEXT          NOT NULL DEFAULT 'Other',
    amount             NUMERIC(12,2) NOT NULL,
    original_amount    NUMERIC(12,2),
    amount_revision    INTEGER       NOT NULL DEFAULT 0,
    source             TEXT          NOT NULL,
    dedup_key          TEXT          NOT NULL,
    status             TEXT          NOT NULL DEFAULT 'active',
    dedup_of           TEXT,
    manually_corrected BOOLEAN       DEFAULT FALSE,
    import_file        TEXT,
    created_at         TIMESTAMPTZ   DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_tx_date   ON transactions(date DESC);
CREATE INDEX IF NOT EXISTS idx_tx_source ON transactions(source);
CREATE INDEX IF NOT EXISTS idx_tx_cat    ON transactions(category);

CREATE TABLE IF NOT EXISTS categories (
    id      SERIAL  PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    name    TEXT    NOT NULL,
    UNIQUE (user_id, name)
);

CREATE TABLE IF NOT EXISTS tags (
    id      SERIAL  PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    name    TEXT    NOT NULL,
    UNIQUE (user_id, name)
);

CREATE TABLE IF NOT EXISTS transaction_tags (
    transaction_id INTEGER REFERENCES transactions(id) ON DELETE CASCADE,
    tag_id         INTEGER REFERENCES tags(id) ON DELETE CASCADE,
    PRIMARY KEY (transaction_id, tag_id)
);

CREATE TABLE IF NOT EXISTS uploaded_files (
    id          SERIAL      PRIMARY KEY,
    user_id     INTEGER     REFERENCES users(id) ON DELETE CASCADE,
    filename    TEXT        NOT NULL,
    file_hash   TEXT        NOT NULL,
    source      TEXT,
    tx_new      INTEGER     DEFAULT 0,
    tx_dupes    INTEGER     DEFAULT 0,
    uploaded_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, file_hash)
);

CREATE TABLE IF NOT EXISTS invited_users (
    id           SERIAL      PRIMARY KEY,
    email        TEXT        UNIQUE NOT NULL,
    role         TEXT        NOT NULL DEFAULT 'read',
    invited_at   TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at TIMESTAMPTZ
);
"""

def _migrate_primary_tags():
    """One-time migration: assign primary_tag_id from transaction_tags data."""
    try:
        with db() as conn:
            with conn.cursor() as cur:
                # Check if migration already ran (any row has a status set)
                cur.execute("SELECT 1 FROM transactions WHERE primary_migration_status IS NOT NULL LIMIT 1")
                if cur.fetchone():
                    return  # already migrated

                # Check if there are any transactions at all
                cur.execute("SELECT 1 FROM transactions LIMIT 1")
                if not cur.fetchone():
                    return  # empty DB, nothing to migrate

                # Load all tags with hierarchy
                cur.execute("SELECT id, user_id, name, group_tag_id FROM tags")
                all_tags = {t[0]: {"id": t[0], "user_id": t[1], "name": t[2], "group_tag_id": t[3]}
                            for t in cur.fetchall()}
                parent_of = {tid: tag["group_tag_id"] for tid, tag in all_tags.items()}

                def ancestors(tid):
                    chain = set()
                    cur_id = parent_of.get(tid)
                    while cur_id:
                        chain.add(cur_id)
                        cur_id = parent_of.get(cur_id)
                    return chain

                def chain_depth(tid):
                    depth = 0
                    cur_id = parent_of.get(tid)
                    while cur_id:
                        depth += 1
                        cur_id = parent_of.get(cur_id)
                    return depth

                # Get all transaction-tag assignments
                cur.execute("""
                    SELECT t.id, array_agg(tt.tag_id) as tag_ids
                    FROM transactions t
                    JOIN transaction_tags tt ON tt.transaction_id = t.id
                    WHERE t.status = 'active'
                    GROUP BY t.id
                """)
                tx_tags = cur.fetchall()

                for (tx_id, tag_ids) in tx_tags:
                    tag_id_set = set(tag_ids)

                    # Build ancestor sets for each tag
                    tag_ancestors = {tid: ancestors(tid) for tid in tag_id_set}

                    # A tag is an "ancestor" if another tag on this tx has it in its ancestor chain
                    ancestor_ids = set()
                    for tid in tag_id_set:
                        ancestor_ids |= (tag_ancestors[tid] & tag_id_set)

                    leaves = tag_id_set - ancestor_ids

                    if len(leaves) == 0:
                        # All tags are ancestors of each other (shouldn't happen, but handle it)
                        primary_id = max(tag_id_set, key=chain_depth)
                        status = 'auto'
                    elif len(leaves) == 1:
                        primary_id = next(iter(leaves))
                        status = 'auto'
                    else:
                        # Multiple leaves — check if they share a chain
                        # Pick deepest by hierarchy depth; mark ambiguous if depths are equal
                        sorted_leaves = sorted(leaves, key=chain_depth, reverse=True)
                        primary_id = sorted_leaves[0]
                        if chain_depth(sorted_leaves[0]) == chain_depth(sorted_leaves[1]):
                            status = 'ambiguous'
                        else:
                            status = 'auto'

                    # Set primary tag
                    cur.execute(
                        "UPDATE transactions SET primary_tag_id=%s, primary_migration_status=%s WHERE id=%s",
                        (primary_id, status, tx_id))

                    # Remove primary tag and its ancestors from transaction_tags (they're now implicit)
                    remove_ids = {primary_id} | (tag_ancestors.get(primary_id, set()) & tag_id_set)
                    if remove_ids:
                        cur.execute(
                            "DELETE FROM transaction_tags WHERE transaction_id=%s AND tag_id = ANY(%s)",
                            (tx_id, list(remove_ids)))

                # Mark untagged transactions as auto-migrated
                cur.execute(
                    "UPDATE transactions SET primary_migration_status='auto' "
                    "WHERE primary_migration_status IS NULL")

        print("[migrate:primary-tags] Migration complete")
    except Exception as e:
        print(f"[migrate:primary-tags] {type(e).__name__}: {e}")


def init_db():
    # Base schema (safe for both fresh and existing DBs)
    with db() as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(SCHEMA)
            except Exception as e:
                # If it already exists, ignore common "already exists" errors during SERIAL creation
                print(f"[init_db] Note: {e}")
                conn.rollback()

    # Each migration in its own transaction — a failure in one doesn't block others.
    migrations = [
        # ── dedup / soft-delete migrations (from previous version) ───────────────
        ("drop dedup_key unique constraint", """
            DO $$
            DECLARE cname TEXT;
            BEGIN
                SELECT con.conname INTO cname
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                JOIN pg_attribute att
                     ON att.attrelid = rel.oid AND att.attnum = ANY(con.conkey)
                WHERE rel.relname = 'transactions'
                  AND con.contype = 'u'
                  AND att.attname = 'dedup_key'
                LIMIT 1;
                IF cname IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE transactions DROP CONSTRAINT ' || quote_ident(cname);
                END IF;
            END $$
        """),
        ("add status column",
         "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active'"),
        ("add dedup_of column",
         "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS dedup_of TEXT"),
        ("add import_file column",
         "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS import_file TEXT"),
        ("add manually_corrected column",
         "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS manually_corrected BOOLEAN DEFAULT FALSE"),
        ("add status index",
         "CREATE INDEX IF NOT EXISTS idx_tx_status ON transactions(status)"),
        ("add dedup_key index",
         "CREATE INDEX IF NOT EXISTS idx_tx_dedup ON transactions(dedup_key)"),

        # ── auth / multi-user migrations ─────────────────────────────────────────
        ("add user_id to transactions",
         "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id) ON DELETE CASCADE"),
        ("add user_id to uploaded_files",
         "ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id) ON DELETE CASCADE"),

        # Recreate categories with per-user schema if still on old name-primary-key schema
        ("recreate categories with user_id", """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'categories' AND column_name = 'user_id'
                ) THEN
                    DROP TABLE IF EXISTS categories CASCADE;
                    CREATE TABLE categories (
                        id      SERIAL  PRIMARY KEY,
                        user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        name    TEXT    NOT NULL,
                        UNIQUE (user_id, name)
                    );
                END IF;
            END $$
        """),

        # Drop old single-column file_hash unique constraint on uploaded_files
        # and replace with (user_id, file_hash)
        ("update uploaded_files unique constraint", """
            DO $$
            DECLARE cname TEXT;
            BEGIN
                SELECT con.conname INTO cname
                FROM pg_constraint con
                JOIN pg_class rel ON rel.oid = con.conrelid
                WHERE rel.relname = 'uploaded_files'
                  AND con.contype = 'u'
                  AND array_length(con.conkey, 1) = 1
                  AND EXISTS (
                      SELECT 1 FROM pg_attribute att
                      WHERE att.attrelid = rel.oid
                        AND att.attnum = con.conkey[1]
                        AND att.attname = 'file_hash'
                  );
                IF cname IS NOT NULL THEN
                    EXECUTE 'ALTER TABLE uploaded_files DROP CONSTRAINT ' || quote_ident(cname);
                END IF;
            END $$
        """),
        ("add uploaded_files (user_id, file_hash) unique constraint", """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint con
                    JOIN pg_class rel ON rel.oid = con.conrelid
                    WHERE rel.relname = 'uploaded_files'
                      AND con.contype = 'u'
                      AND array_length(con.conkey, 1) = 2
                ) THEN
                    ALTER TABLE uploaded_files
                        ADD CONSTRAINT uploaded_files_user_file_hash_key
                        UNIQUE (user_id, file_hash);
                END IF;
            END $$
        """),

        ("add excluded_from_spending to categories",
         "ALTER TABLE categories ADD COLUMN IF NOT EXISTS excluded_from_spending BOOLEAN NOT NULL DEFAULT FALSE"),

        ("add invited_users table", """
            CREATE TABLE IF NOT EXISTS invited_users (
                id           SERIAL      PRIMARY KEY,
                email        TEXT        UNIQUE NOT NULL,
                role         TEXT        NOT NULL DEFAULT 'read',
                invited_at   TIMESTAMPTZ DEFAULT NOW(),
                last_seen_at TIMESTAMPTZ
            )
        """),

        ("add card_last4 to uploaded_files",
         "ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS card_last4 TEXT"),

        ("rename BofA Checking source to Bank of America", """
            UPDATE transactions SET source = 'Bank of America' WHERE source = 'BofA Checking';
            UPDATE uploaded_files SET source = 'Bank of America' WHERE source = 'BofA Checking'
        """),

        # ── tags (replaces subcategories) ────────────────────────────────────────
        ("add tags table", """
            CREATE TABLE IF NOT EXISTS tags (
                id      SERIAL  PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                name    TEXT    NOT NULL,
                UNIQUE (user_id, name)
            )
        """),

        ("add transaction_tags table", """
            CREATE TABLE IF NOT EXISTS transaction_tags (
                transaction_id INTEGER REFERENCES transactions(id) ON DELETE CASCADE,
                tag_id         INTEGER REFERENCES tags(id) ON DELETE CASCADE,
                PRIMARY KEY (transaction_id, tag_id)
            )
        """),

        ("drop subcategory column from transactions",
         "ALTER TABLE transactions DROP COLUMN IF EXISTS subcategory"),

        ("drop subcategories table",
         "DROP TABLE IF EXISTS subcategories CASCADE"),

        ("create upload_jobs table", """
            CREATE TABLE IF NOT EXISTS upload_jobs (
                id TEXT PRIMARY KEY,
                user_id INTEGER,
                filename TEXT,
                status TEXT DEFAULT 'pending',
                result_json TEXT,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """),
        ("add upload job observation fields", """
            ALTER TABLE upload_jobs ADD COLUMN IF NOT EXISTS file_hash TEXT;
            ALTER TABLE upload_jobs ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();
            UPDATE upload_jobs SET updated_at=created_at WHERE updated_at IS NULL;
            CREATE INDEX IF NOT EXISTS idx_upload_jobs_user_recent
                ON upload_jobs(user_id, created_at DESC)
        """),

        ("delete activity-4 uploads and transactions 2026-03-11", """
            DELETE FROM transactions
            WHERE import_file IN ('activity-4.csv', 'activity-4-part2.csv', 'activity-4-part3.csv');
            DELETE FROM uploaded_files
            WHERE filename IN ('activity-4.csv', 'activity-4-part2.csv', 'activity-4-part3.csv')
        """),

        ("create cards table", """
            CREATE TABLE IF NOT EXISTS cards (
                id      SERIAL  PRIMARY KEY,
                user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                name    TEXT    NOT NULL,
                UNIQUE(user_id, name)
            )
        """),

        ("add card_id to uploaded_files",
         "ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS card_id INTEGER REFERENCES cards(id) ON DELETE SET NULL"),

        ("strip AplPay from transaction descriptions", """
            UPDATE transactions
            SET description = TRIM(REGEXP_REPLACE(description, '(?i)\\mAplPay\\s*', '', 'g'))
            WHERE description ~* 'AplPay'
        """),

        ("strip SP prefix from transaction descriptions", """
            UPDATE transactions
            SET description = TRIM(REGEXP_REPLACE(description, '^SP\\s+', '', 'i'))
            WHERE description ~* '^SP\\s+'
        """),

        ("strip TST prefix from transaction descriptions", """
            UPDATE transactions
            SET description = TRIM(REGEXP_REPLACE(description, '^\\*?TST\\*?\\s*', '', 'i'))
            WHERE description ~* '^\\*?TST\\*?'
        """),

        ("add excluded_from_spending to tags",
         "ALTER TABLE tags ADD COLUMN IF NOT EXISTS excluded_from_spending BOOLEAN NOT NULL DEFAULT FALSE"),

        ("add group_tag_id to tags",
         "ALTER TABLE tags ADD COLUMN IF NOT EXISTS group_tag_id INTEGER REFERENCES tags(id) ON DELETE SET NULL"),

        ("add primary_tag_id to transactions",
         "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS primary_tag_id INTEGER REFERENCES tags(id) ON DELETE SET NULL"),

        ("index primary_tag_id",
         "CREATE INDEX IF NOT EXISTS idx_tx_primary_tag ON transactions(primary_tag_id)"),

        ("add primary_migration_status to transactions",
         "ALTER TABLE transactions ADD COLUMN IF NOT EXISTS primary_migration_status TEXT"),

        ("add categorization guidance", """
            CREATE TABLE IF NOT EXISTS categorization_settings (
                user_id INTEGER PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
                guide TEXT NOT NULL DEFAULT '',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS correction_scope TEXT;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS correction_note TEXT NOT NULL DEFAULT '';
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS needs_review BOOLEAN NOT NULL DEFAULT FALSE;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS suggested_tag_id INTEGER REFERENCES tags(id) ON DELETE SET NULL;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS categorization_reason TEXT NOT NULL DEFAULT '';
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS categorization_confidence TEXT;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS categorization_example_ids INTEGER[] NOT NULL DEFAULT '{}';
            CREATE INDEX IF NOT EXISTS idx_tx_review ON transactions(user_id) WHERE needs_review AND status='active';
        """),
        ("add correction archive", """
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS correction_archived BOOLEAN NOT NULL DEFAULT FALSE;
            CREATE INDEX IF NOT EXISTS idx_tx_corrections ON transactions(user_id, date DESC, id DESC)
                WHERE manually_corrected=TRUE AND status='active';
        """),
        ("add explicit transaction type", """
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS transaction_type TEXT;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS type_revision INTEGER NOT NULL DEFAULT 0;
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS type_updated_at TIMESTAMPTZ;
            DO $$ BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname='transactions_type_allowed') THEN
                    ALTER TABLE transactions ADD CONSTRAINT transactions_type_allowed
                        CHECK (transaction_type IN ('expense','income','transfer','refund'));
                END IF;
            END $$;
        """),
        ("add correction revision", """
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS correction_revision INTEGER NOT NULL DEFAULT 0;
        """),
        ("add amount provenance and reversal history", """
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS original_amount NUMERIC(12,2);
            ALTER TABLE transactions ADD COLUMN IF NOT EXISTS amount_revision INTEGER NOT NULL DEFAULT 0;
            UPDATE transactions SET original_amount=amount WHERE original_amount IS NULL;
            CREATE OR REPLACE FUNCTION set_transaction_original_amount() RETURNS trigger AS $$
            BEGIN
                IF TG_OP = 'UPDATE' AND OLD.original_amount IS NOT NULL
                   AND NEW.original_amount IS DISTINCT FROM OLD.original_amount THEN
                    RAISE EXCEPTION 'original_amount is immutable';
                END IF;
                IF NEW.original_amount IS NULL THEN NEW.original_amount := NEW.amount; END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            DROP TRIGGER IF EXISTS trg_transaction_original_amount ON transactions;
            CREATE TRIGGER trg_transaction_original_amount BEFORE INSERT OR UPDATE ON transactions
                FOR EACH ROW EXECUTE FUNCTION set_transaction_original_amount();
            ALTER TABLE transactions ALTER COLUMN original_amount SET NOT NULL;
            CREATE TABLE IF NOT EXISTS transaction_amount_changes (
                id BIGSERIAL PRIMARY KEY,
                transaction_id INTEGER NOT NULL REFERENCES transactions(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                actor_user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                operation_id UUID NOT NULL UNIQUE,
                action TEXT NOT NULL CHECK (action IN ('reverse', 'undo')),
                undo_of BIGINT UNIQUE REFERENCES transaction_amount_changes(id),
                original_amount NUMERIC(12,2) NOT NULL,
                before_amount NUMERIC(12,2) NOT NULL,
                after_amount NUMERIC(12,2) NOT NULL,
                before_revision INTEGER NOT NULL,
                after_revision INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_tx_amount_changes
                ON transaction_amount_changes(transaction_id, id DESC);
        """),
    ]

    for label, sql in migrations:
        try:
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
        except Exception as e:
            print(f"[migrate:{label}] {e}")

    # ── Primary tag data migration ──────────────────────────────────────────────
    _migrate_primary_tags()

    # In LOCAL_DEV mode, ensure the test user exists and owns any orphaned records.
    if LOCAL_DEV:
        user = _ensure_local_user()
        uid  = user["id"]
        try:
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE transactions   SET user_id = %s WHERE user_id IS NULL", (uid,))
                    cur.execute("UPDATE uploaded_files SET user_id = %s WHERE user_id IS NULL", (uid,))
        except Exception as e:
            print(f"[migrate:assign-orphans] {e}")

# ── Local dev user ────────────────────────────────────────────────────────────────
_local_user_cache: Optional[dict] = None

def _ensure_local_user() -> dict:
    global _local_user_cache
    if _local_user_cache:
        return _local_user_cache
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO users (google_id, email, name, picture)
                VALUES ('local', 'local@localhost', 'Local Dev User', NULL)
                ON CONFLICT (email) DO UPDATE SET name = EXCLUDED.name
                RETURNING id, email, name, picture
            """)
            _local_user_cache = dict(cur.fetchone())
    return _local_user_cache

# ── Auth dependency ───────────────────────────────────────────────────────────────
def get_current_user(request: Request) -> dict:
    """
    FastAPI dependency — resolves the authenticated user.
    Returns a dict with: id (owner's id for data queries), email, name, picture,
    role ('owner'|'edit'|'read'), is_owner (bool).
    In LOCAL_DEV mode returns the local test user without checking a cookie.
    """
    if LOCAL_DEV:
        local = _ensure_local_user()
        return {**local, "role": "owner", "is_owner": True}

    token = request.cookies.get("session")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user_id = _unsign_session(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, email, name, picture FROM users WHERE id = %s", (user_id,))
            auth_user = cur.fetchone()
    if not auth_user:
        raise HTTPException(status_code=401, detail="User not found")
    auth_user = dict(auth_user)

    # No OWNER_EMAIL set → backward compat: every user is their own owner
    if not OWNER_EMAIL:
        return {**auth_user, "role": "owner", "is_owner": True}

    # Owner access
    if auth_user["email"].lower() == OWNER_EMAIL.lower():
        return {**auth_user, "role": "owner", "is_owner": True}

    # Invited user — resolve role fresh from DB on every request so revocations take effect immediately
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT role FROM invited_users WHERE lower(email) = lower(%s)",
                (auth_user["email"],)
            )
            invite = cur.fetchone()
            if not invite:
                raise HTTPException(status_code=403, detail="Not authorized")
            cur.execute(
                "UPDATE invited_users SET last_seen_at = NOW() WHERE lower(email) = lower(%s)",
                (auth_user["email"],)
            )
            cur.execute("SELECT id FROM users WHERE lower(email) = lower(%s)", (OWNER_EMAIL,))
            owner_row = cur.fetchone()
    if not owner_row:
        raise HTTPException(status_code=403, detail="Owner has not logged in yet")

    return {
        "id":       owner_row["id"],       # data queries always use owner's id
        "auth_id":  auth_user["id"],
        "email":    auth_user["email"],
        "name":     auth_user["name"],
        "picture":  auth_user["picture"],
        "role":     invite["role"],        # 'read' or 'edit'
        "is_owner": False,
    }

def require_edit(user: dict = Depends(get_current_user)) -> dict:
    """Dependency: allows owner and editors; blocks read-only users."""
    if user["role"] == "read":
        raise HTTPException(status_code=403, detail="Read-only access — editing not permitted")
    return user

def require_owner(user: dict = Depends(get_current_user)) -> dict:
    """Dependency: allows only the owner."""
    if not user["is_owner"]:
        raise HTTPException(status_code=403, detail="Owner-only action")
    return user

# ── GPT tag assignment ────────────────────────────────────────────────────────────
def _gpt_tag_chunk(client, model, tag_list, chunk, guide=""):
    """Categorize individual rows with selected human examples, never AI history."""
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "system", "content": SYSTEM_PROMPT},
                  {"role": "user", "content": request_payload(tag_list, guide, chunk)}],
        reasoning_effort="low",
        max_completion_tokens=16384,
        response_format={"type": "json_object"},
    )
    choice = resp.choices[0]
    if choice.finish_reason == "length":
        raise ValueError("Categorization response exceeded its token budget")
    if choice.finish_reason != "stop":
        raise ValueError("Categorization response was not completed")
    return validate_results(json.loads(choice.message.content), tag_list, chunk)


def assign_tags_with_gpt(rows: list, tag_list: list, guide="", history=None) -> list:
    """One decision per input row; unavailable/invalid results go to review."""
    if not rows:
        return []
    if not tag_list or not OPENAI_API_KEY:
        reason = "Create categories before categorizing this transaction." if not tag_list else "AI categorization is unavailable; choose a category."
        return [review(reason) for _ in rows]
    contexts = prepare_context(rows, history or [])
    result = [review("AI categorization failed; choose a category.") for _ in rows]
    from concurrent.futures import ThreadPoolExecutor, as_completed
    try:
        with OpenAI(api_key=OPENAI_API_KEY, timeout=120) as client:
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = {pool.submit(_gpt_tag_chunk, client, OPENAI_TAG_MODEL, tag_list,
                                       contexts[i:i+20], guide): i
                           for i in range(0, len(contexts), 20)}
                for fut in as_completed(futures):
                    try:
                        decisions = fut.result()
                        start = futures[fut]
                        result[start:start+len(decisions)] = decisions
                    except Exception as e:
                        # Avoid logging prompts, private descriptions, or provider response bodies.
                        print(f"[GPT tag chunk] {type(e).__name__}")
    except Exception as e:
        print(f"[GPT assign tags] {type(e).__name__}")
    return result


def load_categorization_context(user_id):
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT guide FROM categorization_settings WHERE user_id=%s", (user_id,))
            setting = cur.fetchone()
            cur.execute("""
                SELECT t.id, t.date::text, t.description, t.amount::float, t.source,
                       t.manually_corrected, t.correction_scope, t.correction_note,
                       t.correction_archived,
                       pt.name AS primary_tag
                FROM transactions t
                LEFT JOIN tags pt ON pt.id=t.primary_tag_id AND pt.user_id=t.user_id
                WHERE t.user_id=%s AND t.status='active' AND t.manually_corrected=TRUE
                  AND t.correction_archived=FALSE
                  AND (t.correction_scope IS NULL OR t.correction_scope='similar')
                ORDER BY t.date DESC, t.id DESC LIMIT 5000
            """, (user_id,))
            history = [dict(row) for row in cur.fetchall()]
    return (setting["guide"] if setting else ""), history


# ── Description cleaning ──────────────────────────────────────────────────────────
def clean_description(desc: str) -> str:
    """Remove payment-method prefixes that add no useful information."""
    desc = re.sub(r'(?i)\bAplPay\s*', '', desc)
    desc = re.sub(r'(?i)^SP\s+', '', desc)
    desc = re.sub(r'(?i)^\*?TST\*?\s*', '', desc)
    return desc.strip()

# ── Dedup key ─────────────────────────────────────────────────────────────────────
def make_dedup_key(date: str, source: str, amount: float, description: str,
                   seq: int = 1, account_key: str = '') -> str:
    norm = re.sub(r'[^A-Z0-9]', '', description.upper())[:12]
    raw = (f"{date}|{source}|{account_key}|{amount:.2f}|{norm}|{seq}" if account_key
           else f"{date}|{source}|{amount:.2f}|{norm}|{seq}")
    return hashlib.md5(raw.encode()).hexdigest()

# ── Source detection ──────────────────────────────────────────────────────────────
def detect_source(text: str) -> Optional[str]:
    t = text.upper()
    if "COINBASE ONE CARD" in t or ("CARDLESS" in t and "FIRST ELECTRONIC BANK" in t):
        return "Coinbase"
    if "APPLE CARD" in t and ("GOLDMAN SACHS" in t or "DAILY CASH" in t):
        return "Apple Card"
    if "CITI DOUBLE CASH" in t or "CITICARDS.COM" in t:
        return "Citi"
    if "PRIME VISA" in t or "CHASE.COM/AMAZON" in t or "CHASE MOBILE" in t:
        return "Amazon"
    if "ADV RELATIONSHIP BANKING" in t:
        return "Bank of America"
    if "BANKOFAMERICA.COM" in t or "BANK OF AMERICA" in t:
        return "Bank of America"
    return None

# ── Date helpers ──────────────────────────────────────────────────────────────────
def parse_date(d: str) -> str:
    for fmt in ("%m/%d/%Y", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(d.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return d.strip()

# ── GPT-based parser ──────────────────────────────────────────────────────────────
GPT_PARSE_PROMPT = """You are a financial statement parser. Extract ALL current-period account activity rows that have a date, description, and non-zero dollar amount.

Return JSON in this exact format:
{"transactions": [{"date": "YYYY-MM-DD", "description": "merchant name or description", "amount": 0.00}, ...]}

Rules:
- Include EVERY dated current-period account activity row — purchases, fees, interest, payments, refunds, credits, transfers, everything
- amount: use the Spending Dashboard sign convention, not necessarily the sign printed on the statement
- amount: positive for charges/purchases/fees/withdrawals/debits/payments made/outflows; negative for deposits/additions/income/interest/refunds/credits/payments received/inflows
- If a statement groups transactions by section, use the section's meaning to set the sign: rows under deposits/additions/credits/income are inflows and must be negative; rows under withdrawals/subtractions/debits/payments/fees/purchases are outflows and must be positive, even if the statement prints the opposite sign
- date: YYYY-MM-DD format
- Skip informational rewards/points activity sections (such as Chase Shop with Points) and Apple Card installment financing schedules that repeat earlier purchases. Their dated entries are not new account activity. For a year-end transaction summary, include the itemized transaction list but not category subtotals.
- SKIP also: rows with no date, rows with $0.00 amount, pure header/summary/subtotal rows with no transaction meaning
- Do NOT skip fees, interest, payments, transfers, or anything else — include them all"""

def parse_with_gpt(text: str, filename: str) -> tuple:
    """Parse statement text using GPT. Returns (rows, source, error)."""
    if not OPENAI_API_KEY:
        return [], None, "No OpenAI API key configured"
    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": GPT_PARSE_PROMPT},
                {"role": "user",   "content": text},
            ],
            response_format={"type": "json_object"},
            temperature=0,
            max_tokens=32000,
        )
        choice = resp.choices[0]
        if choice.finish_reason != "stop":
            return [], None, f"Statement extraction ended early ({choice.finish_reason}); no rows saved"
        data = json.loads(choice.message.content)
        raw_rows = data.get("transactions", [])
        if not isinstance(raw_rows, list):
            return [], None, "Statement extraction returned an invalid transaction list"
        rows = []
        for r in raw_rows:
            try:
                row = {
                    "date":        str(r["date"]).strip(),
                    "description": str(r["description"]).strip(),
                    "amount":      float(r["amount"]),
                }
                if not row['description'] or not math.isfinite(row['amount']) or row['amount'] == 0:
                    raise ValueError('empty description or invalid amount')
                rows.append(row)
            except (KeyError, ValueError, TypeError):
                return [], None, "Statement extraction contained an invalid row; no rows saved"
        print(f"[GPT parse] '{filename}': {len(rows)} transactions, finish={choice.finish_reason}")
        return rows, None, ""
    except Exception as e:
        return [], None, f"GPT parse error for '{filename}': {e}"

# ── Main parse dispatcher ─────────────────────────────────────────────────────────
def parse_file_bytes(content: bytes, filename: str) -> tuple:
    fname = filename.lower()

    # Extract raw text
    pages = None  # only set for PDFs
    try:
        if fname.endswith(".pdf"):
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                pages = [p.extract_text() or "" for p in pdf.pages]
            text = "\n".join(pages)
        elif fname.endswith(".csv"):
            text = content.decode("utf-8", errors="replace")
        else:
            return [], None, f"Unsupported file type: '{filename}' (use PDF or CSV)"
    except Exception as e:
        return [], None, f"Could not read '{filename}': {e}"

    CHUNK_CHARS = 30_000  # ~300 rows per chunk; keeps GPT output well under 32k token limit

    if len(text) > CHUNK_CHARS:
        # Chunk large files so GPT output never hits token limits.
        # PDFs: split by pages. CSVs: split by rows, repeating header in each chunk.
        if pages is not None:
            segments = pages
            def make_chunk(segs): return "\n".join(segs)
        else:
            all_lines = text.splitlines()
            csv_header = all_lines[0] if all_lines else ""
            segments = [l for l in all_lines[1:] if l.strip()]
            print(f"[parse] CSV '{filename}': {len(segments)} data rows, {len(text)} chars")
            def make_chunk(segs): return csv_header + "\n" + "\n".join(segs)

        chunks, cur_seg, cur_len = [], [], 0
        for seg in segments:
            if cur_len + len(seg) + 1 > CHUNK_CHARS and cur_seg:
                chunks.append(make_chunk(cur_seg))
                cur_seg, cur_len = [], 0
            cur_seg.append(seg)
            cur_len += len(seg) + 1
        if cur_seg:
            chunks.append(make_chunk(cur_seg))

        rows, gpt_error = [], ""
        for i, chunk_text in enumerate(chunks):
            cr, _, ce = parse_with_gpt(chunk_text, f"{filename}[{i+1}/{len(chunks)}]")
            if ce:
                return [], None, f"Chunk {i+1}/{len(chunks)} failed: {ce}"
            rows.extend(cr)
        print(f"[parse] '{filename}': {len(chunks)} chunks → {len(rows)} rows")
    else:
        rows, _, gpt_error = parse_with_gpt(text, filename)

    if not rows:
        return [], None, gpt_error or f"No transactions found in '{filename}'"

    source = detect_source(text) or "Unknown"
    account_key = detect_account_key(pages, source)
    if pages and source == 'Bank of America' and not account_key:
        return [], None, 'Bank of America account number could not be verified; no rows saved'

    for r in rows:
        r["date"] = parse_date(r["date"])
        try:
            datetime.strptime(r['date'], '%Y-%m-%d')
        except ValueError:
            return [], None, f"Statement extraction returned an invalid date: {r['date']}"

    try:
        reconcile_card_statement(pages, source, rows, filename)
    except StatementMismatch as mismatch:
        # One focused pass gives the model a chance to recover a skipped return
        # without asking it to reinterpret the full statement. Recheck everything.
        if mismatch.extra or not mismatch.missing_lines:
            return [], None, str(mismatch)
        hint = ('Extract ONLY these omitted dated statement lines. Preserve their '
                'printed dates, descriptions, and credit/refund signs.\n' +
                '\n'.join(mismatch.missing_lines))
        recovered, _, repair_error = parse_with_gpt(hint, f'{filename}[reconcile]')
        if repair_error:
            return [], None, f'{mismatch} Recovery failed: {repair_error}'
        for r in recovered:
            r['date'] = parse_date(r['date'])
        rows.extend(recovered)
        try:
            reconcile_card_statement(pages, source, rows, filename)
        except ValueError as exc:
            return [], None, str(exc)
    except ValueError as exc:
        return [], None, str(exc)

    seq_counts: Counter = Counter()
    seen_keys = set()
    for r in rows:
        r.setdefault("source", source)
        base = (r["date"], r["source"], r["amount"], r["description"])
        seq_counts[base] += 1
        if account_key:
            r['account_key'] = account_key
            r['legacy_dedup_keys'] = [make_dedup_key(
                r['date'], alias, r['amount'], r['description'], seq_counts[base])
                for alias in ('Bank of America', 'BofA', 'BofA Checking')]
            r['legacy_dedup_key'] = r['legacy_dedup_keys'][0]
        r["dedup_key"] = make_dedup_key(
            r["date"], r["source"], r["amount"], r["description"],
            seq_counts[base], account_key or '')
        if r['dedup_key'] in seen_keys:
            return [], None, 'Statement contains ambiguous duplicate row identities; no rows saved'
        seen_keys.add(r['dedup_key'])

    return rows, source, ""

# ── FastAPI app ───────────────────────────────────────────────────────────────────
app = FastAPI(title="Spending Dashboard")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def _get_git_version() -> dict:
    # Check for baked-in version file first (written by deploy script)
    version_file = os.path.join(os.path.dirname(__file__), "version.json")
    if os.path.exists(version_file):
        try:
            import json
            with open(version_file) as f:
                return json.load(f)
        except Exception:
            pass
    try:
        sha = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=os.path.dirname(__file__), stderr=subprocess.DEVNULL
        ).decode().strip()
        ts = subprocess.check_output(
            ["git", "log", "-1", "--format=%ci"],
            cwd=os.path.dirname(__file__), stderr=subprocess.DEVNULL
        ).decode().strip()
        return {"sha": sha, "timestamp": ts}
    except Exception:
        return {"sha": "unknown", "timestamp": datetime.utcnow().isoformat()}

GIT_VERSION = _get_git_version()

@app.on_event("startup")
def startup():
    init_db()

@app.get("/api/version")
def get_version():
    return GIT_VERSION

# ── Auth routes ───────────────────────────────────────────────────────────────────
@app.get("/auth/login")
def auth_login():
    if LOCAL_DEV:
        return RedirectResponse("/", status_code=302)
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise HTTPException(500, "Google OAuth not configured")
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  GOOGLE_CALLBACK_URL,
        "response_type": "code",
        "scope":         "openid email profile",
        "access_type":   "offline",
        "prompt":        "select_account",
    }
    return RedirectResponse(
        "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params),
        status_code=302
    )

@app.get("/auth/google/callback")
async def auth_callback(
    response: Response,
    code: Optional[str] = None,
    error: Optional[str] = None,
    error_description: Optional[str] = None,
):
    if error:
        raise HTTPException(400, f"OAuth error: {error_description or error}")
    if not code:
        raise HTTPException(400, "OAuth error: missing authorization code")
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise HTTPException(500, "Google OAuth not configured")
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code":          code,
                "client_id":     GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri":  GOOGLE_CALLBACK_URL,
                "grant_type":    "authorization_code",
            }
        )
    try:
        tokens = token_resp.json()
    except ValueError:
        print(f"[oauth] token exchange failed: status={token_resp.status_code} body={token_resp.text[:500]}")
        raise HTTPException(400, "OAuth error: token exchange failed")

    if token_resp.status_code >= 400 or "error" in tokens:
        error_code = tokens.get("error", "token_exchange_failed")
        detail = tokens.get("error_description") or error_code
        print(f"[oauth] token exchange failed: status={token_resp.status_code} error={error_code} detail={detail} callback={GOOGLE_CALLBACK_URL}")
        if error_code == "invalid_grant" and detail == "Bad Request":
            detail = "Google rejected the authorization code. Check that GOOGLE_CALLBACK_URL exactly matches the authorized redirect URI in Google Cloud."
        raise HTTPException(400, f"OAuth error: {detail}")

    async with httpx.AsyncClient() as client:
        info_resp = await client.get(
            "https://www.googleapis.com/oauth2/v2/userinfo",
            headers={"Authorization": f"Bearer {tokens['access_token']}"}
        )
    userinfo = info_resp.json()

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO users (google_id, email, name, picture)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (google_id) DO UPDATE
                    SET email   = EXCLUDED.email,
                        name    = EXCLUDED.name,
                        picture = EXCLUDED.picture
                RETURNING id, email, name, picture
            """, (userinfo["id"], userinfo["email"],
                  userinfo.get("name", ""), userinfo.get("picture")))
            user = dict(cur.fetchone())

    token    = _sign_session(user["id"])
    redirect = RedirectResponse("/", status_code=302)
    redirect.set_cookie(
        "session", token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=GOOGLE_CALLBACK_URL.lower().startswith("https://"),
    )
    return redirect

@app.post("/auth/logout")
def auth_logout():
    resp = RedirectResponse("/", status_code=302)
    resp.delete_cookie("session")
    return resp

@app.get("/auth/me")
def auth_me(user: dict = Depends(get_current_user)):
    return user

# ── Static page ───────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def index():
    path = os.path.join(os.path.dirname(__file__), "index.html")
    try:
        with open(path) as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>index.html not found</h1>", status_code=500)

# ── Tag filter helper (searches primary tag with hierarchy + secondary tags flat) ─
# Match ONE tag: primary tag ancestor chain OR secondary tag
_TAG_MATCH_ONE = (
    "("
    # Match via primary tag ancestor chain
    "EXISTS ("
    "  WITH RECURSIVE chain AS ("
    "    SELECT t.primary_tag_id AS cid WHERE t.primary_tag_id IS NOT NULL"
    "    UNION ALL"
    "    SELECT tg.group_tag_id FROM tags tg JOIN chain c ON tg.id = c.cid WHERE tg.group_tag_id IS NOT NULL"
    "  ) SELECT 1 FROM chain JOIN tags tg ON tg.id = chain.cid WHERE tg.user_id = %s AND tg.name = %s"
    ")"
    " OR "
    # Match via secondary tags (flat, no hierarchy)
    "t.id IN (SELECT tt.transaction_id FROM transaction_tags tt JOIN tags tg ON tg.id = tt.tag_id WHERE tg.user_id = %s AND tg.name = %s)"
    ")"
)
# Match ANY of multiple tags
_TAG_MATCH_ANY = (
    "("
    "EXISTS ("
    "  WITH RECURSIVE chain AS ("
    "    SELECT t.primary_tag_id AS cid WHERE t.primary_tag_id IS NOT NULL"
    "    UNION ALL"
    "    SELECT tg.group_tag_id FROM tags tg JOIN chain c ON tg.id = c.cid WHERE tg.group_tag_id IS NOT NULL"
    "  ) SELECT 1 FROM chain JOIN tags tg ON tg.id = chain.cid WHERE tg.user_id = %s AND tg.name = ANY(%s)"
    ")"
    " OR "
    "t.id IN (SELECT tt.transaction_id FROM transaction_tags tt JOIN tags tg ON tg.id = tt.tag_id WHERE tg.user_id = %s AND tg.name = ANY(%s))"
    ")"
)
# "No tag" filter: no primary tag AND no secondary tags
_TAG_MATCH_NONE = (
    "(t.primary_tag_id IS NULL"
    " AND t.id NOT IN (SELECT tt.transaction_id FROM transaction_tags tt JOIN tags tg ON tg.id = tt.tag_id WHERE tg.user_id = %s))"
)
# Exact primary tag match (no hierarchy walk) — for "Misc" filter
_TAG_MATCH_EXACT = (
    "(t.primary_tag_id = (SELECT id FROM tags WHERE user_id = %s AND name = %s LIMIT 1))"
)

def _apply_tag_filter(where, params, tag, tag_match, uid):
    exact = [t[8:-2] for t in tag if t.startswith("__exact:") and t.endswith("__")]
    named = [t for t in tag if t != "__none__" and not t.startswith("__exact:")]
    has_none = "__none__" in tag
    if tag_match == "all":
        for tname in named:
            where.append(_TAG_MATCH_ONE)
            params.extend([uid, tname, uid, tname])
        for tname in exact:
            where.append(_TAG_MATCH_EXACT)
            params.extend([uid, tname])
        if has_none:
            where.append(_TAG_MATCH_NONE)
            params.append(uid)
    else:
        clauses = []
        if has_none:
            clauses.append(_TAG_MATCH_NONE)
            params.append(uid)
        if named:
            clauses.append(_TAG_MATCH_ANY)
            params.extend([uid, named, uid, named])
        for tname in exact:
            clauses.append(_TAG_MATCH_EXACT)
            params.extend([uid, tname])
        where.append("(" + " OR ".join(clauses) + ")")
    return where, params

class CategorizationGuideUpdate(BaseModel):
    guide: str = Field(max_length=12000)


@app.get("/api/categorization-guide")
def get_categorization_guide(user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT guide FROM categorization_settings WHERE user_id=%s", (user["id"],))
            row = cur.fetchone()
    return {"guide": row[0] if row else ""}


@app.put("/api/categorization-guide")
def save_categorization_guide(body: CategorizationGuideUpdate, user: dict = Depends(require_edit)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO categorization_settings(user_id, guide) VALUES(%s,%s)
                ON CONFLICT(user_id) DO UPDATE SET guide=EXCLUDED.guide, updated_at=NOW()
            """, (user["id"], body.guide.strip()))
    return {"guide": body.guide.strip()}


# A reusable correction is an example drawn from a source transaction, not a
# separate merchant matcher. Legacy manual corrections have unknown scope but
# remain eligible examples until the user archives or edits them.
@app.get("/api/categorization-corrections")
def list_categorization_corrections(
    kind: Literal["reusable", "one-time", "archived"] = "reusable",
    search: str = Query("", max_length=200),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user: dict = Depends(get_current_user),
):
    uid = user["id"]
    scopes = {
        "reusable": "t.correction_archived=FALSE AND (t.correction_scope='similar' OR t.correction_scope IS NULL)",
        "one-time": "t.correction_archived=FALSE AND t.correction_scope='transaction'",
        "archived": "t.correction_archived=TRUE",
    }
    conditions = ["t.user_id=%s", "t.status='active'", "t.manually_corrected=TRUE"]
    params = [uid]
    term = search.strip()
    if term:
        conditions.append("(t.description ILIKE %s ESCAPE '!' OR t.source ILIKE %s ESCAPE '!' OR "
                          "t.correction_note ILIKE %s ESCAPE '!' OR pt.name ILIKE %s ESCAPE '!')")
        literal_term = term.replace('!', '!!').replace('%', '!%').replace('_', '!_')
        params.extend([f"%{literal_term}%"] * 4)
    base = " AND ".join(conditions)
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE {scopes['reusable']}) AS reusable,
                    COUNT(*) FILTER (WHERE {scopes['one-time']}) AS one_time,
                    COUNT(*) FILTER (WHERE {scopes['archived']}) AS archived
                FROM transactions t
                LEFT JOIN tags pt ON pt.id=t.primary_tag_id AND pt.user_id=t.user_id
                WHERE {base}
            """, params)
            counts = dict(cur.fetchone())
            cur.execute(f"""
                SELECT t.id, t.date::text, t.description, t.amount::float, t.source,
                       t.correction_scope, t.correction_note, t.correction_archived,
                       t.correction_revision,
                       pt.name AS primary_tag,
                       COALESCE(ARRAY(
                           SELECT tg.name FROM transaction_tags tt
                           JOIN tags tg ON tg.id=tt.tag_id AND tg.user_id=t.user_id
                           WHERE tt.transaction_id=t.id ORDER BY tg.name
                       ), '{{}}') AS secondary_tags
                FROM transactions t
                LEFT JOIN tags pt ON pt.id=t.primary_tag_id AND pt.user_id=t.user_id
                WHERE {base} AND {scopes[kind]}
                ORDER BY t.date DESC, t.id DESC LIMIT %s OFFSET %s
            """, params + [limit, offset])
            rows = [dict(row) for row in cur.fetchall()]
    return {"corrections": rows, "counts": counts,
            "total": counts[kind.replace("-", "_")], "limit": limit, "offset": offset}


@app.delete("/api/categorization-corrections/{tx_id}")
def archive_categorization_correction(
    tx_id: int, expected_revision: Optional[int] = Query(None, ge=0),
    user: dict = Depends(require_edit),
):
    revision_clause = " AND correction_revision=%s" if expected_revision is not None else ""
    params = (tx_id, user["id"]) + ((expected_revision,) if expected_revision is not None else ())
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE transactions SET correction_archived=TRUE,
                    correction_revision=correction_revision+1
                WHERE id=%s AND user_id=%s AND status='active'
                  AND manually_corrected=TRUE AND correction_archived=FALSE
                  {revision_clause}
                RETURNING id
            """, params)
            if not cur.fetchone():
                _raise_missing_or_stale_correction(cur, tx_id, user["id"], expected_revision)
    return {"ok": True, "id": tx_id}


def _raise_missing_or_stale_correction(cur, tx_id, uid, expected_revision):
    if expected_revision is not None:
        cur.execute("""SELECT correction_revision FROM transactions
                       WHERE id=%s AND user_id=%s AND status='active'
                         AND manually_corrected=TRUE""", (tx_id, uid))
        if cur.fetchone():
            raise HTTPException(409, "Correction changed. Refresh the list and try again.")
    raise HTTPException(404, "Correction not found")


@app.post("/api/categorization-corrections/{tx_id}/restore")
def restore_categorization_correction(
    tx_id: int, expected_revision: Optional[int] = Query(None, ge=0),
    user: dict = Depends(require_edit),
):
    revision_clause = " AND correction_revision=%s" if expected_revision is not None else ""
    params = (tx_id, user["id"]) + ((expected_revision,) if expected_revision is not None else ())
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE transactions SET correction_archived=FALSE,
                    correction_revision=correction_revision+1
                WHERE id=%s AND user_id=%s AND status='active'
                  AND manually_corrected=TRUE AND correction_archived=TRUE
                  {revision_clause}
                RETURNING id
            """, params)
            if not cur.fetchone():
                _raise_missing_or_stale_correction(cur, tx_id, user["id"], expected_revision)
    return {"ok": True, "id": tx_id}

# ── Transactions ──────────────────────────────────────────────────────────────────
@app.get("/api/transactions")
def get_transactions(
    page: int = Query(1, ge=1, le=1000000), per_page: int = Query(100, ge=1, le=100),
    source: str = "", tag: List[str] = Query([]), tag_match: str = "any",
    search: str = "", date_from: str = "", date_to: str = "",
    import_file: str = "", card_last4: str = "",
    sort_by: str = "date", sort_dir: str = "desc",
    status: str = "active",
    transaction_type: Optional[Literal["expense", "income", "transfer", "refund", "unreviewed"]] = None,
    user: dict = Depends(get_current_user)
):
    uid = user["id"]
    where, params = ["t.user_id = %s"], [uid]
    if status == "review":
        where.extend(["t.status = 'active'", "t.needs_review = TRUE"])
    elif status in ("active", "deleted", "deduped"):
        where.append("t.status = %s"); params.append(status)
    if source:      where.append("t.source = %s");          params.append(source)
    if transaction_type:
        if transaction_type == "unreviewed":
            where.append("t.transaction_type IS NULL")
        else:
            where.append("t.transaction_type = %s"); params.append(transaction_type)
    if tag:
        where, params = _apply_tag_filter(where, params, tag, tag_match, uid)
    if date_from:   where.append("t.date >= %s");           params.append(date_from)
    if date_to:     where.append("t.date <= %s");           params.append(date_to)
    if search:      where.append("t.description ILIKE %s"); params.append(f"%{search}%")
    if import_file: where.append("t.import_file = %s");     params.append(import_file)
    if card_last4:
        where.append("t.import_file IN (SELECT filename FROM uploaded_files WHERE user_id=%s AND card_last4=%s)")
        params.extend([uid, card_last4])
    wc = " AND ".join(where)

    valid_cols = {"date", "amount", "description", "source"}
    sc = "t." + (sort_by if sort_by in valid_cols else "date")
    sd = "DESC" if sort_dir == "desc" else "ASC"
    offset = (page - 1) * per_page

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) as n FROM transactions t WHERE {wc}", params)
            total = cur.fetchone()["n"]
            cur.execute(f"""
                SELECT t.id, t.date::text, t.description,
                       t.amount::float, t.original_amount::float, t.amount_revision,
                       t.source, t.import_file,
                       t.status, t.dedup_of, t.correction_scope, t.correction_note,
                       t.transaction_type, t.type_revision, t.type_updated_at,
                       t.needs_review, st.name AS suggested_tag,
                       t.categorization_reason, t.categorization_confidence,
                       t.categorization_example_ids,
                       pt.name AS primary_tag,
                       COALESCE(ARRAY(
                           SELECT tg2.name FROM (
                               WITH RECURSIVE chain AS (
                                   SELECT pt2.group_tag_id AS cid FROM tags pt2
                                   WHERE pt2.id = t.primary_tag_id AND pt2.group_tag_id IS NOT NULL
                                   UNION ALL
                                   SELECT g.group_tag_id FROM tags g
                                   JOIN chain c ON g.id = c.cid WHERE g.group_tag_id IS NOT NULL
                               )
                               SELECT cid FROM chain
                           ) ch JOIN tags tg2 ON tg2.id = ch.cid ORDER BY tg2.name
                       ), '{{}}') AS primary_tag_implicit,
                       COALESCE(ARRAY(
                           SELECT tg.name FROM transaction_tags tt
                           JOIN tags tg ON tg.id = tt.tag_id
                           WHERE tt.transaction_id = t.id ORDER BY tg.name
                       ), '{{}}') AS tags
                FROM transactions t
                LEFT JOIN tags pt ON pt.id = t.primary_tag_id
                LEFT JOIN tags st ON st.id = t.suggested_tag_id AND st.user_id = t.user_id
                WHERE {wc}
                ORDER BY {sc} {sd}, t.id {sd}
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            rows = [dict(r) for r in cur.fetchall()]
            for row in rows:
                row["type_sign_issue"] = sign_issue(row["transaction_type"], row["amount"])

    return {"transactions": rows, "total": total, "page": page,
            "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page)}


# ── Signed amount correction ───────────────────────────────────────────────────
class AmountChangeRequest(BaseModel):
    operation_id: UUID
    expected_revision: int = Field(ge=0)
    expected_amount: Decimal


class AmountUndoRequest(AmountChangeRequest):
    change_id: int = Field(gt=0)


def _checked_amount(value: Decimal) -> Decimal:
    """Require an exact cent value for compare-and-swap, never a rounded input."""
    if (not value.is_finite() or abs(value) > Decimal('9999999999.99') or
        value != value.quantize(Decimal('0.01'))):
        raise HTTPException(422, 'expected_amount must be a finite cent value')
    return value


def _amount_event(change: dict, kind: Optional[str], replayed: bool = False) -> dict:
    return {
        'id': change['id'], 'transaction_id': change['transaction_id'],
        'action': change['action'], 'undo_of': change['undo_of'],
        'actor_user_id': change['actor_user_id'],
        'actor_email': change.get('actor_email'),
        'original_amount': str(change['original_amount']),
        'before_amount': str(change['before_amount']),
        'after_amount': str(change['after_amount']),
        'before_revision': change['before_revision'],
        'after_revision': change['after_revision'],
        'created_at': change['created_at'].isoformat(),
        'type_sign_issue': sign_issue(kind, change['after_amount']),
        'replayed': replayed,
    }


@app.get('/api/transactions/{tx_id}/amount')
def get_transaction_amount(tx_id: int, user: dict = Depends(get_current_user)):
    """Read-only preview and recent audit trail, scoped to the shared dataset."""
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute('''SELECT id, amount, original_amount, amount_revision, status,
                                  transaction_type,
                                  date::text, description, source
                           FROM transactions WHERE id=%s AND user_id=%s''',
                        (tx_id, user['id']))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, 'Transaction not found')
            cur.execute('''SELECT c.id, c.transaction_id, c.action, c.undo_of,
                                  c.actor_user_id, u.email AS actor_email,
                                  c.original_amount, c.before_amount, c.after_amount,
                                  c.before_revision, c.after_revision, c.created_at
                           FROM transaction_amount_changes c
                           LEFT JOIN users u ON u.id=c.actor_user_id
                           WHERE c.transaction_id=%s AND c.user_id=%s
                           ORDER BY c.id DESC LIMIT 50''', (tx_id, user['id']))
            history = [_amount_event(r, row['transaction_type']) for r in cur.fetchall()]
    amount = row['amount']
    return {
        'id': tx_id, 'date': row['date'], 'description': row['description'],
        'source': row['source'], 'status': row['status'],
        'amount': str(amount),
        'original_amount': str(row['original_amount'] if row['original_amount'] is not None else amount),
        'amount_revision': row['amount_revision'],
        'transaction_type': row['transaction_type'],
        'type_sign_issue': sign_issue(row['transaction_type'], amount),
        'reverse_preview': str(-amount) if row['status'] == 'active' and amount != 0 else None,
        'can_undo': bool(row['status'] == 'active' and history and
                         history[0]['action'] == 'reverse' and
                         history[0]['after_revision'] == row['amount_revision'] and
                         Decimal(history[0]['after_amount']) == amount),
        'history': history,
    }


def _change_transaction_amount(tx_id: int, body: AmountChangeRequest,
                               user: dict, action: str, change_id: Optional[int] = None):
    expected_amount = _checked_amount(body.expected_amount)
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            operation_lock = int.from_bytes(
                hashlib.sha256(body.operation_id.bytes).digest()[:8], 'big', signed=True)
            cur.execute('SELECT pg_advisory_xact_lock(%s)', (operation_lock,))
            # Row lock serializes independent clicks and response retries on this transaction.
            cur.execute('''SELECT id, amount, original_amount, amount_revision, status,
                                  transaction_type FROM transactions
                           WHERE id=%s AND user_id=%s FOR UPDATE''', (tx_id, user['id']))
            tx = cur.fetchone()
            if not tx:
                raise HTTPException(404, 'Transaction not found')
            cur.execute('''SELECT * FROM transaction_amount_changes WHERE operation_id=%s''',
                        (str(body.operation_id),))
            prior = cur.fetchone()
            if prior:
                if (prior['transaction_id'] != tx_id or
                    prior['actor_user_id'] != user.get('auth_id', user['id']) or
                    prior['action'] != action or prior['before_revision'] != body.expected_revision or
                    prior['before_amount'] != expected_amount or prior['undo_of'] != change_id):
                    raise HTTPException(409, 'Operation ID was used for a different request')
                return _amount_event(prior, tx['transaction_type'], replayed=True)
            if tx['status'] != 'active':
                raise HTTPException(409, 'Only active transactions can have their sign corrected')
            if tx['amount_revision'] != body.expected_revision or tx['amount'] != expected_amount:
                raise HTTPException(409, 'Amount changed; reopen the preview before saving')
            if tx['amount'] == 0:
                raise HTTPException(422, 'Zero has no sign to reverse')

            if action == 'undo':
                cur.execute('''SELECT id, transaction_id, user_id, action, after_amount,
                                      after_revision FROM transaction_amount_changes
                               WHERE id=%s''', (change_id,))
                target = cur.fetchone()
                if (not target or target['transaction_id'] != tx_id or
                    target['user_id'] != user['id'] or target['action'] != 'reverse'):
                    raise HTTPException(404, 'Reversal not found')
                # Undo must remove exactly the last reversal; it cannot erase later edits.
                if target['after_revision'] != tx['amount_revision'] or target['after_amount'] != tx['amount']:
                    raise HTTPException(409, 'A later amount change prevents this undo')
                next_amount = -tx['amount']
            else:
                next_amount = -tx['amount']

            original = tx['original_amount'] if tx['original_amount'] is not None else tx['amount']
            cur.execute('''UPDATE transactions
                           SET amount=%s, original_amount=COALESCE(original_amount, amount),
                               amount_revision=amount_revision+1
                           WHERE id=%s AND user_id=%s RETURNING amount_revision''',
                        (next_amount, tx_id, user['id']))
            revision = cur.fetchone()['amount_revision']
            cur.execute('''INSERT INTO transaction_amount_changes
                           (transaction_id, user_id, actor_user_id, operation_id, action,
                            undo_of, original_amount, before_amount, after_amount,
                            before_revision, after_revision)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *''',
                        (tx_id, user['id'], user.get('auth_id', user['id']), str(body.operation_id),
                         action, change_id, original, tx['amount'], next_amount,
                         tx['amount_revision'], revision))
            return _amount_event(cur.fetchone(), tx['transaction_type'])


@app.post('/api/transactions/{tx_id}/amount/reverse')
def reverse_transaction_amount(tx_id: int, body: AmountChangeRequest,
                               user: dict = Depends(require_edit)):
    return _change_transaction_amount(tx_id, body, user, 'reverse')


@app.post('/api/transactions/{tx_id}/amount/undo')
def undo_transaction_amount(tx_id: int, body: AmountUndoRequest,
                            user: dict = Depends(require_edit)):
    return _change_transaction_amount(tx_id, body, user, 'undo', body.change_id)

@app.get("/api/stats")
def get_stats(
    source: str = "", tag: List[str] = Query([]), tag_match: str = "any",
    search: str = "", date_from: str = "", date_to: str = "", import_file: str = "",
    card_last4: str = "", user: dict = Depends(get_current_user)
):
    uid = user["id"]
    where, params = ["t.status = 'active'", "t.user_id = %s"], [uid]
    if source:      where.append("t.source = %s");      params.append(source)
    if tag:
        where, params = _apply_tag_filter(where, params, tag, tag_match, uid)
    if date_from:   where.append("t.date >= %s");       params.append(date_from)
    if date_to:     where.append("t.date <= %s");       params.append(date_to)
    if search:      where.append("t.description ILIKE %s"); params.append(f"%{search}%")
    if import_file: where.append("t.import_file = %s"); params.append(import_file)
    if card_last4:
        where.append("t.import_file IN (SELECT filename FROM uploaded_files WHERE user_id=%s AND card_last4=%s)")
        params.extend([uid, card_last4])

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Exclude transactions whose primary tag (or ancestor) is excluded
            cur.execute(
                "SELECT id FROM tags WHERE user_id=%s AND excluded_from_spending=TRUE",
                (uid,)
            )
            excluded_tag_ids = [r["id"] for r in cur.fetchall()]
            if excluded_tag_ids:
                where.append("""(t.primary_tag_id IS NULL OR NOT EXISTS (
                    WITH RECURSIVE chain AS (
                        SELECT t.primary_tag_id AS cid
                        UNION ALL
                        SELECT tg.group_tag_id FROM tags tg JOIN chain c ON tg.id = c.cid
                        WHERE tg.group_tag_id IS NOT NULL
                    )
                    SELECT 1 FROM chain WHERE cid = ANY(%s)
                ))""")
                params.append(excluded_tag_ids)

            wc = " AND ".join(where)
            cur.execute(f"SELECT TO_CHAR(t.date,'YYYY-MM') AS month, SUM(t.amount)::float AS total FROM transactions t WHERE {wc} GROUP BY month ORDER BY month", params)
            by_month = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT t.source, SUM(t.amount)::float AS total, COUNT(*)::int AS count FROM transactions t WHERE {wc} GROUP BY t.source ORDER BY total DESC", params)
            by_source = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT COALESCE(SUM(t.amount),0)::float AS total, COUNT(*)::int AS count FROM transactions t WHERE {wc}", params)
            summary = dict(cur.fetchone())
            # Tag breakdown — primary tag + ancestor chain, only non-excluded
            cur.execute(f"""
                SELECT tg.name AS tag, SUM(t.amount)::float AS total, COUNT(DISTINCT t.id)::int AS count
                FROM transactions t
                JOIN LATERAL (
                    WITH RECURSIVE chain AS (
                        SELECT pt.id AS cid, pt.name, pt.excluded_from_spending
                        FROM tags pt WHERE pt.id = t.primary_tag_id
                        UNION ALL
                        SELECT g.id, g.name, g.excluded_from_spending
                        FROM tags g JOIN chain c ON g.id = (SELECT group_tag_id FROM tags WHERE id = c.cid)
                    )
                    SELECT cid, name, excluded_from_spending FROM chain
                ) tg ON TRUE
                WHERE {wc} AND t.primary_tag_id IS NOT NULL AND tg.excluded_from_spending = FALSE
                GROUP BY tg.name
                ORDER BY total DESC
            """, params)
            by_tag = [dict(r) for r in cur.fetchall()]
            # Untagged total (no primary tag)
            cur.execute(f"""
                SELECT COALESCE(SUM(t.amount),0)::float AS total, COUNT(*)::int AS count
                FROM transactions t
                WHERE {wc} AND t.primary_tag_id IS NULL
            """, params)
            untagged = dict(cur.fetchone())
            # Tag hierarchy — from explicit group_tag_id
            cur.execute("""
                SELECT child.name AS child_tag, parent.name AS parent_tag
                FROM tags child
                JOIN tags parent ON parent.id = child.group_tag_id
                WHERE child.user_id = %s AND child.excluded_from_spending = FALSE
                  AND parent.excluded_from_spending = FALSE
            """, [uid])
            tag_hierarchy = [dict(r) for r in cur.fetchall()]

    return {**summary, "by_month": by_month,
            "by_source": by_source, "by_tag": by_tag, "untagged": untagged,
            "tag_hierarchy": tag_hierarchy}


def _financial_ledger_rows(uid, source="", tag=(), tag_match="any", search="",
                           date_from="", date_to="", import_file="", card_last4="",
                           transaction_type=None):
    """Use the same active and primary-tag-ancestor exclusion semantics as /api/stats."""
    where, params = ["t.status='active'", "t.user_id=%s"], [uid]
    if source: where.append("t.source=%s"); params.append(source)
    if tag: _apply_tag_filter(where, params, tag, tag_match, uid)
    if date_from: where.append("t.date >= %s"); params.append(date_from)
    if date_to: where.append("t.date <= %s"); params.append(date_to)
    if search: where.append("t.description ILIKE %s"); params.append(f"%{search}%")
    if import_file: where.append("t.import_file=%s"); params.append(import_file)
    if card_last4:
        where.append("t.import_file IN (SELECT filename FROM uploaded_files WHERE user_id=%s AND card_last4=%s)")
        params.extend([uid, card_last4])
    if transaction_type == "unreviewed":
        where.append("t.transaction_type IS NULL")
    elif transaction_type:
        where.append("t.transaction_type=%s"); params.append(transaction_type)
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"""
                SELECT t.id, t.date::text AS date, t.description, t.amount,
                       t.source, t.import_file, t.transaction_type, t.needs_review,
                       pt.name AS primary_tag,
                       EXISTS (
                           WITH RECURSIVE chain AS (
                               SELECT t.primary_tag_id AS cid WHERE t.primary_tag_id IS NOT NULL
                               UNION ALL
                               SELECT tg.group_tag_id FROM tags tg JOIN chain c ON tg.id=c.cid
                               WHERE tg.group_tag_id IS NOT NULL
                           )
                           SELECT 1 FROM chain JOIN tags excluded_tag ON excluded_tag.id=chain.cid
                           WHERE excluded_tag.user_id=t.user_id AND excluded_tag.excluded_from_spending=TRUE
                       ) AS excluded
                FROM transactions t
                LEFT JOIN tags pt ON pt.id=t.primary_tag_id AND pt.user_id=t.user_id
                WHERE {' AND '.join(where)}
                ORDER BY t.date DESC, t.id DESC
            """, params)
            return [dict(row) for row in cur.fetchall()]


@app.get("/api/analytics")
def get_analytics(
    source: str = "", tag: List[str] = Query([]), tag_match: str = "any",
    search: str = "", date_from: str = "", date_to: str = "", import_file: str = "",
    card_last4: str = "",
    transaction_type: Optional[Literal["expense", "income", "transfer", "refund", "unreviewed"]] = None,
    user: dict = Depends(get_current_user),
):
    rows = _financial_ledger_rows(user["id"], source, tag, tag_match, search,
                                  date_from, date_to, import_file, card_last4, transaction_type)
    return summarize_ledger(rows)


@app.get("/api/transaction-type-preview")
def get_transaction_type_preview(
    limit: int = Query(50, ge=1, le=100), offset: int = Query(0, ge=0),
    user: dict = Depends(get_current_user),
):
    """Historical mapping proposal only. No endpoint applies this proposal."""
    from collections import Counter
    rows = _financial_ledger_rows(user["id"])
    untyped = [row for row in rows if row["transaction_type"] is None]
    suggestions, ambiguous = Counter(), Counter()
    for row in untyped:
        candidate, reason = preview_candidate(row)
        (suggestions if candidate else ambiguous)[candidate or reason] += 1
    sample = []
    for row in untyped[offset:offset + limit]:
        candidate, reason = preview_candidate(row)
        sample.append({"id": row["id"], "date": row["date"],
                       "description": row["description"], "amount": float(row["amount"]),
                       "primary_tag": row["primary_tag"], "excluded": row["excluded"],
                       "suggested_type": candidate, "reason": reason})
    return {"dry_run": True, "requires_review": True, "applied": 0,
            "active_count": len(rows), "already_typed_count": len(rows) - len(untyped),
            "untyped_count": len(untyped), "candidate_counts": dict(suggestions),
            "ambiguous_reasons": dict(ambiguous), "sample": sample,
            "limit": limit, "offset": offset}


class TransactionTypeUpdate(BaseModel):
    transaction_type: Optional[Literal["expense", "income", "transfer", "refund"]]
    expected_revision: int = Field(ge=0)


@app.put("/api/transactions/{tx_id}/type")
def update_transaction_type(tx_id: int, body: TransactionTypeUpdate,
                            user: dict = Depends(require_edit)):
    """Only a person can commit a type; revision prevents stale review edits."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT transaction_type, type_revision, amount FROM transactions
                           WHERE id=%s AND user_id=%s FOR UPDATE""", (tx_id, user["id"]))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Transaction not found")
            old_type, revision, amount = row
            if revision != body.expected_revision:
                raise HTTPException(409, "Transaction type was changed; reload before saving")
            cur.execute("""UPDATE transactions
                           SET transaction_type=%s, type_revision=type_revision+1,
                               type_updated_at=NOW()
                           WHERE id=%s AND user_id=%s
                           RETURNING type_revision, type_updated_at""",
                        (body.transaction_type, tx_id, user["id"]))
            new_revision, updated_at = cur.fetchone()
    return {"ok": True, "id": tx_id, "previous_type": old_type,
            "transaction_type": body.transaction_type, "type_revision": new_revision,
            "type_updated_at": updated_at,
            "type_sign_issue": sign_issue(body.transaction_type, amount)}

# ── Source update ─────────────────────────────────────────────────────────────────
class SourceUpdate(BaseModel):
    source: Optional[str] = None

@app.patch("/api/transactions/{tx_id}")
def update_transaction(tx_id: int, body: SourceUpdate, user: dict = Depends(require_edit)):
    if body.source is None:
        raise HTTPException(400, "Nothing to update")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE transactions SET source = %s
                WHERE id = %s AND user_id = %s RETURNING id
            """, (body.source, tx_id, user["id"]))
            if cur.rowcount == 0:
                raise HTTPException(404, "Transaction not found")
    return {"ok": True, "id": tx_id}

# ── Soft-delete ───────────────────────────────────────────────────────────────────
@app.delete("/api/transactions/{tx_id}")
def delete_transaction(tx_id: int, user: dict = Depends(require_edit)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET status='deleted', correction_revision=correction_revision+1 WHERE id=%s AND user_id=%s AND status='active' RETURNING id",
                (tx_id, user["id"]))
            if cur.rowcount == 0:
                raise HTTPException(404, "Transaction not found")
    return {"ok": True, "id": tx_id}

class BulkDelete(BaseModel):
    ids: List[int]

@app.post("/api/transactions/bulk-delete")
def bulk_delete_transactions(body: BulkDelete, user: dict = Depends(require_edit)):
    if not body.ids: raise HTTPException(400, "No IDs provided")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET status='deleted', correction_revision=correction_revision+1 WHERE id=ANY(%s) AND user_id=%s AND status='active'",
                (body.ids, user["id"]))
            deleted = cur.rowcount
    return {"ok": True, "deleted": deleted}

# ── Restore ───────────────────────────────────────────────────────────────────────
@app.post("/api/transactions/{tx_id}/restore")
def restore_transaction(tx_id: int, user: dict = Depends(require_edit)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET status='active', dedup_of=NULL, correction_revision=correction_revision+1 WHERE id=%s AND user_id=%s RETURNING id",
                (tx_id, user["id"]))
            if cur.rowcount == 0:
                raise HTTPException(404, "Transaction not found")
    return {"ok": True, "id": tx_id}

class BulkRestore(BaseModel):
    ids: List[int]

@app.post("/api/transactions/bulk-restore")
def bulk_restore_transactions(body: BulkRestore, user: dict = Depends(require_edit)):
    if not body.ids: raise HTTPException(400, "No IDs provided")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET status='active', dedup_of=NULL, correction_revision=correction_revision+1 WHERE id=ANY(%s) AND user_id=%s",
                (body.ids, user["id"]))
            restored = cur.rowcount
    return {"ok": True, "restored": restored}

@app.post("/api/transactions/purge-deduped")
def purge_deduped_transactions(user: dict = Depends(require_edit)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM transactions WHERE user_id=%s AND status='deduped'",
                (user["id"],))
            purged = cur.rowcount
    return {"ok": True, "purged": purged}

# ── Upload ────────────────────────────────────────────────────────────────────────
UPLOAD_STALE_SECONDS = 120


def _set_upload_job(job_id, user_id, status, result=None):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""UPDATE upload_jobs SET status=%s, result_json=%s, updated_at=NOW()
                WHERE id=%s AND user_id=%s AND status IN
                ('processing','parsing','categorizing','saving')""",
                (status, json.dumps(result) if result is not None else None, job_id, user_id))
            return cur.rowcount == 1


def _mark_stale_upload_jobs(cur, user_id):
    # The file bytes live only in a worker process. A restart requires re-upload.
    cur.execute("""UPDATE upload_jobs SET status='interrupted', updated_at=NOW(),
        result_json=json_build_object('status','error','filename',filename,
            'message','Import was interrupted. Re-upload this file to retry safely.',
            'new',0,'dupes',0)::text
        WHERE user_id=%s AND status IN ('pending','processing','parsing','categorizing','saving')
          AND updated_at < NOW() - INTERVAL '1 second' * %s""",
        (user_id, UPLOAD_STALE_SECONDS))


def _finish_upload_job(cur, job_id, user_id, status, result):
    cur.execute("""UPDATE upload_jobs SET status=%s, result_json=%s, updated_at=NOW()
        WHERE id=%s AND user_id=%s AND status IN
        ('processing','parsing','categorizing','saving')""",
        (status, json.dumps(result), job_id, user_id))
    if cur.rowcount != 1:
        raise RuntimeError('Upload was interrupted before saving; no rows saved')


def _process_upload_job(job_id: str, user_id: int, filename: str, content: bytes, force: bool):
    """Runs in a background thread. Processes one file and updates upload_jobs on completion."""
    def set_status(status, result=None):
        return _set_upload_job(job_id, user_id, status, result)

    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""UPDATE upload_jobs SET status='processing', updated_at=NOW()
                WHERE id=%s AND user_id=%s AND status='pending' RETURNING id""", (job_id,user_id))
            claimed = cur.fetchone() is not None
    if not claimed:
        return

    heartbeat_stop = threading.Event()
    def heartbeat():
        while not heartbeat_stop.wait(15):
            try:
                with db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""UPDATE upload_jobs SET updated_at=NOW()
                            WHERE id=%s AND user_id=%s AND status IN
                            ('processing','parsing','categorizing','saving')""", (job_id,user_id))
            except Exception as exc:
                print(f'[upload_job:{job_id}] heartbeat: {type(exc).__name__}: {exc}')
    heartbeat_thread = threading.Thread(target=heartbeat, daemon=True)
    heartbeat_thread.start()

    try:
        file_hash = hashlib.md5(content).hexdigest()

        # 1. Quick DB checks — release connection before slow work
        if not force:
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM uploaded_files WHERE user_id=%s AND file_hash=%s",
                                (user_id, file_hash))
                    if cur.fetchone():
                        set_status("done", {"filename": filename, "status": "already_imported",
                                            "message": "File was already imported", "new": 0, "dupes": 0})
                        return

        set_status('parsing')
        rows, source, error = parse_file_bytes(content, filename)
        if error:
            set_status("error", {"filename": filename, "status": "error",
                                "message": error, "new": 0, "dupes": 0})
            return

        for r in rows:
            r["description"] = clean_description(r["description"])

        set_status('categorizing')
        # 2. Snapshot owner-scoped guidance and human corrections; release DB for AI.
        guide, history = load_categorization_context(user_id)
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM tags WHERE user_id=%s ORDER BY name", (user_id,))
                tag_list = [r[0] for r in cur.fetchall()]
        decisions = assign_tags_with_gpt(rows, tag_list, guide, history)
        if len(decisions) != len(rows):
            raise ValueError('Categorization returned the wrong row count')
        gpt_tagged = needs_review = 0
        set_status('saving')

        # 4. Insert transactions, then tags
        dedup_keys = [r["dedup_key"] for r in rows]
        legacy_keys = [key for r in rows for key in
                       (r.get('legacy_dedup_keys') or [r.get('legacy_dedup_key')]) if key]
        all_keys = list(set(dedup_keys + legacy_keys))
        account_key = rows[0].get('account_key')
        account_last4 = account_key.split(':')[-1] if account_key else None
        with db() as conn:
            with conn.cursor() as cur:
                # Serialize one owner's final writes across workers, including
                # overlapping statements. Keep AI calls outside this short lock.
                lock_key = int.from_bytes(
                    hashlib.sha256(f'upload-owner:{user_id}'.encode()).digest()[:8],
                    'big', signed=True)
                cur.execute('SELECT pg_advisory_xact_lock(%s)', (lock_key,))
                cur.execute("SELECT filename,card_last4 FROM uploaded_files WHERE user_id=%s AND file_hash=%s",
                            (user_id, file_hash))
                row = cur.fetchone()
                if row and not force:
                    _finish_upload_job(cur, job_id, user_id, 'done',
                        {'filename':filename,'status':'already_imported',
                         'message':'File was already imported','new':0,'dupes':0})
                    return
                import_name = row[0] if row else filename
                if row and row[1] and account_last4 and row[1] != account_last4:
                    raise ValueError('Printed account number disagrees with saved upload metadata')

                imported_keys = set()
                if row:
                    cur.execute("""SELECT COUNT(*) FROM uploaded_files
                        WHERE user_id=%s AND filename=%s""", (user_id, import_name))
                    if cur.fetchone()[0] > 1:
                        raise ValueError('Cannot safely reimport: multiple uploads share this filename')
                    cur.execute("""SELECT dedup_key FROM transactions WHERE user_id=%s
                        AND import_file=%s AND dedup_key=ANY(%s)""",
                        (user_id, import_name, all_keys))
                    imported_keys = {r[0] for r in cur.fetchall()}
                cur.execute("SELECT dedup_key FROM transactions WHERE user_id=%s AND dedup_key=ANY(%s) AND status='active'",
                            (user_id, all_keys))
                existing_keys = {r[0] for r in cur.fetchall()}

                new_count = dupe_count = skipped = possible_overlap = 0
                insert_rows = []
                decision_by_key = {}
                for r, decision in zip(rows, decisions):
                    row_legacy_keys = [key for key in
                                       (r.get('legacy_dedup_keys') or [r.get('legacy_dedup_key')]) if key]
                    if r['dedup_key'] in imported_keys or any(key in imported_keys for key in row_legacy_keys):
                        skipped += 1
                        continue
                    is_dupe   = r["dedup_key"] in existing_keys
                    legacy_overlap = (not is_dupe and any(key in existing_keys for key in row_legacy_keys))
                    tx_status = "deduped" if is_dupe else "active"
                    if legacy_overlap:
                        possible_overlap += 1
                        decision = review('Possible match to an older import with unknown account; verify this row.')
                    insert_rows.append((user_id, r["date"], r["description"],
                                        r["amount"], r["source"], r["dedup_key"],
                                        tx_status, r["dedup_key"] if is_dupe else None, import_name))
                    decision_by_key[r["dedup_key"]] = decision
                    if tx_status == "active":
                        new_count += 1
                    else:
                        dupe_count += 1

                returned = []
                if insert_rows:
                    returned = psycopg2.extras.execute_values(cur, """
                        INSERT INTO transactions
                            (user_id, date, description, amount, source,
                             dedup_key, status, dedup_of, import_file)
                        VALUES %s RETURNING id, dedup_key
                    """, insert_rows, fetch=True)

                # Resolve only categories that still exist; do not resurrect deleted tags.
                cur.execute("SELECT name, id FROM tags WHERE user_id=%s", (user_id,))
                tag_ids = dict(cur.fetchall())
                for tx_id, dk in returned:
                    decision = decision_by_key[dk]
                    if decision["primary_tag"] and decision["primary_tag"] not in tag_ids:
                        decision = review("The suggested category was removed during import; choose a category.")
                    if dk not in existing_keys:
                        if decision["needs_review"]:
                            needs_review += 1
                        elif decision["primary_tag"]:
                            gpt_tagged += 1
                    cur.execute("""
                        UPDATE transactions SET primary_tag_id=%s, primary_migration_status='auto',
                            needs_review=%s, suggested_tag_id=%s, categorization_reason=%s,
                            categorization_confidence=%s, categorization_example_ids=%s
                        WHERE id=%s AND user_id=%s
                    """, (tag_ids.get(decision["primary_tag"]), decision["needs_review"],
                          tag_ids.get(decision["suggested_tag"]), decision["reason"],
                          decision["confidence"], decision["example_ids"], tx_id, user_id))

                if row:
                    cur.execute("""UPDATE uploaded_files
                        SET tx_new=COALESCE(tx_new,0)+%s, tx_dupes=COALESCE(tx_dupes,0)+%s,
                            card_last4=COALESCE(NULLIF(card_last4,''),%s)
                        WHERE user_id=%s AND file_hash=%s""",
                        (new_count, dupe_count, account_last4, user_id, file_hash))
                    new_upload_id = None
                else:
                    cur.execute("""INSERT INTO uploaded_files
                        (user_id, filename, file_hash, source, tx_new, tx_dupes, card_last4)
                        VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (user_id, filename, file_hash, source, new_count, dupe_count, account_last4))
                    new_upload_id = cur.fetchone()[0]

                _finish_upload_job(cur, job_id, user_id, 'done',
                    {'filename': filename, 'file_hash': file_hash,
                     'new_upload_id': new_upload_id, 'status': 'ok', 'source': source,
                     'new': new_count, 'dupes': dupe_count, 'skipped': skipped,
                     'possible_overlap': possible_overlap,
                     'needs_review': needs_review, 'gpt_tagged': gpt_tagged})
    except Exception as e:
        print(f"[upload_job:{job_id}] {type(e).__name__}: {e}")
        try:
            set_status("error", {"filename": filename, "status": "error",
                                 "message": str(e), "new": 0, "dupes": 0})
        except Exception as update_error:
            print(f'[upload_job:{job_id}] cannot save error: {update_error}')
    finally:
        heartbeat_stop.set()
        heartbeat_thread.join(timeout=1)


@app.post("/api/upload")
async def upload_files(files: List[UploadFile] = File(...),
                       force: bool = False,
                       expected_file_hash: Optional[str] = None,
                       user: dict = Depends(require_edit)):
    user_id = user["id"]
    if expected_file_hash is not None:
        if not force or len(files) != 1 or not re.fullmatch(r'[0-9a-f]{32}', expected_file_hash):
            raise HTTPException(400, 'Choose one existing file to reimport')
        selected = files[0]
        content = await selected.read()
        await selected.seek(0)
        if hashlib.md5(content).hexdigest() != expected_file_hash:
            raise HTTPException(400, 'Selected file does not match this upload record')
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT 1 FROM uploaded_files WHERE user_id=%s AND file_hash=%s',
                            (user_id, expected_file_hash))
                if not cur.fetchone():
                    raise HTTPException(404, 'Upload record not found')
    jobs = []
    for f in files:
        content = await f.read()
        job_id  = str(uuid.uuid4())
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO upload_jobs (id, user_id, filename, file_hash, status)
                    VALUES (%s,%s,%s,%s,'pending')""",
                    (job_id, user_id, f.filename, hashlib.md5(content).hexdigest()))
        threading.Thread(target=_process_upload_job,
                         args=(job_id, user_id, f.filename, content, force),
                         daemon=True).start()
        jobs.append({"job_id": job_id, "filename": f.filename})
    return {"jobs": jobs}


@app.get('/api/upload/jobs')
def list_upload_jobs(user: dict = Depends(get_current_user),
                     limit: int = Query(25, ge=1, le=100),
                     filename: Optional[str] = None):
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            _mark_stale_upload_jobs(cur, user['id'])
            cur.execute("""SELECT id,filename,status,result_json,created_at,updated_at
                FROM upload_jobs WHERE user_id=%s AND (%s='' OR filename=%s)
                ORDER BY created_at DESC LIMIT %s""",
                (user['id'],filename or '',filename or '',limit))
            return {'jobs':[{'job_id':r['id'],'filename':r['filename'],
                'status':r['status'],
                'result':json.loads(r['result_json']) if r['result_json'] else None,
                'created_at':r['created_at'].isoformat(),
                'updated_at':r['updated_at'].isoformat()} for r in cur.fetchall()]}


@app.get("/api/upload/status/{job_id}")
def get_upload_job_status(job_id: str, user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor() as cur:
            _mark_stale_upload_jobs(cur, user['id'])
            cur.execute(
                "SELECT status, result_json FROM upload_jobs WHERE id=%s AND user_id=%s",
                (job_id, user["id"]))
            row = cur.fetchone()
    if not row:
        raise HTTPException(404, "Job not found")
    return {"status": row[0], "result": json.loads(row[1]) if row[1] else None}

# ── Tags ──────────────────────────────────────────────────────────────────────────
@app.get("/api/tags")
def get_tags(user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT t.name, t.excluded_from_spending,
                       g.name AS group_tag,
                       (
                         WITH RECURSIVE descendants AS (
                           SELECT t.id AS id
                           UNION ALL
                           SELECT c.id FROM tags c JOIN descendants d ON c.group_tag_id = d.id
                         )
                         SELECT COUNT(*) FROM transactions tx
                         WHERE tx.primary_tag_id IN (SELECT id FROM descendants)
                           AND tx.status = 'active'
                       ) AS tx_count
                FROM tags t
                LEFT JOIN tags g ON g.id = t.group_tag_id
                WHERE t.user_id=%s ORDER BY t.name
            """, (user["id"],))
            return {"tags": [dict(r) for r in cur.fetchall()]}

class TagCreate(BaseModel):
    name: str

@app.post("/api/tags", status_code=201)
def create_tag(body: TagCreate, user: dict = Depends(require_edit)):
    name = body.name.strip()
    if not name: raise HTTPException(400, "Tag name cannot be empty")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO tags (user_id, name) VALUES (%s,%s) ON CONFLICT DO NOTHING RETURNING name",
                (user["id"], name)
            )
            if cur.rowcount == 0: raise HTTPException(409, "Tag already exists")
    return {"ok": True, "name": name}

@app.delete("/api/tags")
def delete_tag(name: str, user: dict = Depends(require_edit)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM tags WHERE user_id=%s AND name=%s RETURNING name",
                (uid, name)
            )
            if cur.rowcount == 0: raise HTTPException(404, "Tag not found")
    return {"ok": True, "name": name}

class TagExclusionToggle(BaseModel):
    name: str
    excluded: bool

@app.patch("/api/tags/exclusion")
def toggle_tag_exclusion(body: TagExclusionToggle, user: dict = Depends(require_edit)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tags SET excluded_from_spending=%s WHERE user_id=%s AND name=%s RETURNING name",
                (body.excluded, user["id"], body.name)
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Tag not found")
    return {"ok": True, "name": body.name, "excluded": body.excluded}

class TagGroupUpdate(BaseModel):
    name: str
    group_tag: Optional[str] = None  # null to clear

@app.patch("/api/tags/group")
def set_tag_group(body: TagGroupUpdate, user: dict = Depends(require_edit)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            # Resolve child tag id
            cur.execute("SELECT id FROM tags WHERE user_id=%s AND name=%s", (uid, body.name))
            child_row = cur.fetchone()
            if not child_row:
                raise HTTPException(404, "Tag not found")
            child_id = child_row[0]

            if body.group_tag:
                group_name = body.group_tag.strip()
                if group_name == body.name:
                    raise HTTPException(400, "A tag cannot be its own group")
                # Resolve (or create) the group tag
                cur.execute(
                    "INSERT INTO tags (user_id, name) VALUES (%s,%s) "
                    "ON CONFLICT (user_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                    (uid, group_name)
                )
                group_id = cur.fetchone()[0]
                # Prevent cycles: walk up from group_id to ensure child_id is not an ancestor
                cur.execute("""
                    WITH RECURSIVE ancestors AS (
                        SELECT group_tag_id AS id FROM tags WHERE id = %s AND group_tag_id IS NOT NULL
                        UNION ALL
                        SELECT t.group_tag_id FROM tags t JOIN ancestors a ON t.id = a.id
                        WHERE t.group_tag_id IS NOT NULL
                    )
                    SELECT 1 FROM ancestors WHERE id = %s LIMIT 1
                """, (group_id, child_id))
                if cur.fetchone():
                    raise HTTPException(400, "Circular grouping not allowed")

                cur.execute("UPDATE tags SET group_tag_id=%s WHERE id=%s", (group_id, child_id))

                # Remove explicit ancestor tags from transactions that have this child or its descendants
                cur.execute("""
                    WITH RECURSIVE descendants AS (
                        SELECT %s AS id
                        UNION ALL
                        SELECT t.id FROM tags t JOIN descendants d ON t.group_tag_id = d.id
                    ),
                    ancestors AS (
                        SELECT %s AS id
                        UNION ALL
                        SELECT t.group_tag_id FROM tags t JOIN ancestors a ON t.id = a.id
                        WHERE t.group_tag_id IS NOT NULL
                    )
                    DELETE FROM transaction_tags tt
                    WHERE tt.tag_id IN (SELECT id FROM ancestors WHERE id != %s)
                      AND tt.transaction_id IN (
                          SELECT transaction_id FROM transaction_tags WHERE tag_id IN (SELECT id FROM descendants)
                      )
                """, (child_id, group_id, child_id))
            else:
                cur.execute("UPDATE tags SET group_tag_id=NULL WHERE id=%s", (child_id,))

    return {"ok": True, "name": body.name, "group_tag": body.group_tag}

class TagRename(BaseModel):
    old_name: str
    new_name: str

@app.patch("/api/tags")
def rename_tag(body: TagRename, user: dict = Depends(require_edit)):
    uid = user["id"]
    old_name = body.old_name.strip()
    new_name = body.new_name.strip()
    if not new_name: raise HTTPException(400, "Tag name cannot be empty")
    if old_name == new_name: return {"ok": True, "name": new_name}
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE tags SET name=%s WHERE user_id=%s AND name=%s RETURNING id",
                (new_name, uid, old_name)
            )
            if cur.rowcount == 0: raise HTTPException(404, "Tag not found")
    return {"ok": True, "old_name": old_name, "name": new_name}

class TagsUpdate(BaseModel):
    tags: List[str]

@app.put("/api/transactions/{tx_id}/tags")
def update_transaction_tags(tx_id: int, body: TagsUpdate, user: dict = Depends(require_edit)):
    uid = user["id"]
    tag_names = [t.strip() for t in body.tags if t.strip()]
    with db() as conn:
        with conn.cursor() as cur:
            # Verify ownership
            cur.execute("SELECT id FROM transactions WHERE id=%s AND user_id=%s", (tx_id, uid))
            if not cur.fetchone():
                raise HTTPException(404, "Transaction not found")
            # Upsert tags and resolve IDs
            tag_ids = []
            for name in tag_names:
                cur.execute(
                    "INSERT INTO tags (user_id, name) VALUES (%s,%s) ON CONFLICT (user_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                    (uid, name)
                )
                tag_ids.append(cur.fetchone()[0])
            # Replace all tags for this transaction
            cur.execute("DELETE FROM transaction_tags WHERE transaction_id=%s", (tx_id,))
            for tag_id in tag_ids:
                cur.execute(
                    "INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (tx_id, tag_id)
                )
    return {"ok": True, "id": tx_id, "tags": tag_names}

@app.delete("/api/transactions/{tx_id}/tags")
def clear_transaction_tags(tx_id: int, user: dict = Depends(require_edit)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM transactions WHERE id=%s AND user_id=%s", (tx_id, uid))
            if not cur.fetchone():
                raise HTTPException(404, "Transaction not found")
            cur.execute(
                """UPDATE transactions SET primary_tag_id=NULL, manually_corrected=TRUE,
                    correction_scope='transaction', correction_note='', correction_archived=FALSE,
                    correction_revision=correction_revision+1, needs_review=FALSE,
                    suggested_tag_id=NULL, categorization_reason='', categorization_confidence=NULL,
                    categorization_example_ids='{}' WHERE id=%s""",
                (tx_id,))
            cur.execute("DELETE FROM transaction_tags WHERE transaction_id=%s", (tx_id,))
    return {"ok": True, "id": tx_id, "tags": [], "primary_tag": None}

class PrimaryTagUpdate(BaseModel):
    primary_tag: Optional[str] = Field(default=None, max_length=200)  # null to clear
    correction_scope: Literal["transaction", "similar"] = "transaction"
    correction_note: str = Field(default="", max_length=1000)
    expected_correction_revision: Optional[int] = Field(default=None, ge=0)


@app.put("/api/transactions/{tx_id}/primary-tag")
def set_primary_tag(tx_id: int, body: PrimaryTagUpdate, user: dict = Depends(require_edit)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            # Verify ownership
            cur.execute("""SELECT id, primary_tag_id, correction_revision,
                                  manually_corrected, correction_archived, status
                           FROM transactions WHERE id=%s AND user_id=%s FOR UPDATE""", (tx_id, uid))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Transaction not found")
            old_primary_id = row[1]
            if body.expected_correction_revision is not None and (
                row[2] != body.expected_correction_revision or not row[3] or row[4] or row[5] != 'active'
            ):
                raise HTTPException(409, "Correction changed. Refresh the list and try again.")

            if body.primary_tag is None:
                # Clear primary tag — demote old primary to secondary
                if old_primary_id:
                    cur.execute(
                        "INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (tx_id, old_primary_id))
                cur.execute(
                    """UPDATE transactions SET primary_tag_id=NULL, manually_corrected=TRUE,
                        correction_scope=%s, correction_note=%s, correction_archived=FALSE,
                        correction_revision=correction_revision+1, needs_review=FALSE,
                        suggested_tag_id=NULL, categorization_reason='', categorization_confidence=NULL,
                        categorization_example_ids='{}' WHERE id=%s""",
                    (body.correction_scope, body.correction_note.strip(), tx_id))
                return {"ok": True, "id": tx_id, "primary_tag": None}

            tag_name = body.primary_tag.strip()
            if not tag_name:
                raise HTTPException(400, "Tag name cannot be empty")

            # Resolve (or create) the tag
            cur.execute(
                "INSERT INTO tags (user_id, name) VALUES (%s,%s) "
                "ON CONFLICT (user_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                (uid, tag_name))
            new_tag_id = cur.fetchone()[0]

            # If new primary was a secondary tag, remove it from transaction_tags
            cur.execute(
                "DELETE FROM transaction_tags WHERE transaction_id=%s AND tag_id=%s",
                (tx_id, new_tag_id))

            # If old primary exists and differs, demote it to secondary
            if old_primary_id and old_primary_id != new_tag_id:
                cur.execute(
                    "INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (tx_id, old_primary_id))

            # Set new primary
            cur.execute(
                """UPDATE transactions SET primary_tag_id=%s, manually_corrected=TRUE,
                    correction_scope=%s, correction_note=%s, correction_archived=FALSE,
                    correction_revision=correction_revision+1, needs_review=FALSE,
                    suggested_tag_id=NULL, categorization_reason='', categorization_confidence=NULL,
                    categorization_example_ids='{}' WHERE id=%s""",
                (new_tag_id, body.correction_scope, body.correction_note.strip(), tx_id))
    return {"ok": True, "id": tx_id, "primary_tag": tag_name}

class BulkTagUpdate(BaseModel):
    ids: List[int] = Field(min_length=1, max_length=1000)
    tag: str = Field(min_length=1, max_length=200)
    action: Literal["add", "remove", "set-primary"] = "add"
    correction_scope: Literal["transaction", "similar"] = "transaction"
    correction_note: str = Field(default="", max_length=1000)


@app.post("/api/transactions/bulk-tag")
def bulk_tag_transactions(body: BulkTagUpdate, user: dict = Depends(require_edit)):
    tag_name = body.tag.strip()
    if not tag_name:
        raise HTTPException(400, "Tag name required")
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, primary_tag_id FROM transactions WHERE user_id=%s AND id=ANY(%s)",
                        (uid, body.ids))
            owned = cur.fetchall()
            if not owned:
                raise HTTPException(404, "Transactions not found")
            ids = [row[0] for row in owned]
            if body.action == "remove":
                cur.execute("""
                    DELETE FROM transaction_tags tt USING tags tg
                    WHERE tg.id=tt.tag_id AND tg.user_id=%s AND tg.name=%s
                      AND tt.transaction_id=ANY(%s)
                """, (uid, tag_name, ids))
            else:
                cur.execute("""
                    INSERT INTO tags(user_id,name) VALUES(%s,%s)
                    ON CONFLICT(user_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id
                """, (uid, tag_name))
                tag_id = cur.fetchone()[0]
                for tx_id, old_primary in owned:
                    if body.action == "set-primary":
                        cur.execute("DELETE FROM transaction_tags WHERE transaction_id=%s AND tag_id=%s", (tx_id, tag_id))
                        if old_primary and old_primary != tag_id:
                            cur.execute("INSERT INTO transaction_tags VALUES(%s,%s) ON CONFLICT DO NOTHING", (tx_id, old_primary))
                        cur.execute("""
                            UPDATE transactions SET primary_tag_id=%s, manually_corrected=TRUE,
                                correction_scope=%s, correction_note=%s, correction_archived=FALSE,
                                correction_revision=correction_revision+1, needs_review=FALSE,
                                suggested_tag_id=NULL, categorization_reason='', categorization_confidence=NULL,
                                categorization_example_ids='{}' WHERE id=%s AND user_id=%s
                        """, (tag_id, body.correction_scope, body.correction_note.strip(), tx_id, uid))
                    else:
                        cur.execute("INSERT INTO transaction_tags VALUES(%s,%s) ON CONFLICT DO NOTHING", (tx_id, tag_id))
    return {"ok": True, "updated": len(ids)}

# ── Primary tag migration review ─────────────────────────────────────────────────
@app.get("/api/migration/primary-tags")
def get_migration_review(user: dict = Depends(require_owner)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT COUNT(*) AS n FROM transactions WHERE user_id=%s AND primary_migration_status='ambiguous'",
                (uid,))
            ambiguous_count = cur.fetchone()["n"]
            cur.execute(f"""
                SELECT t.id, t.date::text, t.description, t.amount::float, t.source,
                       pt.name AS primary_tag,
                       COALESCE(ARRAY(
                           SELECT tg.name FROM transaction_tags tt
                           JOIN tags tg ON tg.id = tt.tag_id
                           WHERE tt.transaction_id = t.id ORDER BY tg.name
                       ), '{{}}') AS secondary_tags
                FROM transactions t
                LEFT JOIN tags pt ON pt.id = t.primary_tag_id
                WHERE t.user_id = %s AND t.primary_migration_status = 'ambiguous'
                ORDER BY t.date DESC
            """, (uid,))
            transactions = [dict(r) for r in cur.fetchall()]
    return {"ambiguous_count": ambiguous_count, "transactions": transactions}

class MigrationPrimaryTagUpdate(BaseModel):
    primary_tag: str

@app.patch("/api/migration/primary-tags/{tx_id}")
def update_migration_primary_tag(tx_id: int, body: MigrationPrimaryTagUpdate,
                                  user: dict = Depends(require_owner)):
    uid = user["id"]
    tag_name = body.primary_tag.strip()
    if not tag_name:
        raise HTTPException(400, "Tag name cannot be empty")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT primary_tag_id FROM transactions WHERE id=%s AND user_id=%s", (tx_id, uid))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Transaction not found")
            old_primary_id = row[0]

            # Resolve tag
            cur.execute(
                "INSERT INTO tags (user_id, name) VALUES (%s,%s) "
                "ON CONFLICT (user_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                (uid, tag_name))
            new_tag_id = cur.fetchone()[0]

            # Remove new primary from secondary if present
            cur.execute("DELETE FROM transaction_tags WHERE transaction_id=%s AND tag_id=%s", (tx_id, new_tag_id))

            # Demote old primary to secondary if different
            if old_primary_id and old_primary_id != new_tag_id:
                cur.execute(
                    "INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (tx_id, old_primary_id))

            cur.execute(
                "UPDATE transactions SET primary_tag_id=%s, primary_migration_status='reviewed', manually_corrected=TRUE, correction_revision=correction_revision+1 WHERE id=%s",
                (new_tag_id, tx_id))
    return {"ok": True, "id": tx_id, "primary_tag": tag_name}

@app.post("/api/migration/primary-tags/finalize")
def finalize_migration(user: dict = Depends(require_owner)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET primary_migration_status='reviewed' "
                "WHERE user_id=%s AND primary_migration_status='ambiguous'",
                (user["id"],))
            updated = cur.rowcount
    return {"ok": True, "finalized": updated}

# ── Upload history ────────────────────────────────────────────────────────────────
@app.get("/api/uploads")
def get_uploads(user: dict = Depends(get_current_user), limit: int = 25, offset: int = 0):
    uid = user["id"]
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Total count
            cur.execute("SELECT COUNT(*) FROM uploaded_files WHERE user_id=%s", (uid,))
            total = cur.fetchone()["count"]
            # Paginated uploads (most recent first)
            cur.execute("""
                SELECT filename, file_hash, source, card_last4, tx_new, tx_dupes,
                       to_char(uploaded_at,'YYYY-MM-DD HH24:MI') as uploaded_at
                FROM uploaded_files WHERE user_id=%s
                ORDER BY uploaded_at DESC
                LIMIT %s OFFSET %s
            """, (uid, limit, offset))
            uploads = [dict(r) for r in cur.fetchall()]
            # Lightweight dropdown data (all uploads)
            cur.execute("""
                SELECT DISTINCT source FROM uploaded_files
                WHERE user_id=%s AND source IS NOT NULL ORDER BY source
            """, (uid,))
            all_sources = [r["source"] for r in cur.fetchall()]
            cur.execute("""
                SELECT DISTINCT card_last4, source FROM uploaded_files
                WHERE user_id=%s AND card_last4 IS NOT NULL
                ORDER BY source, card_last4
            """, (uid,))
            all_cards = [dict(r) for r in cur.fetchall()]
            cur.execute("""
                SELECT filename FROM uploaded_files
                WHERE user_id=%s ORDER BY uploaded_at DESC
            """, (uid,))
            all_filenames = [r["filename"] for r in cur.fetchall()]
            return {
                "uploads": uploads, "total": total,
                "all_sources": all_sources, "all_cards": all_cards,
                "all_filenames": all_filenames,
            }

class UploadRename(BaseModel):
    old_name: str
    new_name: str

class UploadDisplayName(BaseModel):
    job_id: str
    new_name: str = Field(min_length=1, max_length=200)

@app.patch("/api/uploads/display-name")
def name_new_upload(body: UploadDisplayName, user: dict = Depends(require_edit)):
    """Name one completed new import, without changing its parser or account identity."""
    new = body.new_name.strip()
    if not new or '/' in new or '\\' in new or any(ord(c) < 32 for c in new):
        raise HTTPException(400, "Invalid display filename")
    with db() as conn:
        with conn.cursor() as cur:
            lock_key = int.from_bytes(hashlib.sha256(f'upload-owner:{user["id"]}'.encode()).digest()[:8],
                                      'big', signed=True)
            cur.execute('SELECT pg_advisory_xact_lock(%s)', (lock_key,))
            cur.execute("SELECT status, result_json FROM upload_jobs WHERE id=%s AND user_id=%s FOR UPDATE",
                        (body.job_id, user["id"]))
            job = cur.fetchone()
            if not job:
                raise HTTPException(404, "Upload job not found")
            result = json.loads(job[1]) if job[1] else {}
            if job[0] != 'done' or result.get('status') != 'ok' or not result.get('file_hash') or not result.get('new_upload_id'):
                raise HTTPException(409, "Only a completed new import can be named")
            if result.get('display_name'):
                if result['display_name'] == new:
                    return {"ok": True, "old_name": result['filename'], "new_name": new, "updated": 0}
                raise HTTPException(409, "This import was already named")
            cur.execute("SELECT filename FROM uploaded_files WHERE id=%s AND user_id=%s AND file_hash=%s FOR UPDATE",
                        (result['new_upload_id'], user["id"], result['file_hash']))
            upload = cur.fetchone()
            if not upload:
                raise HTTPException(404, "Import record not found")
            old = upload[0]
            if old != result.get('filename'):
                raise HTTPException(409, "Import filename changed; review it in history")
            if not re.search(r'\.(pdf|csv)$', new, re.I) or new.rsplit('.', 1)[-1].lower() != old.rsplit('.', 1)[-1].lower():
                raise HTTPException(400, "Keep the original file extension")
            if old == new:
                result['display_name'] = new
                cur.execute("UPDATE upload_jobs SET result_json=%s WHERE id=%s AND user_id=%s",
                            (json.dumps(result), body.job_id, user['id']))
                return {"ok": True, "old_name": old, "new_name": new, "updated": 0}
            cur.execute("SELECT COUNT(*) FROM uploaded_files WHERE user_id=%s AND filename=%s",
                        (user["id"], old))
            if cur.fetchone()[0] != 1:
                raise HTTPException(409, "Original filename is shared by multiple imports; rename manually after review")
            cur.execute("SELECT 1 FROM uploaded_files WHERE user_id=%s AND filename=%s",
                        (user["id"], new))
            if cur.fetchone():
                raise HTTPException(409, "Display filename already exists")
            cur.execute("UPDATE uploaded_files SET filename=%s WHERE id=%s AND user_id=%s AND file_hash=%s",
                        (new, result['new_upload_id'], user["id"], result['file_hash']))
            cur.execute("UPDATE transactions SET import_file=%s WHERE user_id=%s AND import_file=%s",
                        (new, user["id"], old))
            updated = cur.rowcount
            result['display_name'] = new
            cur.execute("UPDATE upload_jobs SET result_json=%s WHERE id=%s AND user_id=%s",
                        (json.dumps(result), body.job_id, user['id']))
    return {"ok": True, "old_name": old, "new_name": new, "updated": updated}

@app.patch("/api/uploads/rename")
def rename_upload(body: UploadRename, user: dict = Depends(require_edit)):
    old, new, uid = body.old_name.strip(), body.new_name.strip(), user["id"]
    if not new:    raise HTTPException(400, "New name cannot be empty")
    if old == new: raise HTTPException(400, "New name is the same as old name")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM uploaded_files WHERE user_id=%s AND filename=%s",
                (uid, old))
            if not cur.fetchone():
                raise HTTPException(404, "Upload record not found")
            cur.execute(
                "UPDATE uploaded_files SET filename=%s WHERE user_id=%s AND filename=%s",
                (new, uid, old))
            cur.execute(
                "UPDATE transactions SET import_file=%s WHERE user_id=%s AND import_file=%s",
                (new, uid, old))
            updated = cur.rowcount
    return {"ok": True, "old_name": old, "new_name": new, "updated": updated}

class UploadSourceUpdate(BaseModel):
    filename: str
    source: str

@app.patch("/api/uploads/source")
def set_upload_source(body: UploadSourceUpdate, user: dict = Depends(require_edit)):
    source = body.source.strip()
    if not source:
        raise HTTPException(400, "Source cannot be empty")
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE uploaded_files SET source=%s WHERE user_id=%s AND filename=%s RETURNING filename",
                (source, uid, body.filename)
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Upload record not found")
            cur.execute(
                "UPDATE transactions SET source=%s WHERE user_id=%s AND import_file=%s",
                (source, uid, body.filename)
            )
    return {"ok": True, "filename": body.filename, "source": source}

class CardLast4Update(BaseModel):
    filename: str
    card_last4: str  # empty string = clear it

@app.patch("/api/uploads/card-last4")
def set_card_last4(body: CardLast4Update, user: dict = Depends(require_edit)):
    val = body.card_last4.strip() or None
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE uploaded_files SET card_last4=%s WHERE user_id=%s AND filename=%s RETURNING filename",
                (val, user["id"], body.filename)
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Upload record not found")
    return {"ok": True, "filename": body.filename, "card_last4": val}

@app.delete("/api/uploads")
def delete_upload(filename: str, user: dict = Depends(require_edit)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM uploaded_files WHERE user_id=%s AND filename=%s RETURNING id",
                (uid, filename))
            if cur.rowcount == 0: raise HTTPException(404, "Upload record not found")
            cur.execute("DELETE FROM transactions WHERE user_id=%s AND import_file=%s", (uid, filename))
            deleted = cur.rowcount
    return {"ok": True, "filename": filename, "deleted_transactions": deleted}

# ── Invite management ─────────────────────────────────────────────────────────────
@app.get("/api/invites")
def list_invites(user: dict = Depends(require_owner)):
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT i.email, i.role,
                       to_char(i.invited_at, 'YYYY-MM-DD') AS invited_at,
                       to_char(i.last_seen_at, 'YYYY-MM-DD HH24:MI') AS last_seen_at,
                       u.id IS NOT NULL AS has_account
                FROM invited_users i
                LEFT JOIN users u ON lower(u.email) = lower(i.email)
                ORDER BY i.invited_at DESC
            """)
            return {"invites": [dict(r) for r in cur.fetchall()]}

class InviteCreate(BaseModel):
    email: str
    role: str = "read"

@app.post("/api/invites", status_code=201)
def create_invite(body: InviteCreate, user: dict = Depends(require_owner)):
    email = body.email.strip().lower()
    if not email or "@" not in email:
        raise HTTPException(400, "Invalid email address")
    if body.role not in ("read", "edit"):
        raise HTTPException(400, "Role must be 'read' or 'edit'")
    if OWNER_EMAIL and email == OWNER_EMAIL.lower():
        raise HTTPException(400, "Cannot invite the owner")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO invited_users (email, role) VALUES (%s, %s) ON CONFLICT (email) DO NOTHING RETURNING id",
                (email, body.role)
            )
            if cur.rowcount == 0:
                raise HTTPException(409, "Email already invited")
    return {"ok": True, "email": email, "role": body.role}

class InviteRoleUpdate(BaseModel):
    role: str

@app.patch("/api/invites/{email}")
def update_invite_role(email: str, body: InviteRoleUpdate, user: dict = Depends(require_owner)):
    if body.role not in ("read", "edit"):
        raise HTTPException(400, "Role must be 'read' or 'edit'")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE invited_users SET role=%s WHERE lower(email)=lower(%s) RETURNING email",
                (body.role, email)
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Invite not found")
    return {"ok": True, "email": email, "role": body.role}

@app.delete("/api/invites/{email}")
def revoke_invite(email: str, user: dict = Depends(require_owner)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM invited_users WHERE lower(email)=lower(%s) RETURNING email",
                (email,)
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Invite not found")
    return {"ok": True, "email": email}

# ── Run ───────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=True)

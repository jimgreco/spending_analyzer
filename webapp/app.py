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
import os, re, io, json, hashlib, secrets, uuid, threading, subprocess
from collections import Counter
from datetime import datetime, timedelta
from difflib import SequenceMatcher
from contextlib import contextmanager
from typing import List, Optional
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
from pydantic import BaseModel
from dotenv import load_dotenv

# ── Load .env (one level up from this file) ───────────────────────────────────────
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))

# ── Configuration ────────────────────────────────────────────────────────────────
DATABASE_URL         = os.getenv("DATABASE_URL", "postgresql://spending:spending@localhost/spending")
PORT                 = int(os.getenv("PORT", "8000"))
OPENAI_API_KEY       = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL         = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
GOOGLE_CLIENT_ID     = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
SECRET_KEY           = os.getenv("SECRET_KEY", secrets.token_hex(32))
APP_URL              = os.getenv("APP_URL", "http://localhost:8000").rstrip("/")
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
def _gpt_tag_chunk(client, model, tag_list, chunk):
    """Assign one primary tag to each transaction description in the chunk."""
    items = "\n".join(f"{i}: {d}" for i, d in enumerate(chunk))
    prompt = (
        f"For each credit card transaction, pick the single best matching tag from this list: "
        f"{', '.join(tag_list)}.\n"
        f"If none fit, use null.\n\n"
        f"Transactions:\n{items}\n\n"
        f'Respond with JSON only: {{"results": [{{"index": 0, "primary_tag": "tag1"}}]}}'
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"},
    )
    data = json.loads(resp.choices[0].message.content)
    tag_set = set(tag_list)
    result = {}
    for item in data.get("results", []):
        idx = item.get("index", -1)
        if 0 <= idx < len(chunk):
            primary = item.get("primary_tag")
            if primary and primary in tag_set:
                result[chunk[idx]] = primary
    return result


def assign_tags_with_gpt(descriptions: list, tag_list: list) -> dict:
    """Returns {description: 'primary_tag'} — assigns one primary tag per description."""
    if not OPENAI_API_KEY or not descriptions or not tag_list:
        return {}
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        client = OpenAI(api_key=OPENAI_API_KEY)
        CHUNK  = 80
        chunks = [descriptions[i:i+CHUNK] for i in range(0, len(descriptions), CHUNK)]
        result = {}
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_gpt_tag_chunk, client, OPENAI_MODEL, tag_list, ch): ch
                       for ch in chunks}
            for fut in as_completed(futures):
                try:
                    result.update(fut.result())
                except Exception as e:
                    print(f"[GPT tag chunk] {type(e).__name__}: {e}")
        return result
    except Exception as e:
        print(f"[GPT assign tags] {type(e).__name__}: {e}")
        return {}

# ── Description cleaning ──────────────────────────────────────────────────────────
def clean_description(desc: str) -> str:
    """Remove payment-method prefixes that add no useful information."""
    desc = re.sub(r'(?i)\bAplPay\s*', '', desc)
    desc = re.sub(r'(?i)^SP\s+', '', desc)
    desc = re.sub(r'(?i)^\*?TST\*?\s*', '', desc)
    return desc.strip()

# ── Dedup key ─────────────────────────────────────────────────────────────────────
def make_dedup_key(date: str, source: str, amount: float, description: str, seq: int = 1) -> str:
    norm = re.sub(r'[^A-Z0-9]', '', description.upper())[:12]
    raw = f"{date}|{source}|{amount:.2f}|{norm}|{seq}"
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
GPT_PARSE_PROMPT = """You are a financial statement parser. Extract ALL rows that have a date, description, and non-zero dollar amount.

Return JSON in this exact format:
{"transactions": [{"date": "YYYY-MM-DD", "description": "merchant name or description", "amount": 0.00}, ...]}

Rules:
- Include EVERY row that has a date and a non-zero amount — purchases, fees, interest, payments, refunds, credits, transfers, everything
- amount: positive for charges/purchases/fees/outflows, negative for refunds/credits/payments received
- date: YYYY-MM-DD format
- SKIP only: rows with no date, rows with $0.00 amount, pure header/summary/subtotal rows with no transaction meaning
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
        if choice.finish_reason == "length":
            print(f"[GPT parse] WARNING: '{filename}' hit max_tokens — response truncated")
        data = json.loads(choice.message.content)
        raw_rows = data.get("transactions", [])
        rows = []
        for r in raw_rows:
            try:
                rows.append({
                    "date":        str(r["date"]).strip(),
                    "description": str(r["description"]).strip(),
                    "amount":      float(r["amount"]),
                })
            except (KeyError, ValueError, TypeError):
                continue
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
            rows.extend(cr)
            if ce and not rows:
                gpt_error = ce
        print(f"[parse] '{filename}': {len(chunks)} chunks → {len(rows)} rows")
    else:
        rows, _, gpt_error = parse_with_gpt(text, filename)

    if not rows:
        return [], None, gpt_error or f"No transactions found in '{filename}'"

    source = detect_source(text) or "Unknown"

    seq_counts: Counter = Counter()
    for r in rows:
        r["date"] = parse_date(r["date"])
        r.setdefault("source", source)
        base = (r["date"], r["source"], r["amount"], r["description"])
        seq_counts[base] += 1
        r["dedup_key"] = make_dedup_key(
            r["date"], r["source"], r["amount"], r["description"], seq_counts[base])

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
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(500, "GOOGLE_CLIENT_ID not configured")
    params = {
        "client_id":     GOOGLE_CLIENT_ID,
        "redirect_uri":  f"{APP_URL}/auth/google/callback",
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
async def auth_callback(code: str, response: Response):
    if not GOOGLE_CLIENT_ID:
        raise HTTPException(500, "Google OAuth not configured")
    async with httpx.AsyncClient() as client:
        token_resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code":          code,
                "client_id":     GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "redirect_uri":  f"{APP_URL}/auth/google/callback",
                "grant_type":    "authorization_code",
            }
        )
    tokens = token_resp.json()
    if "error" in tokens:
        raise HTTPException(400, f"OAuth error: {tokens.get('error_description', tokens['error'])}")

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
        secure=APP_URL.startswith("https://"),
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

# ── Transactions ──────────────────────────────────────────────────────────────────
@app.get("/api/transactions")
def get_transactions(
    page: int = 1, per_page: int = 100,
    source: str = "", tag: List[str] = Query([]), tag_match: str = "any",
    search: str = "", date_from: str = "", date_to: str = "",
    import_file: str = "", card_last4: str = "",
    sort_by: str = "date", sort_dir: str = "desc",
    status: str = "active",
    user: dict = Depends(get_current_user)
):
    uid = user["id"]
    where, params = ["t.user_id = %s"], [uid]
    if status in ("active", "deleted", "deduped"):
        where.append("t.status = %s"); params.append(status)
    if source:      where.append("t.source = %s");          params.append(source)
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
                       t.amount::float, t.source, t.import_file,
                       t.status, t.dedup_of,
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
                WHERE {wc}
                ORDER BY {sc} {sd}, t.id {sd}
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            rows = [dict(r) for r in cur.fetchall()]

    return {"transactions": rows, "total": total, "page": page,
            "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page)}

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
                "UPDATE transactions SET status='deleted' WHERE id=%s AND user_id=%s AND status='active' RETURNING id",
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
                "UPDATE transactions SET status='deleted' WHERE id=ANY(%s) AND user_id=%s AND status='active'",
                (body.ids, user["id"]))
            deleted = cur.rowcount
    return {"ok": True, "deleted": deleted}

# ── Restore ───────────────────────────────────────────────────────────────────────
@app.post("/api/transactions/{tx_id}/restore")
def restore_transaction(tx_id: int, user: dict = Depends(require_edit)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET status='active', dedup_of=NULL WHERE id=%s AND user_id=%s RETURNING id",
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
                "UPDATE transactions SET status='active', dedup_of=NULL WHERE id=ANY(%s) AND user_id=%s",
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
def _process_upload_job(job_id: str, user_id: int, filename: str, content: bytes, force: bool):
    """Runs in a background thread. Processes one file and updates upload_jobs on completion."""
    def set_status(status, result=None):
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE upload_jobs SET status=%s, result_json=%s WHERE id=%s",
                    (status, json.dumps(result) if result is not None else None, job_id))

    try:
        set_status("processing")
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

        rows, source, error = parse_file_bytes(content, filename)
        if error:
            set_status("done", {"filename": filename, "status": "error",
                                "message": error, "new": 0, "dupes": 0})
            return

        for r in rows:
            r["description"] = clean_description(r["description"])

        # 2. Load user's tags for GPT assignment
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM tags WHERE user_id=%s ORDER BY name", (user_id,))
                tag_list = [r[0] for r in cur.fetchall()]

        # 3. GPT tag assignment — no DB connection held
        unique_descs = list({r["description"] for r in rows})
        gpt_tag_map = assign_tags_with_gpt(unique_descs, tag_list) if tag_list else {}

        # 4. Insert transactions, then tags
        dedup_keys = [r["dedup_key"] for r in rows]
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT filename FROM uploaded_files WHERE user_id=%s AND file_hash=%s",
                            (user_id, file_hash))
                row = cur.fetchone()
                import_name = row[0] if row else filename

                cur.execute("SELECT dedup_key FROM transactions WHERE user_id=%s AND dedup_key=ANY(%s) AND status='active'",
                            (user_id, dedup_keys))
                existing_keys = {r[0] for r in cur.fetchall()}

                new_count = dupe_count = 0
                insert_rows = []
                dedup_key_to_desc = {}
                for r in rows:
                    is_dupe   = r["dedup_key"] in existing_keys
                    tx_status = "deduped" if is_dupe else "active"
                    insert_rows.append((user_id, r["date"], r["description"],
                                        r["amount"], r["source"], r["dedup_key"],
                                        tx_status, r["dedup_key"] if is_dupe else None, import_name))
                    dedup_key_to_desc[r["dedup_key"]] = r["description"]
                    if tx_status == "active": new_count  += 1
                    else:                     dupe_count += 1

                returned = psycopg2.extras.execute_values(cur, """
                    INSERT INTO transactions
                        (user_id, date, description, amount, source,
                         dedup_key, status, dedup_of, import_file)
                    VALUES %s
                    RETURNING id, dedup_key
                """, insert_rows, fetch=True)

                # Upsert primary tags needed and build name→id map
                all_needed_tags = set()
                for desc in dedup_key_to_desc.values():
                    primary = gpt_tag_map.get(desc)
                    if primary:
                        all_needed_tags.add(primary)
                tag_name_to_id = {}
                for name in all_needed_tags:
                    cur.execute(
                        "INSERT INTO tags (user_id, name) VALUES (%s,%s) "
                        "ON CONFLICT (user_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                        (user_id, name)
                    )
                    tag_name_to_id[name] = cur.fetchone()[0]

                # Assign primary tag to newly inserted transactions
                for (tx_id, dk) in returned:
                    desc = dedup_key_to_desc.get(dk, "")
                    primary_name = gpt_tag_map.get(desc)
                    if primary_name:
                        tag_id = tag_name_to_id.get(primary_name)
                        if tag_id:
                            cur.execute(
                                "UPDATE transactions SET primary_tag_id=%s, primary_migration_status='auto' WHERE id=%s",
                                (tag_id, tx_id)
                            )

                cur.execute("""
                    INSERT INTO uploaded_files (user_id, filename, file_hash, source, tx_new, tx_dupes)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (user_id, file_hash) DO NOTHING
                """, (user_id, filename, file_hash, source, new_count, dupe_count))

        set_status("done", {"filename": filename, "status": "ok", "source": source,
                            "new": new_count, "dupes": dupe_count})
    except Exception as e:
        print(f"[upload_job:{job_id}] {type(e).__name__}: {e}")
        set_status("error", {"filename": filename, "status": "error",
                             "message": str(e), "new": 0, "dupes": 0})


@app.post("/api/upload")
async def upload_files(files: List[UploadFile] = File(...),
                       force: bool = False,
                       user: dict = Depends(require_edit)):
    user_id = user["id"]
    jobs = []
    for f in files:
        content = await f.read()
        job_id  = str(uuid.uuid4())
        with db() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO upload_jobs (id, user_id, filename, status) VALUES (%s,%s,%s,'pending')",
                    (job_id, user_id, f.filename))
        threading.Thread(target=_process_upload_job,
                         args=(job_id, user_id, f.filename, content, force),
                         daemon=True).start()
        jobs.append({"job_id": job_id, "filename": f.filename})
    return {"jobs": jobs}


@app.get("/api/upload/status/{job_id}")
def get_upload_job_status(job_id: str, user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor() as cur:
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

class PrimaryTagUpdate(BaseModel):
    primary_tag: Optional[str] = None  # null to clear

@app.put("/api/transactions/{tx_id}/primary-tag")
def set_primary_tag(tx_id: int, body: PrimaryTagUpdate, user: dict = Depends(require_edit)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            # Verify ownership
            cur.execute("SELECT id, primary_tag_id FROM transactions WHERE id=%s AND user_id=%s", (tx_id, uid))
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Transaction not found")
            old_primary_id = row[1]

            if body.primary_tag is None:
                # Clear primary tag — demote old primary to secondary
                if old_primary_id:
                    cur.execute(
                        "INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (tx_id, old_primary_id))
                cur.execute("UPDATE transactions SET primary_tag_id=NULL WHERE id=%s", (tx_id,))
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
            cur.execute("UPDATE transactions SET primary_tag_id=%s WHERE id=%s", (new_tag_id, tx_id))
    return {"ok": True, "id": tx_id, "primary_tag": tag_name}

@app.post("/api/transactions/bulk-tag")
def bulk_tag_transactions(body: dict, user: dict = Depends(require_edit)):
    ids = body.get("ids", [])
    tag_name = (body.get("tag") or "").strip()
    action = body.get("action", "add")  # "add", "remove", or "set-primary"
    if not ids: raise HTTPException(400, "No IDs provided")
    if not tag_name: raise HTTPException(400, "Tag name required")
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            if action == "remove":
                cur.execute("""
                    DELETE FROM transaction_tags tt
                    USING tags tg
                    WHERE tg.id = tt.tag_id
                      AND tg.user_id = %s
                      AND tg.name = %s
                      AND tt.transaction_id = ANY(%s)
                """, (uid, tag_name, ids))
            elif action == "set-primary":
                # Upsert tag
                cur.execute(
                    "INSERT INTO tags (user_id, name) VALUES (%s,%s) ON CONFLICT (user_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                    (uid, tag_name))
                tag_id = cur.fetchone()[0]
                for tx_id in ids:
                    # Remove new primary from secondary if present
                    cur.execute("DELETE FROM transaction_tags WHERE transaction_id=%s AND tag_id=%s", (tx_id, tag_id))
                    # Demote old primary to secondary if different
                    cur.execute("SELECT primary_tag_id FROM transactions WHERE id=%s AND user_id=%s", (tx_id, uid))
                    row = cur.fetchone()
                    if row and row[0] and row[0] != tag_id:
                        cur.execute(
                            "INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                            (tx_id, row[0]))
                    cur.execute("UPDATE transactions SET primary_tag_id=%s WHERE id=%s AND user_id=%s", (tag_id, tx_id, uid))
            else:
                # Upsert tag
                cur.execute(
                    "INSERT INTO tags (user_id, name) VALUES (%s,%s) ON CONFLICT (user_id,name) DO UPDATE SET name=EXCLUDED.name RETURNING id",
                    (uid, tag_name)
                )
                tag_id = cur.fetchone()[0]
                for tx_id in ids:
                    cur.execute(
                        "INSERT INTO transaction_tags (transaction_id, tag_id) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                        (tx_id, tag_id)
                    )
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
                "UPDATE transactions SET primary_tag_id=%s, primary_migration_status='reviewed' WHERE id=%s",
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

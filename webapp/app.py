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
from fastapi import FastAPI, UploadFile, File, HTTPException, Request, Response, Depends
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

def init_db():
    # Base schema (safe for both fresh and existing DBs)
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)

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
    ]

    for label, sql in migrations:
        try:
            with db() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
        except Exception as e:
            print(f"[migrate:{label}] {e}")

    # In LOCAL_DEV mode, ensure the test user exists and owns any orphaned records.
    if LOCAL_DEV:
        user = _ensure_local_user()
        uid  = user["id"]
        _seed_user_categories(uid)
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

def _seed_user_categories(user_id: int):
    """Seed the default category list — only if the user has no categories yet."""
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM categories WHERE user_id = %s LIMIT 1", (user_id,))
            if cur.fetchone():
                return  # already seeded; don't overwrite user's current list
            for cat in ALL_CATEGORIES:
                excluded = cat in EXCLUDED_FROM_SPENDING_DEFAULT
                cur.execute(
                    "INSERT INTO categories (user_id, name, excluded_from_spending) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                    (user_id, cat, excluded)
                )

ALL_CATEGORIES = [
    "Alcohol", "Childcare", "Clothing", "Dining", "Education", "Entertainment",
    "Fees", "Groceries", "Health & Fitness", "Other", "Services", "Shopping",
    "Subscriptions", "Taxes", "Transportation", "Transfers", "Travel",
]

# Categories that are excluded from spending totals by default
EXCLUDED_FROM_SPENDING_DEFAULT = {"Taxes", "Transfers"}

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

# ── DB-based category matching ────────────────────────────────────────────────────
def load_all_tx(conn, user_id: int) -> tuple:
    """Returns (manually_corrected_rows, all_rows) as lists of (description, category)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT description, category FROM transactions "
            "WHERE manually_corrected = TRUE AND status = 'active' AND user_id = %s",
            (user_id,)
        )
        manual = cur.fetchall()
        cur.execute(
            "SELECT description, category FROM transactions "
            "WHERE status = 'active' AND user_id = %s ORDER BY created_at DESC LIMIT 2000",
            (user_id,)
        )
        all_tx = cur.fetchall()
    return manual, all_tx

def find_db_match(description: str, manual: list, all_tx: list,
                  valid_cats: Optional[set] = None) -> Optional[str]:
    if not all_tx and not manual:
        return None
    d = description.upper()
    for h_desc, h_cat in all_tx:
        if h_desc.upper() == d:
            return h_cat if (valid_cats is None or h_cat in valid_cats) else None
    if manual:
        best_r, best_c = 0.0, None
        for h_desc, h_cat in manual:
            r = SequenceMatcher(None, d, h_desc.upper()).ratio()
            if r > best_r:
                best_r, best_c = r, h_cat
        if best_r >= 0.75 and (valid_cats is None or best_c in valid_cats):
            return best_c
    best_r, best_c = 0.0, None
    for h_desc, h_cat in all_tx:
        r = SequenceMatcher(None, d, h_desc.upper()).ratio()
        if r > best_r:
            best_r, best_c = r, h_cat
    if best_r >= 0.85 and (valid_cats is None or best_c in valid_cats):
        return best_c
    return None

# ── GPT batch categorization ──────────────────────────────────────────────────────
def _gpt_chunk(client, model, cat_list, cat_set, chunk):
    items = "\n".join(f"{i}: {d}" for i, d in enumerate(chunk))
    prompt = (
        f"Categorize these credit card transactions. "
        f"Pick ONLY from: {cat_list}. Use 'Other' if nothing fits.\n\n"
        f"Transactions:\n{items}\n\n"
        f'Respond with JSON only: {{"results": [{{"index": 0, "category": "..."}}]}}'
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[{"role": "user", "content": prompt}],
        temperature=0,
        response_format={"type": "json_object"},
    )
    data = json.loads(resp.choices[0].message.content)
    return {chunk[item["index"]]: item["category"] if item["category"] in cat_set else "Other"
            for item in data.get("results", [])
            if 0 <= item.get("index", -1) < len(chunk)}


def categorize_with_gpt(descriptions: list, categories: list) -> dict:
    if not OPENAI_API_KEY or not descriptions:
        return {}
    try:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        client   = OpenAI(api_key=OPENAI_API_KEY)
        cat_set  = set(categories)
        cat_list = ", ".join(sorted(categories))
        CHUNK    = 80
        chunks   = [descriptions[i:i+CHUNK] for i in range(0, len(descriptions), CHUNK)]
        result   = {}
        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {pool.submit(_gpt_chunk, client, OPENAI_MODEL, cat_list, cat_set, ch): ch
                       for ch in chunks}
            for fut in as_completed(futures):
                try:
                    result.update(fut.result())
                except Exception as e:
                    print(f"[GPT chunk] {type(e).__name__}: {e}")
        return result
    except Exception as e:
        print(f"[GPT categorize] {type(e).__name__}: {e}")
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
        r.setdefault("category", "Other")
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

    _seed_user_categories(user["id"])

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

# ── Transactions ──────────────────────────────────────────────────────────────────
@app.get("/api/transactions")
def get_transactions(
    page: int = 1, per_page: int = 100,
    source: str = "", category: str = "", tag: str = "", search: str = "",
    date_from: str = "", date_to: str = "",
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
    if category:    where.append("t.category = %s");        params.append(category)
    if tag:
        where.append("t.id IN (SELECT tt.transaction_id FROM transaction_tags tt JOIN tags tg ON tg.id = tt.tag_id WHERE tg.user_id = %s AND tg.name = %s)")
        params.extend([uid, tag])
    if date_from:   where.append("t.date >= %s");           params.append(date_from)
    if date_to:     where.append("t.date <= %s");           params.append(date_to)
    if search:      where.append("t.description ILIKE %s"); params.append(f"%{search}%")
    if import_file: where.append("t.import_file = %s");     params.append(import_file)
    if card_last4:
        where.append("t.import_file IN (SELECT filename FROM uploaded_files WHERE user_id=%s AND card_last4=%s)")
        params.extend([uid, card_last4])
    wc = " AND ".join(where)

    valid_cols = {"date", "amount", "description", "category", "source"}
    sc = "t." + (sort_by if sort_by in valid_cols else "date")
    sd = "DESC" if sort_dir == "desc" else "ASC"
    offset = (page - 1) * per_page

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) as n FROM transactions t WHERE {wc}", params)
            total = cur.fetchone()["n"]
            cur.execute(f"""
                SELECT t.id, t.date::text, t.description, t.category,
                       t.amount::float, t.source, t.manually_corrected, t.import_file,
                       t.status, t.dedup_of,
                       COALESCE(ARRAY(
                           SELECT tg.name FROM transaction_tags tt
                           JOIN tags tg ON tg.id = tt.tag_id
                           WHERE tt.transaction_id = t.id
                           ORDER BY tg.name
                       ), '{{}}') AS tags
                FROM transactions t WHERE {wc}
                ORDER BY {sc} {sd}, t.id {sd}
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            rows = [dict(r) for r in cur.fetchall()]

    return {"transactions": rows, "total": total, "page": page,
            "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page)}

@app.get("/api/stats")
def get_stats(
    source: str = "", category: str = "", tag: str = "", search: str = "",
    date_from: str = "", date_to: str = "", import_file: str = "",
    card_last4: str = "", user: dict = Depends(get_current_user)
):
    uid = user["id"]
    where, params = ["t.status = 'active'", "t.user_id = %s"], [uid]
    if source:      where.append("t.source = %s");      params.append(source)
    if category:    where.append("t.category = %s");    params.append(category)
    if tag:
        where.append("t.id IN (SELECT tt.transaction_id FROM transaction_tags tt JOIN tags tg ON tg.id = tt.tag_id WHERE tg.user_id = %s AND tg.name = %s)")
        params.extend([uid, tag])
    if date_from:   where.append("t.date >= %s");       params.append(date_from)
    if date_to:     where.append("t.date <= %s");       params.append(date_to)
    if search:      where.append("t.description ILIKE %s"); params.append(f"%{search}%")
    if import_file: where.append("t.import_file = %s"); params.append(import_file)
    if card_last4:
        where.append("t.import_file IN (SELECT filename FROM uploaded_files WHERE user_id=%s AND card_last4=%s)")
        params.extend([uid, card_last4])

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT name FROM categories WHERE user_id=%s AND excluded_from_spending=TRUE",
                (uid,)
            )
            excluded = [r["name"] for r in cur.fetchall()]
            if excluded:
                where.append("NOT (t.category = ANY(%s))")
                params.append(excluded)

            wc = " AND ".join(where)
            cur.execute(f"SELECT t.category, SUM(t.amount)::float AS total, COUNT(*)::int AS count FROM transactions t WHERE {wc} GROUP BY t.category ORDER BY total DESC", params)
            by_category = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT TO_CHAR(t.date,'YYYY-MM') AS month, SUM(t.amount)::float AS total FROM transactions t WHERE {wc} GROUP BY month ORDER BY month", params)
            by_month = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT t.source, SUM(t.amount)::float AS total, COUNT(*)::int AS count FROM transactions t WHERE {wc} GROUP BY t.source ORDER BY total DESC", params)
            by_source = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT COALESCE(SUM(t.amount),0)::float AS total, COUNT(*)::int AS count FROM transactions t WHERE {wc}", params)
            summary = dict(cur.fetchone())
            # Tag breakdown — always compute
            cur.execute(f"""
                SELECT tg.name AS tag, SUM(t.amount)::float AS total, COUNT(DISTINCT t.id)::int AS count
                FROM transactions t
                JOIN transaction_tags tt ON tt.transaction_id = t.id
                JOIN tags tg ON tg.id = tt.tag_id
                WHERE {wc}
                GROUP BY tg.name
                ORDER BY total DESC
            """, params)
            by_tag = [dict(r) for r in cur.fetchall()]

    return {**summary, "by_category": by_category, "by_month": by_month,
            "by_source": by_source, "by_tag": by_tag}

# ── Category / source update ──────────────────────────────────────────────────────
class CategoryUpdate(BaseModel):
    category: Optional[str] = None
    source: Optional[str] = None

@app.patch("/api/transactions/{tx_id}")
def update_transaction(tx_id: int, body: CategoryUpdate, user: dict = Depends(require_edit)):
    with db() as conn:
        with conn.cursor() as cur:
            if body.source is not None:
                cur.execute("""
                    UPDATE transactions SET source = %s
                    WHERE id = %s AND user_id = %s RETURNING id
                """, (body.source, tx_id, user["id"]))
            elif body.category is not None:
                cur.execute("""
                    UPDATE transactions SET category = %s, manually_corrected = TRUE
                    WHERE id = %s AND user_id = %s RETURNING id
                """, (body.category, tx_id, user["id"]))
            else:
                raise HTTPException(400, "Nothing to update")
            if cur.rowcount == 0:
                raise HTTPException(404, "Transaction not found")
    return {"ok": True, "id": tx_id}

class BulkCategoryUpdate(BaseModel):
    ids: List[int]
    category: Optional[str] = None

@app.patch("/api/transactions")
def bulk_update_category(body: BulkCategoryUpdate, user: dict = Depends(require_edit)):
    if not body.ids:
        raise HTTPException(400, "No IDs provided")
    if body.category is None:
        raise HTTPException(400, "Nothing to update")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET category = %s, manually_corrected = TRUE WHERE id = ANY(%s) AND user_id = %s",
                [body.category, body.ids, user["id"]]
            )
            updated = cur.rowcount
    return {"ok": True, "updated": updated}

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

        # 2. Load reference data — release connection before fuzzy/GPT
        with db() as conn:
            manual_tx, all_tx = load_all_tx(conn, user_id)
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM categories WHERE user_id=%s ORDER BY name", (user_id,))
                cat_list = [r[0] for r in cur.fetchall()]
        valid_cats = set(cat_list)

        # 3. Fuzzy match + GPT — no DB connection held
        fuzzy_cache = {}
        needs_gpt = []
        for r in rows:
            existing = r.get("category", "Other")
            if existing and existing not in ("Other", "") and existing in valid_cats:
                continue
            desc = r["description"]
            if desc not in fuzzy_cache:
                fuzzy_cache[desc] = find_db_match(desc, manual_tx, all_tx, valid_cats)
            match = fuzzy_cache[desc]
            if match:
                r["category"] = match
            else:
                r["category"] = "Other"
                needs_gpt.append(r)
        unique_for_gpt = list({r["description"] for r in needs_gpt})
        if unique_for_gpt:
            gpt_map = categorize_with_gpt(unique_for_gpt, cat_list)
            for r in needs_gpt:
                r["category"] = gpt_map.get(r["description"], "Other")

        # 4. Insert — use executemany for bulk dedup check + insert
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
                for r in rows:
                    is_dupe   = r["dedup_key"] in existing_keys
                    tx_status = "deduped" if is_dupe else "active"
                    insert_rows.append((user_id, r["date"], r["description"], r["category"],
                                        r["amount"], r["source"], r["dedup_key"],
                                        tx_status, r["dedup_key"] if is_dupe else None, import_name))
                    if tx_status == "active": new_count  += 1
                    else:                     dupe_count += 1

                psycopg2.extras.execute_values(cur, """
                    INSERT INTO transactions
                        (user_id, date, description, category, amount, source,
                         dedup_key, status, dedup_of, import_file)
                    VALUES %s
                """, insert_rows)

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

# ── Categories ────────────────────────────────────────────────────────────────────
@app.get("/api/categories")
def get_categories(user: dict = Depends(get_current_user)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT name, excluded_from_spending FROM categories WHERE user_id=%s ORDER BY name",
                (uid,)
            )
            cats = [dict(r) for r in cur.fetchall()]
    return {"categories": cats}

class CategoryCreate(BaseModel):
    name: str

@app.post("/api/categories", status_code=201)
def create_category(body: CategoryCreate, user: dict = Depends(require_edit)):
    name = body.name.strip()
    if not name: raise HTTPException(400, "Category name cannot be empty")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO categories (user_id, name) VALUES (%s,%s) ON CONFLICT DO NOTHING RETURNING name",
                (user["id"], name))
            if cur.rowcount == 0: raise HTTPException(409, "Category already exists")
    return {"ok": True, "name": name}

class CategoryExclusionToggle(BaseModel):
    name: str
    excluded: bool

@app.patch("/api/categories/exclusion")
def toggle_category_exclusion(body: CategoryExclusionToggle, user: dict = Depends(require_edit)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE categories SET excluded_from_spending = %s WHERE user_id=%s AND name=%s RETURNING name",
                (body.excluded, uid, body.name)
            )
            if cur.rowcount == 0:
                raise HTTPException(404, "Category not found")
    return {"ok": True, "name": body.name, "excluded": body.excluded}

class CategoryRename(BaseModel):
    old_name: str; new_name: str

@app.patch("/api/categories")
def rename_category(body: CategoryRename, user: dict = Depends(require_edit)):
    old, new, uid = body.old_name.strip(), body.new_name.strip(), user["id"]
    if not new:    raise HTTPException(400, "New name cannot be empty")
    if old == new: raise HTTPException(400, "New name is the same as old name")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM categories WHERE user_id=%s AND name=%s", (uid, old))
            if not cur.fetchone(): raise HTTPException(404, "Category not found")
            cur.execute("SELECT 1 FROM categories WHERE user_id=%s AND name=%s", (uid, new))
            if cur.fetchone():     raise HTTPException(409, "A category with that name already exists")
            cur.execute("UPDATE categories SET name=%s WHERE user_id=%s AND name=%s", (new, uid, old))
            cur.execute("UPDATE transactions SET category=%s WHERE user_id=%s AND category=%s",
                        (new, uid, old))
            updated = cur.rowcount
    return {"ok": True, "old_name": old, "new_name": new, "updated": updated}

@app.delete("/api/categories")
def delete_category(name: str, user: dict = Depends(require_edit)):
    if name == "Other": raise HTTPException(400, "Cannot delete the 'Other' category")
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("UPDATE transactions SET category='Other' WHERE user_id=%s AND category=%s",
                        (uid, name))
            reassigned = cur.rowcount
            cur.execute("DELETE FROM categories WHERE user_id=%s AND name=%s RETURNING name",
                        (uid, name))
            if cur.rowcount == 0: raise HTTPException(404, "Category not found")
    return {"ok": True, "name": name, "reassigned": reassigned}

# ── Tags ──────────────────────────────────────────────────────────────────────────
@app.get("/api/tags")
def get_tags(user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT name FROM tags WHERE user_id=%s ORDER BY name",
                (user["id"],)
            )
            return {"tags": [r["name"] for r in cur.fetchall()]}

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

@app.post("/api/transactions/bulk-tag")
def bulk_tag_transactions(body: dict, user: dict = Depends(require_edit)):
    ids = body.get("ids", [])
    tag_name = (body.get("tag") or "").strip()
    action = body.get("action", "add")  # "add" or "remove"
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

# ── Upload history ────────────────────────────────────────────────────────────────
@app.get("/api/uploads")
def get_uploads(user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT filename, file_hash, source, card_last4, tx_new, tx_dupes,
                       to_char(uploaded_at,'YYYY-MM-DD HH24:MI') as uploaded_at
                FROM uploaded_files WHERE user_id=%s
                ORDER BY source, filename LIMIT 50
            """, (user["id"],))
            return {"uploads": [dict(r) for r in cur.fetchall()]}

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

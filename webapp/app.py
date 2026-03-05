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
import os, re, io, json, hashlib, secrets
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
        _pool = psycopg2.pool.SimpleConnectionPool(1, 10, DATABASE_URL)
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
                cur.execute(
                    "INSERT INTO categories (user_id, name) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                    (user_id, cat)
                )

ALL_CATEGORIES = [
    "Alcohol", "Childcare", "Clothing", "Dining", "Education", "Entertainment",
    "Fees", "Groceries", "Health & Fitness", "Other", "Services", "Shopping",
    "Subscriptions", "Transportation", "Travel",
]

# ── Auth dependency ───────────────────────────────────────────────────────────────
def get_current_user(request: Request) -> dict:
    """
    FastAPI dependency — resolves the authenticated user.
    In LOCAL_DEV mode returns the local test user without checking a cookie.
    """
    if LOCAL_DEV:
        return _ensure_local_user()

    token = request.cookies.get("session")
    if not token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    user_id = _unsign_session(token)
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid or expired session")

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, email, name, picture FROM users WHERE id = %s", (user_id,))
            user = cur.fetchone()
    if not user:
        raise HTTPException(status_code=401, detail="User not found")
    return dict(user)

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
def categorize_with_gpt(descriptions: list, categories: list) -> dict:
    if not OPENAI_API_KEY or not descriptions:
        return {}
    try:
        client   = OpenAI(api_key=OPENAI_API_KEY)
        cat_set  = set(categories)
        cat_list = ", ".join(sorted(categories))
        result   = {}
        CHUNK    = 80
        for offset in range(0, len(descriptions), CHUNK):
            chunk = descriptions[offset:offset + CHUNK]
            items = "\n".join(f"{i}: {d}" for i, d in enumerate(chunk))
            prompt = (
                f"Categorize these credit card transactions. "
                f"Pick ONLY from: {cat_list}. Use 'Other' if nothing fits.\n\n"
                f"Transactions:\n{items}\n\n"
                f'Respond with JSON only: {{"results": [{{"index": 0, "category": "..."}}]}}'
            )
            resp = client.chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"},
            )
            data = json.loads(resp.choices[0].message.content)
            for item in data.get("results", []):
                idx = item.get("index", -1)
                cat = item.get("category", "Other")
                if 0 <= idx < len(chunk):
                    result[chunk[idx]] = cat if cat in cat_set else "Other"
        return result
    except Exception as e:
        print(f"[GPT categorize] {type(e).__name__}: {e}")
        return {}

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

def infer_year(mo: int, day: int, open_dt: datetime, close_dt: datetime) -> Optional[datetime]:
    for yr in [open_dt.year, close_dt.year]:
        try:
            d = datetime(yr, mo, day)
            if open_dt - timedelta(days=5) <= d <= close_dt + timedelta(days=5):
                return d
        except ValueError:
            pass
    try:
        return datetime(close_dt.year, mo, day)
    except ValueError:
        return None

# ── PDF parsers ───────────────────────────────────────────────────────────────────
APPLE_TX = re.compile(
    r'^(\d{2}/\d{2}/\d{4})\s+(.+?)\s+\d+%\s+\$[\d,.]+\s+(\$[\d,]+\.\d{2})\s*$')

def parse_apple_pdf(text: str) -> list:
    rows, in_tx = [], False
    for line in text.split("\n"):
        s = line.strip()
        if s == "Transactions":        in_tx = True; continue
        if s.startswith(("Total Daily Cash", "Total charges")): in_tx = False; continue
        if not in_tx or (s.startswith("Date") and "Description" in s): continue
        m = APPLE_TX.match(s)
        if m:
            amt = float(m.group(3).replace("$", "").replace(",", ""))
            if amt != 0:
                rows.append({"date": parse_date(m.group(1)),
                             "description": m.group(2).strip(),
                             "amount": amt, "source": "Apple Card"})
    return rows

COINBASE_TX = re.compile(
    r'^((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4})\s+'
    r'(.+?)\s+(-?\$[\d,]+\.\d{2})\s*$')
COINBASE_SKIP = re.compile(
    r'Coinbase One Card is offered|@gmail\.com|^Jim Greco|^Page \d+ of \d+|^Date\s+Description|Credit Limit')

COINBASE_PERIOD = re.compile(
    r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2}[, ]+(\d{4})\s*[-–]\s*'
    r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{1,2}[, ]+(\d{4})', re.I)

def parse_coinbase_pdf(text: str) -> list:
    # Determine statement closing month to use as date for credits/returns
    stmt_month_start = None
    mp = COINBASE_PERIOD.search(text)
    if mp:
        try:
            stmt_month_start = datetime.strptime(f"{mp.group(3)} 1 {mp.group(4)}", "%b 1 %Y")
        except ValueError:
            pass

    rows, in_tx, in_credits = [], False, False
    latest_tx_date = None
    for line in text.split("\n"):
        s = line.strip()
        if s == "Transactions":                          in_tx = True;  in_credits = False; continue
        if s.startswith("Total new charges"):            in_tx = False;                     continue
        if re.search(r'Payments and Credits', s, re.I): in_credits = True; in_tx = False;  continue
        if re.search(r'Total payments', s, re.I):        in_credits = False;                continue
        if not (in_tx or in_credits) or COINBASE_SKIP.search(s): continue
        if re.search(r'Credit Limit', s, re.I): continue
        if in_credits and re.search(r'PAYMENT', s, re.I): continue  # skip payments, keep returns
        m = COINBASE_TX.match(s)
        if m:
            amt = float(m.group(3).replace("$", "").replace(",", ""))
            if amt == 0: continue
            desc = re.sub(r'\s+\d{3}\s+\d{3}$', '', m.group(2)).strip()
            if in_credits:
                amt = -abs(amt)
                # Use 1st of statement closing month; fall back to 1st of latest tx month
                ref = stmt_month_start or latest_tx_date
                tx_date = ref.replace(day=1).strftime("%Y-%m-%d") if ref else parse_date(m.group(1))
            else:
                tx_date = parse_date(m.group(1))
                try:    latest_tx_date = datetime.strptime(tx_date, "%Y-%m-%d")
                except ValueError: pass
            rows.append({"date": tx_date, "description": desc,
                         "amount": amt, "source": "Coinbase"})
    return rows

CHASE_PERIOD = re.compile(r'Opening/Closing Date\s+([\d/]+)\s*-\s*([\d/]+)')
CHASE_TX     = re.compile(r'^(\d{2}/\d{2})\s+(.+?)\s+(-?[\d,]+\.\d{2})$')
CHASE_POINTS = re.compile(r'^(\d{2}/\d{2})\s+(.+?)\s+[\d,]+\.\d{2}\s+[\d,]+$')
CHASE_SKIP   = re.compile(
    r'^(Order Number|Date of|Transaction Merchant|WILMINGTON|Pay by phone|Send Inquiries)')

def parse_chase_pdf(text: str) -> list:
    m = CHASE_PERIOD.search(text)
    if not m: return []
    open_dt  = datetime.strptime(m.group(1), "%m/%d/%y")
    close_dt = datetime.strptime(m.group(2), "%m/%d/%y")
    rows, in_p = [], False
    for line in text.split("\n"):
        s = line.strip()
        sc = re.sub(r'(.)\1+', r'\1', s).upper()
        if re.search(r'PURCHASE', sc) and not re.search(r'TOTAL|YEAR|2026', sc):
            in_p = True; continue
        if re.search(r'PAYMENT.*CREDIT|AUTOMATIC PAYMENT|INTEREST CHARGE|SHOP WITH POINTS|YEAR.TO.DATE', sc):
            in_p = False; continue
        if not in_p or CHASE_SKIP.match(s) or CHASE_POINTS.match(s): continue
        m2 = CHASE_TX.match(s)
        if m2:
            mo, day = int(m2.group(1)[:2]), int(m2.group(1)[3:])
            amt = float(m2.group(3).replace(",", ""))
            if amt == 0: continue
            d = infer_year(mo, day, open_dt, close_dt)
            if d:
                rows.append({"date": d.strftime("%Y-%m-%d"),
                             "description": m2.group(2).strip(),
                             "amount": amt, "source": "Amazon"})
    return rows

CITI_BALANCE = re.compile(r'balance as of (\d{2}/\d{2}/\d{2})', re.I)
CITI_TX      = re.compile(r'^(\d{2}/\d{2})(?:\s+\d{2}/\d{2})?\s+(.+?)\s+\$?(-?[\d,]+\.\d{2})')

def parse_citi_pdf(text: str) -> list:
    m = CITI_BALANCE.search(text)
    close_dt = datetime.strptime(m.group(1), "%m/%d/%y") if m else datetime.now()
    open_dt  = close_dt - timedelta(days=35)
    rows, in_tx, in_credits = [], False, False
    for line in text.split("\n"):
        s = line.strip()
        if re.search(r'Standard Purchases', s, re.I):
            in_tx = True; in_credits = False; continue
        if re.search(r'Payments, Credits, and Adjustments', s, re.I):
            in_credits = True; in_tx = False; continue
        if re.search(r'Total fees|Interest charged|202[56] totals', s, re.I):
            in_tx = False; in_credits = False; continue
        if not (in_tx or in_credits): continue
        if re.search(r'^Trans\.|^Post\s|^ThankYou Points|^Bonus Points', s): continue
        if in_credits and re.search(r'AUTOPAY', s, re.I): continue  # skip payments
        m2 = CITI_TX.match(s)
        if m2:
            mo, day = int(m2.group(1)[:2]), int(m2.group(1)[3:])
            amt = float(m2.group(3).replace(",", ""))
            if amt == 0: continue
            if in_credits: amt = -abs(amt)  # credits section: negate to mark as refund
            d = infer_year(mo, day, open_dt, close_dt)
            if d:
                desc = re.sub(r'\s+[A-Z]{2,3}\s*$', '', m2.group(2).strip()).strip()
                rows.append({"date": d.strftime("%Y-%m-%d"), "description": desc,
                             "amount": amt, "source": "Citi"})
    return rows

BOFA_YEAREND_TX = re.compile(r'^(\d{2}/\d{2}/\d{2})\s+(.+?)\s+([\d,]+\.\d{2})(CR)?\s*$')

def parse_bofa_yearend(text: str) -> list:
    rows = []
    for line in text.split("\n"):
        s = line.strip()
        m = BOFA_YEAREND_TX.match(s)
        if not m: continue
        try:    dt = datetime.strptime(m.group(1), "%m/%d/%y")
        except ValueError: continue
        amt = float(m.group(3).replace(",", ""))
        if m.group(4): amt = -amt  # CR suffix = credit/refund
        if amt == 0: continue
        desc_raw = m.group(2).strip()
        desc = re.sub(r'\s+\S+,\s+[A-Z]{2}$', '', desc_raw).strip() or desc_raw
        rows.append({"date": dt.strftime("%Y-%m-%d"), "description": desc,
                     "amount": amt, "source": "Bank of America"})
    return rows

BOFA_PERIOD = re.compile(
    r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d+\s*[-–]\s*'
    r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+(\d+),\s*(\d{4})', re.I)
BOFA_ESTMT_TX = re.compile(
    r'^(\d{2}/\d{2})\s+\d{2}/\d{2}\s+(.+?)\s+\d{4}\s+\d{4}\s+(-?[\d,]+\.\d{2})\s*$')

def parse_bofa_estmt(text: str) -> list:
    m = BOFA_PERIOD.search(text)
    if m:
        close_str = f"{m.group(2)} {m.group(3)}, {m.group(4)}"
        try:    close_dt = datetime.strptime(close_str, "%B %d, %Y")
        except ValueError: close_dt = datetime.strptime(close_str, "%b %d, %Y")
        open_dt = close_dt - timedelta(days=35)
    else:
        close_dt = datetime.now(); open_dt = close_dt - timedelta(days=35)
    rows, in_p = [], False
    for line in text.split("\n"):
        s = line.strip()
        if re.search(r'^Purchases and Adjustments$', s, re.I): in_p = True; continue
        if re.search(r'TOTAL PURCHASES AND ADJUSTMENTS|Interest Charged|Fees Charged|202[56] Totals', s, re.I):
            in_p = False; continue
        if not in_p: continue
        m2 = BOFA_ESTMT_TX.match(s)
        if m2:
            mo, day = int(m2.group(1)[:2]), int(m2.group(1)[3:])
            amt = float(m2.group(3).replace(",", ""))
            if amt == 0: continue
            d = infer_year(mo, day, open_dt, close_dt)
            if d:
                rows.append({"date": d.strftime("%Y-%m-%d"),
                             "description": m2.group(2).strip(),
                             "amount": amt, "source": "Bank of America"})
    return rows

def parse_bofa_pdf(text: str) -> list:
    return parse_bofa_yearend(text) if re.search(r'year.end summary', text, re.I) else parse_bofa_estmt(text)

PDF_PARSERS = {
    "Apple Card":      parse_apple_pdf,
    "Coinbase":        parse_coinbase_pdf,
    "Amazon":          parse_chase_pdf,
    "Citi":            parse_citi_pdf,
    "Bank of America": parse_bofa_pdf,
}

# ── CSV parsers ───────────────────────────────────────────────────────────────────
def parse_csv_bytes(content: bytes, filename: str) -> tuple:
    text = content.decode("utf-8", errors="replace")
    rows, source = [], None
    header_line = text.split("\n")[0].lower()

    if "amount (usd)" in header_line:
        source = "Apple Card"
        df = pd.read_csv(io.StringIO(text))
        for _, row in df.iterrows():
            if str(row.get("Type", "")).strip() in ("Purchase", "Credit"):
                rows.append({"date": parse_date(str(row["Transaction Date"])),
                             "description": str(row["Description"]).strip(),
                             "category": str(row.get("Category", "Other")).strip(),
                             "amount": float(row["Amount (USD)"]), "source": source})

    elif "amount" in header_line and "type" in header_line and "debit" not in header_line:
        source = "Amazon"
        df = pd.read_csv(io.StringIO(text))
        for _, row in df.iterrows():
            tx_type = str(row.get("Type", "")).strip()
            if tx_type in ("Sale", "Refund", "Return"):
                amt = -float(row["Amount"])  # CSV: negative=debit, positive=credit; invert to our convention
                rows.append({"date": parse_date(str(row["Transaction Date"])),
                             "description": str(row["Description"]).strip(),
                             "category": str(row.get("Category", "Other")).strip(),
                             "amount": amt, "source": source})

    else:
        # Citi CSVs may have preamble rows before the real header — scan all lines
        citi_start = next(
            (i for i, l in enumerate(text.split("\n"))
             if l.strip().lower().startswith("date,description,debit")), None)
        if citi_start is None:
            return rows, source
        source = "Citi"
        start = citi_start
        df = pd.read_csv(io.StringIO("\n".join(text.split("\n")[start:])))
        for _, row in df.iterrows():
            debit = row.get("Debit", "")
            credit = row.get("Credit", "")
            if pd.notna(debit) and str(debit).strip() not in ("", "nan"):
                try:
                    amt = float(str(debit).replace(",", "").replace('"', "").strip())
                    if amt > 0:
                        rows.append({"date": parse_date(str(row["Date"]).strip()),
                                     "description": str(row["Description"]).strip(),
                                     "category": str(row.get("Category", "Other")).strip(),
                                     "amount": amt, "source": source})
                except (ValueError, TypeError):
                    pass
            elif pd.notna(credit) and str(credit).strip() not in ("", "nan"):
                try:
                    amt = float(str(credit).replace(",", "").replace('"', "").strip())
                    if amt != 0:  # already negative in CSV, store as-is
                        rows.append({"date": parse_date(str(row["Date"]).strip()),
                                     "description": str(row["Description"]).strip(),
                                     "category": str(row.get("Category", "Other")).strip(),
                                     "amount": amt, "source": source})
                except (ValueError, TypeError):
                    pass

    return rows, source

# ── Main parse dispatcher ─────────────────────────────────────────────────────────
def parse_file_bytes(content: bytes, filename: str) -> tuple:
    fname = filename.lower()
    rows, source = [], None
    try:
        if fname.endswith(".pdf"):
            with pdfplumber.open(io.BytesIO(content)) as pdf:
                text = "\n".join(p.extract_text() or "" for p in pdf.pages)
            source = detect_source(text)
            if not source:
                return [], None, f"Could not detect card type from '{filename}'"
            rows = PDF_PARSERS[source](text)
        elif fname.endswith(".csv"):
            rows, source = parse_csv_bytes(content, filename)
            if not source:
                return [], None, f"Unrecognized CSV format in '{filename}'"
        else:
            return [], None, f"Unsupported file type: '{filename}' (use PDF or CSV)"
    except Exception as e:
        return [], None, f"Parse error in '{filename}': {e}"

    if not rows:
        return [], source, f"No transactions found in '{filename}'"

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

@app.on_event("startup")
def startup():
    init_db()

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
    page: int = 1, per_page: int = 50,
    source: str = "", category: str = "", search: str = "",
    date_from: str = "", date_to: str = "",
    import_file: str = "",
    sort_by: str = "date", sort_dir: str = "desc",
    status: str = "active",
    user: dict = Depends(get_current_user)
):
    where, params = ["user_id = %s"], [user["id"]]
    if status in ("active", "deleted", "deduped"):
        where.append("status = %s"); params.append(status)
    if source:      where.append("source = %s");          params.append(source)
    if category:    where.append("category = %s");        params.append(category)
    if date_from:   where.append("date >= %s");           params.append(date_from)
    if date_to:     where.append("date <= %s");           params.append(date_to)
    if search:      where.append("description ILIKE %s"); params.append(f"%{search}%")
    if import_file: where.append("import_file = %s");     params.append(import_file)
    wc = " AND ".join(where)

    valid_cols = {"date", "amount", "description", "category", "source"}
    sc = sort_by if sort_by in valid_cols else "date"
    sd = "DESC" if sort_dir == "desc" else "ASC"
    offset = (page - 1) * per_page

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT COUNT(*) as n FROM transactions WHERE {wc}", params)
            total = cur.fetchone()["n"]
            cur.execute(f"""
                SELECT id, date::text, description, category,
                       amount::float, source, manually_corrected, import_file,
                       status, dedup_of
                FROM transactions WHERE {wc}
                ORDER BY {sc} {sd}, id {sd}
                LIMIT %s OFFSET %s
            """, params + [per_page, offset])
            rows = [dict(r) for r in cur.fetchall()]

    return {"transactions": rows, "total": total, "page": page,
            "per_page": per_page, "pages": max(1, (total + per_page - 1) // per_page)}

@app.get("/api/stats")
def get_stats(
    source: str = "", category: str = "", date_from: str = "",
    date_to: str = "", import_file: str = "",
    user: dict = Depends(get_current_user)
):
    where, params = ["status = 'active'", "user_id = %s"], [user["id"]]
    if source:      where.append("source = %s");      params.append(source)
    if category:    where.append("category = %s");    params.append(category)
    if date_from:   where.append("date >= %s");       params.append(date_from)
    if date_to:     where.append("date <= %s");       params.append(date_to)
    if import_file: where.append("import_file = %s"); params.append(import_file)
    wc = " AND ".join(where)

    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SELECT category, SUM(amount)::float AS total, COUNT(*)::int AS count FROM transactions WHERE {wc} GROUP BY category ORDER BY total DESC", params)
            by_category = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT TO_CHAR(date,'YYYY-MM') AS month, SUM(amount)::float AS total FROM transactions WHERE {wc} GROUP BY month ORDER BY month", params)
            by_month = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT source, SUM(amount)::float AS total, COUNT(*)::int AS count FROM transactions WHERE {wc} GROUP BY source ORDER BY total DESC", params)
            by_source = [dict(r) for r in cur.fetchall()]
            cur.execute(f"SELECT COALESCE(SUM(amount),0)::float AS total, COUNT(*)::int AS count FROM transactions WHERE {wc}", params)
            summary = dict(cur.fetchone())

    return {**summary, "by_category": by_category, "by_month": by_month, "by_source": by_source}

# ── Category update ───────────────────────────────────────────────────────────────
class CategoryUpdate(BaseModel):
    category: str

@app.patch("/api/transactions/{tx_id}")
def update_category(tx_id: int, body: CategoryUpdate, user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE transactions SET category = %s, manually_corrected = TRUE
                WHERE id = %s AND user_id = %s RETURNING id
            """, (body.category, tx_id, user["id"]))
            if cur.rowcount == 0:
                raise HTTPException(404, "Transaction not found")
    return {"ok": True, "id": tx_id, "category": body.category}

class BulkCategoryUpdate(BaseModel):
    ids: List[int]; category: str

@app.patch("/api/transactions")
def bulk_update_category(body: BulkCategoryUpdate, user: dict = Depends(get_current_user)):
    if not body.ids:
        raise HTTPException(400, "No IDs provided")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE transactions SET category = %s, manually_corrected = TRUE
                WHERE id = ANY(%s) AND user_id = %s
            """, (body.category, body.ids, user["id"]))
            updated = cur.rowcount
    return {"ok": True, "updated": updated, "category": body.category}

# ── Soft-delete ───────────────────────────────────────────────────────────────────
@app.delete("/api/transactions/{tx_id}")
def delete_transaction(tx_id: int, user: dict = Depends(get_current_user)):
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
def bulk_delete_transactions(body: BulkDelete, user: dict = Depends(get_current_user)):
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
def restore_transaction(tx_id: int, user: dict = Depends(get_current_user)):
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
def bulk_restore_transactions(body: BulkRestore, user: dict = Depends(get_current_user)):
    if not body.ids: raise HTTPException(400, "No IDs provided")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE transactions SET status='active', dedup_of=NULL WHERE id=ANY(%s) AND user_id=%s",
                (body.ids, user["id"]))
            restored = cur.rowcount
    return {"ok": True, "restored": restored}

# ── Upload ────────────────────────────────────────────────────────────────────────
@app.post("/api/upload")
async def upload_files(files: List[UploadFile] = File(...),
                       force: bool = False,
                       user: dict = Depends(get_current_user)):
    user_id = user["id"]
    results = []
    with db() as conn:
        manual_tx, all_tx = load_all_tx(conn, user_id)
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM categories WHERE user_id = %s ORDER BY name", (user_id,))
            cat_list = [r[0] for r in cur.fetchall()]
        valid_cats = set(cat_list)

        for f in files:
            content   = await f.read()
            file_hash = hashlib.md5(content).hexdigest()

            if not force:
                with conn.cursor() as cur:
                    cur.execute("SELECT id FROM uploaded_files WHERE user_id=%s AND file_hash=%s",
                                (user_id, file_hash))
                    if cur.fetchone():
                        results.append({"filename": f.filename, "status": "already_imported",
                                        "message": "File was already imported", "new": 0, "dupes": 0})
                        continue

            rows, source, error = parse_file_bytes(content, f.filename)
            if error:
                results.append({"filename": f.filename, "status": "error",
                                 "message": error, "new": 0, "dupes": 0})
                continue

            needs_gpt = []
            for r in rows:
                existing = r.get("category", "Other")
                if existing and existing not in ("Other", "") and existing in valid_cats:
                    continue
                match = find_db_match(r["description"], manual_tx, all_tx, valid_cats)
                if match:
                    r["category"] = match
                else:
                    r["category"] = "Other"
                    needs_gpt.append(r)
            if needs_gpt:
                gpt_map = categorize_with_gpt([r["description"] for r in needs_gpt], cat_list)
                for r in needs_gpt:
                    r["category"] = gpt_map.get(r["description"], "Other")

            new_count = dupe_count = 0
            with conn.cursor() as cur:
                # Use existing display name if file was previously renamed
                cur.execute("SELECT filename FROM uploaded_files WHERE user_id=%s AND file_hash=%s",
                            (user_id, file_hash))
                row = cur.fetchone()
                import_name = row[0] if row else f.filename

                for r in rows:
                    cur.execute(
                        "SELECT id FROM transactions WHERE user_id=%s AND dedup_key=%s AND status='active' LIMIT 1",
                        (user_id, r["dedup_key"]))
                    is_dupe   = cur.fetchone() is not None
                    tx_status = "deduped" if is_dupe else "active"
                    cur.execute("""
                        INSERT INTO transactions
                            (user_id, date, description, category, amount, source,
                             dedup_key, status, dedup_of, import_file)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (user_id, r["date"], r["description"], r["category"],
                          r["amount"], r["source"], r["dedup_key"],
                          tx_status, r["dedup_key"] if is_dupe else None, import_name))
                    if tx_status == "active": new_count  += 1
                    else:                     dupe_count += 1

                cur.execute("""
                    INSERT INTO uploaded_files (user_id, filename, file_hash, source, tx_new, tx_dupes)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (user_id, file_hash) DO NOTHING
                """, (user_id, f.filename, file_hash, source, new_count, dupe_count))

            results.append({"filename": f.filename, "status": "ok", "source": source,
                            "new": new_count, "dupes": dupe_count})

    return {"results": results}

# ── Categories ────────────────────────────────────────────────────────────────────
@app.get("/api/categories")
def get_categories(user: dict = Depends(get_current_user)):
    uid = user["id"]
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM categories WHERE user_id=%s ORDER BY name", (uid,))
            cats = [r[0] for r in cur.fetchall()]
    return {"categories": cats}

class CategoryCreate(BaseModel):
    name: str

@app.post("/api/categories", status_code=201)
def create_category(body: CategoryCreate, user: dict = Depends(get_current_user)):
    name = body.name.strip()
    if not name: raise HTTPException(400, "Category name cannot be empty")
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO categories (user_id, name) VALUES (%s,%s) ON CONFLICT DO NOTHING RETURNING name",
                (user["id"], name))
            if cur.rowcount == 0: raise HTTPException(409, "Category already exists")
    return {"ok": True, "name": name}

class CategoryRename(BaseModel):
    old_name: str; new_name: str

@app.patch("/api/categories")
def rename_category(body: CategoryRename, user: dict = Depends(get_current_user)):
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
def delete_category(name: str, user: dict = Depends(get_current_user)):
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

# ── Upload history ────────────────────────────────────────────────────────────────
@app.get("/api/uploads")
def get_uploads(user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT filename, file_hash, source, tx_new, tx_dupes,
                       to_char(uploaded_at,'YYYY-MM-DD HH24:MI') as uploaded_at
                FROM uploaded_files WHERE user_id=%s
                ORDER BY uploaded_at DESC LIMIT 50
            """, (user["id"],))
            return {"uploads": [dict(r) for r in cur.fetchall()]}

class UploadRename(BaseModel):
    old_name: str
    new_name: str

@app.patch("/api/uploads/rename")
def rename_upload(body: UploadRename, user: dict = Depends(get_current_user)):
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

@app.delete("/api/uploads")
def delete_upload_record(filename: str, user: dict = Depends(get_current_user)):
    with db() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM uploaded_files WHERE user_id=%s AND filename=%s RETURNING id",
                (user["id"], filename))
            if cur.rowcount == 0: raise HTTPException(404, "Upload record not found")
    return {"ok": True, "filename": filename}

# ── Run ───────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, reload=True)

"""Plaid HTTP client and row mapping for bank sync. No database access here.

Bank sync is off unless PLAID_CLIENT_ID, PLAID_SECRET and PLAID_TOKEN_KEY are
all set. Access tokens are encrypted with PLAID_TOKEN_KEY (a Fernet key) before
they reach the database. Nothing in this module logs tokens, secrets or
transaction text.
"""
import os
from datetime import date, datetime

import httpx
from cryptography.fernet import Fernet, InvalidToken

HOSTS = {'sandbox': 'https://sandbox.plaid.com', 'production': 'https://production.plaid.com'}
HISTORY_DAYS = 730          # fixed when an Item is created; sync_from narrows it per account
KEY_PREFIX = 'plaid:'       # transactions.dedup_key = KEY_PREFIX + Plaid transaction_id
MAX_PAGES = 200             # 500 rows a page; bounds a runaway has_more loop


class PlaidError(Exception):
    """A Plaid API failure. `code` is Plaid's error_code, safe to store and show."""
    def __init__(self, code, message=''):
        super().__init__(code)
        self.code = code
        self.message = message or code


def _env(name):
    return os.getenv(name, '').strip()


def enabled():
    return bool(_env('PLAID_CLIENT_ID') and _env('PLAID_SECRET') and _env('PLAID_TOKEN_KEY')
                and plaid_env() in HOSTS)


def plaid_env():
    return _env('PLAID_ENV').lower() or 'production'


def redirect_uri():
    """Optional. Required by OAuth banks (Chase, Bank of America, Citi); must be
    registered in the Plaid dashboard and point at this app's home page."""
    return _env('PLAID_REDIRECT_URI')


def _fernet():
    return Fernet(_env('PLAID_TOKEN_KEY').encode())


def encrypt_token(token):
    return _fernet().encrypt(token.encode()).decode()


def decrypt_token(stored):
    try:
        return _fernet().decrypt(stored.encode()).decode()
    except (InvalidToken, ValueError):
        raise PlaidError('TOKEN_KEY_MISMATCH', 'Stored bank connection cannot be read with the current PLAID_TOKEN_KEY') from None


def post(path, body, timeout=30):
    """POST to Plaid with the app credentials; returns the JSON body or raises PlaidError."""
    payload = {'client_id': _env('PLAID_CLIENT_ID'), 'secret': _env('PLAID_SECRET'), **body}
    try:
        response = httpx.post(HOSTS[plaid_env()] + path, json=payload, timeout=timeout)
        data = response.json()
    except (httpx.HTTPError, ValueError):
        raise PlaidError('PLAID_UNREACHABLE', 'Could not reach Plaid') from None
    if response.status_code != 200:
        code = str(data.get('error_code') or f'HTTP_{response.status_code}') if isinstance(data, dict) else f'HTTP_{response.status_code}'
        shown = data.get('display_message') if isinstance(data, dict) else None
        raise PlaidError(code, shown or code)
    return data


def create_link_token(client_user_id, access_token=None):
    """A Link token for a new connection, or update mode when access_token is given."""
    body = {'client_name': 'Spending Dashboard', 'language': 'en', 'country_codes': ['US'],
            'user': {'client_user_id': str(client_user_id)}}
    if access_token:
        body['access_token'] = access_token
    else:
        body['products'] = ['transactions']
        body['transactions'] = {'days_requested': HISTORY_DAYS}
    if redirect_uri():
        body['redirect_uri'] = redirect_uri()
    return post('/link/token/create', body)['link_token']


def exchange_public_token(public_token):
    data = post('/item/public_token/exchange', {'public_token': public_token})
    return data['access_token'], data['item_id']


def get_accounts(access_token):
    return post('/accounts/get', {'access_token': access_token})['accounts']


def remove_item(access_token):
    post('/item/remove', {'access_token': access_token})


def sync_transactions(access_token, cursor):
    """Everything since `cursor`. Returns (added, modified, removed_ids, next_cursor).

    Plaid can change the data mid-pagination; it then asks for a restart from
    the cursor the loop began with.
    """
    for _ in range(3):
        added, modified, removed, current = [], [], [], cursor
        try:
            for _page in range(MAX_PAGES):
                body = {'access_token': access_token, 'count': 500}
                if current:
                    body['cursor'] = current
                data = post('/transactions/sync', body, timeout=60)
                added += data.get('added', [])
                modified += data.get('modified', [])
                removed += [r['transaction_id'] for r in data.get('removed', [])]
                current = data['next_cursor']
                if not data.get('has_more'):
                    return added, modified, removed, current
            raise PlaidError('SYNC_TOO_LARGE', 'Bank sync returned too many pages')
        except PlaidError as exc:
            if exc.code != 'TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION':
                raise
    raise PlaidError('TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION', 'Bank data kept changing; try again later')


def to_row(tx):
    """Map one posted Plaid transaction to the dashboard's row shape, or None to skip.

    Plaid's sign already matches the dashboard: positive is money out of the
    account. Pending rows are skipped; the posted version arrives later with
    its own id. The purchase date (authorized_date) is preferred over the
    posting date, as on a card statement.
    """
    if tx.get('pending'):
        return None
    try:
        amount = round(float(tx['amount']), 2)
        day = str(tx.get('authorized_date') or tx['date'])
        datetime.strptime(day, '%Y-%m-%d')
        description = str(tx.get('name') or tx.get('merchant_name') or '').strip()
        key = KEY_PREFIX + str(tx['transaction_id'])
        account_id = str(tx['account_id'])
    except (KeyError, TypeError, ValueError):
        return None
    if not description or amount == 0 or amount != amount or abs(amount) == float('inf'):
        return None
    return {'date': day, 'description': description, 'amount': amount,
            'dedup_key': key, 'account_id': account_id}


def parse_day(value):
    return datetime.strptime(value, '%Y-%m-%d').date() if isinstance(value, str) else value


def default_sync_from(latest):
    """Start the day after the latest statement row for the source, else today."""
    return date.fromordinal(parse_day(latest).toordinal() + 1) if latest else date.today()


# ── Matching bank rows to statement rows ──────────────────────────────────────
MATCH_DAYS = 3              # statements and banks can date one charge a few days apart
_GENERIC_WORDS = {'THE', 'AND', 'FOR', 'POS', 'DEBIT', 'CREDIT', 'CARD', 'PURCHASE', 'PAYMENT',
                  'ONLINE', 'WWW', 'COM', 'INC', 'LLC', 'USA'}


def _words(description):
    return {w for w in ''.join(c if c.isalnum() else ' ' for c in str(description).upper()).split()
            if len(w) >= 3 and not w.isdigit() and w not in _GENERIC_WORDS}


def match_rows(incoming, candidates):
    """Pair incoming rows with stored rows that are the same charge seen through
    the other channel (statement upload or bank sync).

    Rows are dicts with date, amount, source, description and dedup_key. A pair
    needs the same source, the same amount to the cent and dates within
    MATCH_DAYS. It is trusted when the dates are equal or the descriptions
    share a word; pairing is one-to-one, best pairs first.

    Returns (matches, possible): matches maps an incoming dedup_key to the
    stored row's dedup_key; possible holds incoming keys that only have an
    untrusted candidate left, for the caller to flag for review.
    """
    by_bucket = {}
    for cand in candidates:
        by_bucket.setdefault((cand['source'], round(float(cand['amount']) * 100)), []).append(cand)
    trusted, weak = [], []
    for row in incoming:
        day, words = parse_day(row['date']).toordinal(), _words(row['description'])
        for cand in by_bucket.get((row['source'], round(float(row['amount']) * 100)), []):
            gap = abs(parse_day(cand['date']).toordinal() - day)
            if gap > MATCH_DAYS:
                continue
            shared = bool(words & _words(cand['description']))
            pair = ((0 if shared else 1, gap, row['dedup_key'], cand['dedup_key']))
            (trusted if shared or gap == 0 else weak).append(pair)
    matches, used = {}, set()
    for _, _, key, cand_key in sorted(trusted):
        if key not in matches and cand_key not in used:
            matches[key] = cand_key
            used.add(cand_key)
    possible = {key for _, _, key, cand_key in trusted + weak
                if key not in matches and cand_key not in used}
    return matches, possible

"""Independent checks on dated rows printed in supported card PDF statements.

These checks never infer categories or alter statement values. An unfamiliar layout
is left to the normal parser; a recognized but inconsistent layout fails closed.
"""
import re
from collections import Counter
from decimal import Decimal, InvalidOperation
from datetime import datetime


MONEY = re.compile(r"(?<!\w)-?\$[\d,]+\.\d{2}\b")
APPLE_DATE = re.compile(r"^(\d{2}/\d{2}/\d{4})\s+(.+)$")
COINBASE_DATE = re.compile(r"^([A-Z][a-z]{2} \d{1,2}, \d{4})\s+(.+)$")


def detect_account_key(pages, source):
    """Use the account printed on a BofA statement, never a linked account."""
    if source != 'Bank of America' or not pages:
        return None
    first = pages[0]
    match = re.search(r'Account\s*(?:number|#)\s*:?\s*((?:\d[ -]*){8,})',
                      first, re.I)
    if match:
        digits = re.sub(r'\D', '', match.group(1))
        if len(digits) >= 8:
            return 'bofa:' + digits[-4:]
    match = re.search(r'credit card ending in\s+(\d{4})', first, re.I)
    if match:
        return 'bofa:' + match.group(1)
    return None


class StatementMismatch(ValueError):
    def __init__(self, message, missing_lines=None, extra=0):
        super().__init__(message)
        self.missing_lines = missing_lines or []
        self.extra = extra


def cents(value):
    return int((Decimal(str(value).replace('$', '').replace(',', '')) * 100).quantize(Decimal('1')))


def _printed_total(text, pattern):
    match = re.search(pattern, text, re.I)
    return cents(match.group(1)) if match else None


def _statement_year_month(first, filename):
    # The statement period wins over a filename; one saved Citi PDF is
    # misnamed with 2025 even though its printed billing period is in 2026.
    match = re.search(r'Billing Period:\s*\d{2}/\d{2}/\d{2}-\d{2}/\d{2}/(\d{2})', first)
    if match:
        return 2000 + int(match.group(1)), int(re.search(
            r'Billing Period:\s*\d{2}/\d{2}/\d{2}-([0-9]{2})', first).group(1))
    match = re.search(r'20\d{2}-(\d{2})(?:-\d{2})?', filename)
    if match:
        year = int(filename[match.start():match.start()+4])
        return year, int(match.group(1))
    match = re.search(r'\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*[ -]?(20\d{2}|\d{2})\b',
                      filename, re.I)
    if match:
        year = int(match.group(2))
        return (year if year > 2000 else 2000+year,
                datetime.strptime(match.group(1).title(), '%b').month)
    match = re.search(r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+(20\d{2})', first)
    if match:
        return int(match.group(2)), datetime.strptime(match.group(1), '%B').month
    return None


def _reconcile_other_cards(pages, source, rows, filename):
    """Check transaction ledgers for recognized BofA, Citi, and Chase layouts."""
    first = pages[0]
    text = '\n'.join(pages)
    kind = None
    printed = None
    if source == 'Bank of America':
        if 'Beginning balance on' in first and 'Ending balance on' in first:
            kind = 'bofa_checking'
            begin = _printed_total(first, r'Beginning balance on[^\n]*?(\$[\d,]+\.\d{2})')
            end = _printed_total(first, r'Ending balance on[^\n]*?(\$[\d,]+\.\d{2})')
            if begin is not None and end is not None:
                printed = begin - end
        elif 'year-end summary of credit card transactions' in first.lower():
            kind = 'bofa_yearend'
            printed = _printed_total(first, r'Total spent Total interest\s+(\$[\d,]+\.\d{2})')
        elif 'Previous Balance' in first and 'New Balance Total' in first:
            kind = 'bofa_card'
            begin = _printed_total(first, r'Previous Balance\s+(\$[\d,]+\.\d{2})')
            end = _printed_total(first, r'New Balance Total\s+(\$[\d,]+\.\d{2})')
            if begin is not None and end is not None:
                printed = end - begin
    elif source == 'Amazon' and re.search(
            r'^Transaction Merchant Name or Transaction Description \$ Amount\s*$', text, re.M):
        # Chase's extracted account-activity title can have doubled letters;
        # the transaction header is the reliable section marker.
        kind = 'amazon'
        begin = _printed_total(first, r'Previous Balance\s+(\$[\d,]+\.\d{2})')
        end = _printed_total(first, r'New Balance\s+(\$[\d,]+\.\d{2})')
        if begin is not None and end is not None:
            printed = end - begin
    elif source == 'Citi' and 'Billing Period:' in first:
        kind = 'citi'
        begin = _printed_total(first, r'Previous balance\s+(\$[\d,]+\.\d{2})')
        end = _printed_total(first, r'New balance\s+(\$[\d,]+\.\d{2})')
        if begin is not None and end is not None:
            printed = end - begin
    if not kind:
        return None
    period = _statement_year_month(first, filename)
    if kind in ('bofa_card', 'citi', 'amazon') and period is None:
        raise ValueError('Statement period could not be verified for reconciliation')

    ledger = []
    def add(date, amount, line, alternate=None):
        if amount:
            ledger.append((date, alternate, amount, line))

    for page_number, page in enumerate(pages, 1):
        if kind == 'amazon':
            in_activity = in_rewards = False
        for line in page.splitlines():
            line = line.strip()
            if kind == 'amazon':
                if ('Transaction Merchant Name or Transaction Description $ Amount' in line
                        and 'Rewards' not in line):
                    in_activity = True
                if ('Transaction Merchant Name or Transaction Description $ Amount Rewards' in line
                        or line in ('SHOP WITH POINTS ACTIVITY','PURCHASES AND REDEMPTIONS',
                                    'RETURNS AND OTHER CREDITS')):
                    in_rewards = True
                if page_number < 3 or not in_activity or in_rewards:
                    continue
                match = re.match(r'^(\d{2})/(\d{2})\s+.+?\s+(-?[\d,]+\.\d{2})$', line)
                if match:
                    month, day = int(match.group(1)), int(match.group(2))
                    year = period[0] - (month > period[1])
                    add(datetime(year,month,day).strftime('%Y-%m-%d'),
                        cents(match.group(3)), line)
            elif kind == 'bofa_checking' and page_number >= 3:
                match = re.match(r'^(\d{2}/\d{2}/\d{2})\s+.+?\s+(-?[\d,]+\.\d{2})$', line)
                if match:
                    date = datetime.strptime(match.group(1), '%m/%d/%y').strftime('%Y-%m-%d')
                    add(date, -cents(match.group(2)), line)
            elif kind == 'bofa_yearend' and page_number >= 2:
                match = re.match(r'^(\d{2}/\d{2}/\d{2})\s+.+?\s+(-?[\d,]+\.\d{2})(CR)?$', line)
                if match:
                    date = datetime.strptime(match.group(1), '%m/%d/%y').strftime('%Y-%m-%d')
                    amount = -abs(cents(match.group(2))) if match.group(3) else cents(match.group(2))
                    add(date, amount, line)
            elif kind in ('bofa_card', 'citi') and page_number >= 3:
                pattern = (r'^(\d{2})/(\d{2})\s+(\d{2})/(\d{2})\s+.+?\s+(-?[\d,]+\.\d{2})(CR)?$'
                           if kind == 'bofa_card' else
                           r'^(\d{2})/(\d{2})(?:\s+(\d{2})/(\d{2}))?\s+.+?\s+(-?\$[\d,]+\.\d{2})(?:\s|$)')
                match = re.match(pattern, line)
                if match:
                    month, day = int(match.group(1)), int(match.group(2))
                    date = datetime(period[0] - (month > period[1]),month,day).strftime('%Y-%m-%d')
                    alternate = None
                    if match.group(3):
                        post_month, post_day = int(match.group(3)), int(match.group(4))
                        alternate = datetime(period[0] - (post_month > period[1]),
                                             post_month,post_day).strftime('%Y-%m-%d')
                    amount = (-abs(cents(match.group(5))) if kind == 'bofa_card' and match.group(6)
                              else cents(match.group(5)))
                    add(date, amount, line, alternate)

    if not ledger:
        raise ValueError('Statement reconciliation found no dated account-activity rows')
    total = sum(item[2] for item in ledger)
    if printed is not None and total != printed:
        raise ValueError('Statement printed total disagrees with dated account activity')

    remaining = Counter((str(r['date']), cents(r['amount'])) for r in rows)
    missing = []
    for entry in ledger:
        key = (entry[0], entry[2])
        if remaining[key]:
            remaining[key] -= 1
        else:
            missing.append(entry)
    unmatched = []
    for entry in missing:
        key = (entry[1], entry[2])
        if entry[1] and remaining[key]:
            remaining[key] -= 1
        else:
            unmatched.append(entry)
    extra = sum(remaining.values())
    if unmatched or extra:
        raise StatementMismatch(
            f'Statement reconciliation failed: {len(unmatched)} missing, {extra} unexpected dated rows; '
            f'dated net {total/100:.2f}. No rows were saved.',
            missing_lines=[f'account activity: {entry[3]}' for entry in unmatched], extra=extra)
    return {'rows':len(ledger), 'net':round(total/100,2)}


def reconcile_card_statement(pages, source, rows, filename=''):
    """Return a reconciliation summary or raise ValueError before DB insertion.

    Match a multiset of (date, signed amount), which catches missing identical
    small charges as well as missing credits. Descriptions remain GPT's concern.
    """
    if not pages:
        return None
    if source not in ('Apple Card', 'Coinbase'):
        return _reconcile_other_cards(pages, source, rows, filename)
    text = '\n'.join(pages)
    if source == 'Apple Card':
        if not re.search(r'^Total charges, credits and returns\s', text, re.M):
            return None
        date_re = APPLE_DATE
        date_fmt = '%m/%d/%Y'
        totals = {
            'transactions': _printed_total(text, r'Total charges, credits and returns\s+(-?\$[\d,]+\.\d{2})'),
            'payments': _printed_total(text, r'Total payments for this period\s+(-?\$[\d,]+\.\d{2})'),
        }
    else:
        if not re.search(r'^Total new charges in this period\s', text, re.M):
            return None
        date_re = COINBASE_DATE
        date_fmt = '%b %d, %Y'
        totals = {
            'transactions': _printed_total(text, r'Total new charges in this period\s+(-?\$[\d,]+\.\d{2})'),
            'payments': _printed_total(text, r'Total payments and credits in this period\s+(-?\$[\d,]+\.\d{2})'),
        }

    if totals['transactions'] is None:
        raise ValueError('Statement transaction total could not be read')
    has_payment_total = (
        (source == 'Apple Card' and 'Total payments for this period' in text) or
        (source == 'Coinbase' and 'Total payments and credits in this period' in text)
    )
    if has_payment_total:
        if totals['payments'] is None:
            raise ValueError('Statement payment total could not be read')

    ledger = Counter()
    evidence = {}
    section_totals = {'transactions': 0, 'payments': 0}
    section = None
    for line in text.splitlines():
        line = line.strip()
        if line in ('Payments', 'Payments and credits'):
            section = 'payments'
        elif line == 'Transactions':
            section = 'transactions'
        elif line.startswith(('Total charges, credits and returns', 'Total new charges in this period')):
            section = None
        match = date_re.match(line)
        if not match or not section:
            continue
        amounts = MONEY.findall(match.group(2))
        if not amounts:
            raise ValueError(f'Statement reconciliation cannot read amount on dated {section} line')
        amount = cents(amounts[-1])
        # Card payments are credits even when an issuer prints an unsigned amount.
        if section == 'payments' and amount > 0:
            amount = -amount
        date = datetime.strptime(match.group(1), date_fmt).strftime('%Y-%m-%d')
        ledger[(date, amount)] += 1
        evidence.setdefault((date, amount), []).append(f'{section}: {line}')
        section_totals[section] += amount

    if not ledger:
        raise ValueError('Statement reconciliation found no dated card rows')
    adjustments = 0
    if source == 'Apple Card':
        # Apple prints a non-dated reversal of earned Daily Cash when a purchase
        # is returned. It affects the section total but is not a dated row.
        for line in text.splitlines():
            if line.strip().startswith('Daily Cash Adjustment'):
                amounts = MONEY.findall(line)
                if amounts:
                    adjustments += cents(amounts[-1])
    for section, printed in totals.items():
        expected = section_totals[section] + (adjustments if section == 'transactions' else 0)
        if printed is not None and printed != expected:
            raise ValueError(f'Statement {section} total disagrees with its dated rows')

    try:
        parsed = Counter((str(r['date']), cents(r['amount'])) for r in rows)
    except (KeyError, InvalidOperation, ValueError) as exc:
        raise ValueError('Parsed row has an invalid date or amount') from exc
    missing = ledger - parsed
    extra = parsed - ledger
    if missing or extra:
        missing_lines = []
        for key, count in missing.items():
            missing_lines.extend(evidence[key][:count])
        raise StatementMismatch(
            f'Statement reconciliation failed: {sum(missing.values())} missing, '
            f'{sum(extra.values())} unexpected dated rows; '
            f'dated net {sum(section_totals.values()) / 100:.2f}, '
            f'parsed net {sum(amount * count for (_, amount), count in parsed.items()) / 100:.2f}. '
            'No rows were saved. Re-upload after the extraction issue is fixed.',
            missing_lines=missing_lines, extra=sum(extra.values())
        )
    return {'rows': sum(ledger.values()), 'net': round(sum(section_totals.values()) / 100, 2),
            'transactions_net': round(section_totals['transactions'] / 100, 2),
            'payments_net': round(section_totals['payments'] / 100, 2),
            'undated_adjustments': round(adjustments / 100, 2)}

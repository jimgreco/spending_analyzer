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


def reconcile_card_statement(pages, source, rows):
    """Return a reconciliation summary or raise ValueError before DB insertion.

    Match a multiset of (date, signed amount), which catches missing identical
    small charges as well as missing credits. Descriptions remain GPT's concern.
    """
    if source not in ('Apple Card', 'Coinbase') or not pages:
        return None
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

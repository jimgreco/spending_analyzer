"""Explicit financial-flow types and conservative, read-only ledger summaries.

Amounts follow the existing convention: charges are positive, credits negative.
Categories describe purpose; they do not determine a transaction's financial flow.
"""

from collections import Counter
from decimal import Decimal


TYPES = ("expense", "income", "transfer", "refund")


def sign_issue(kind, amount):
    """Report a sign that conflicts with a reviewed type, without changing it."""
    if kind is None:
        return None
    if amount == 0:
        return "zero_amount"
    if kind == "expense" and amount < 0:
        return "expense_credit"
    if kind in ("income", "refund") and amount > 0:
        return f"{kind}_debit"
    return None


def preview_candidate(row):
    """Offer a historical review hint, never an automatic classification."""
    amount = Decimal(str(row["amount"]))
    tag = (row.get("primary_tag") or "").strip().casefold()
    if amount == 0:
        return None, "zero_amount"
    if tag in ("transfers", "transfer"):
        return "transfer", "category_hint_verify_account_flow"
    if tag in ("income", "salary", "payroll"):
        return ("income", "category_hint_verify_credit") if amount < 0 else (None, "income_tag_positive_amount")
    if tag in ("refund", "refunds", "reimbursements", "reimbursement"):
        return None, "refund_or_income_ambiguous"
    if row.get("excluded"):
        return None, "excluded_category_flow_unknown"
    if amount < 0:
        return None, "credit_could_be_refund_income_or_transfer"
    return "expense", "positive_amount_verify_not_transfer_or_fee_credit"


def summarize_ledger(rows):
    """Summarize active rows. Excluded rows retain the legacy exclusion policy."""
    cents = lambda value: round(float(value), 2)
    legacy_total = gross = refunds = income = transfer_net = untyped_net = Decimal("0")
    excluded_amount = ambiguous_sign_net = Decimal("0")
    counts = Counter()
    months = {}
    for row in rows:
        amount = Decimal(str(row["amount"]))
        kind = row.get("transaction_type")
        excluded = bool(row.get("excluded"))
        category_review = bool(row.get("needs_review"))
        issue = sign_issue(kind, amount)
        counts["active_count"] += 1
        counts["category_review_count"] += category_review
        counts["type_review_count"] += kind is None
        counts["sign_review_count"] += issue is not None
        counts["review_count"] += category_review or kind is None or issue is not None
        if excluded:
            counts["excluded_count"] += 1
            excluded_amount += amount
            continue
        counts["included_count"] += 1
        legacy_total += amount
        month = str(row["date"])[:7]
        bucket = months.setdefault(month, {"gross_charges": Decimal("0"),
                                           "refunds": Decimal("0"), "net_spend": Decimal("0")})
        if kind is None:
            counts["untyped_count"] += 1
            untyped_net += amount
        elif issue:
            counts["ambiguous_sign_count"] += 1
            ambiguous_sign_net += amount
        elif kind == "expense":
            counts["expense_count"] += 1
            gross += amount
            bucket["gross_charges"] += amount
            bucket["net_spend"] += amount
        elif kind == "refund":
            counts["refund_count"] += 1
            refunds -= amount
            bucket["refunds"] -= amount
            bucket["net_spend"] += amount
        elif kind == "income":
            counts["income_count"] += 1
            income -= amount
        elif kind == "transfer":
            counts["transfer_count"] += 1
            transfer_net += amount
    return {
        "basis": "reviewed_types_only",
        "legacy_total": cents(legacy_total),
        "gross_charges": cents(gross), "refunds": cents(refunds),
        "net_spend": cents(gross - refunds),
        "income_credits": cents(income), "transfer_net": cents(transfer_net),
        "untyped_signed_total": cents(untyped_net),
        "ambiguous_sign_signed_total": cents(ambiguous_sign_net),
        "excluded_signed_total": cents(excluded_amount),
        **{key: counts[key] for key in (
            "active_count", "included_count", "excluded_count", "category_review_count",
            "type_review_count", "sign_review_count", "review_count", "untyped_count",
            "ambiguous_sign_count", "expense_count", "refund_count", "income_count", "transfer_count")},
        "by_month": [{"month": month, **{key: cents(value) for key, value in bucket.items()}}
                     for month, bucket in sorted(months.items())],
    }

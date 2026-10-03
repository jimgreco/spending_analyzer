"""Synthetic financial-flow checks; no database or personal transactions."""

from decimal import Decimal
import unittest

from transaction_types import preview_candidate, sign_issue, summarize_ledger


def row(amount, kind=None, *, excluded=False, review=False, tag=None, date="2026-09-01"):
    return {"amount": Decimal(str(amount)), "transaction_type": kind,
            "excluded": excluded, "needs_review": review, "primary_tag": tag,
            "date": date}


class TransactionTypeTests(unittest.TestCase):
    def test_mixed_ledger_keeps_legacy_and_reviewed_totals_separate(self):
        summary = summarize_ledger([
            row("100", "expense"), row("40", "expense", date="2026-10-01"),
            row("-25", "refund"), row("-500", "income"),
            row("300", "transfer"), row("-300", "transfer"),
            row("-10", None, review=True), row("20", None),
            row("999", "expense", excluded=True), row("-50", "refund", excluded=True),
            row("-7", "expense"),
        ])
        self.assertEqual(summary["legacy_total"], -382.0)
        self.assertEqual((summary["gross_charges"], summary["refunds"], summary["net_spend"]),
                         (140.0, 25.0, 115.0))
        self.assertEqual(summary["income_credits"], 500.0)
        self.assertEqual(summary["transfer_net"], 0.0)
        self.assertEqual(summary["excluded_count"], 2)
        self.assertEqual(summary["excluded_signed_total"], 949.0)
        self.assertEqual(summary["type_review_count"], 2)
        self.assertEqual(summary["category_review_count"], 1)
        self.assertEqual(summary["sign_review_count"], 1)
        self.assertEqual(summary["review_count"], 3)
        self.assertEqual(summary["ambiguous_sign_signed_total"], -7.0)
        self.assertEqual(summary["untyped_signed_total"], 10.0)
        self.assertEqual(summary["by_month"][1]["net_spend"], 40.0)
        self.assertEqual(summary["legacy_total"], summary["net_spend"] - summary["income_credits"]
                         + summary["transfer_net"] + summary["untyped_signed_total"]
                         + summary["ambiguous_sign_signed_total"])

    def test_sign_warnings_do_not_rewrite_explicit_type(self):
        self.assertEqual(sign_issue("refund", Decimal("12")), "refund_debit")
        self.assertEqual(sign_issue("income", Decimal("12")), "income_debit")
        self.assertEqual(sign_issue("expense", Decimal("-12")), "expense_credit")
        self.assertIsNone(sign_issue("transfer", Decimal("-12")))
        self.assertEqual(sign_issue("transfer", Decimal("0")), "zero_amount")

    def test_preview_is_conservative_about_negative_and_excluded_rows(self):
        self.assertEqual(preview_candidate(row("-20", tag="Groceries")),
                         (None, "credit_could_be_refund_income_or_transfer"))
        self.assertEqual(preview_candidate(row("-20", tag="Reimbursements")),
                         (None, "refund_or_income_ambiguous"))
        self.assertEqual(preview_candidate(row("20", excluded=True, tag="Gifts")),
                         (None, "excluded_category_flow_unknown"))
        self.assertEqual(preview_candidate(row("20", tag="Transfers"))[0], "transfer")
        self.assertEqual(preview_candidate(row("20", tag="Groceries"))[0], "expense")


if __name__ == "__main__":
    unittest.main()

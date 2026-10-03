"""Type/API integration on an explicitly disposable local *_test PostgreSQL DB."""

import os
import unittest
from unittest.mock import patch

import psycopg2.extensions
from fastapi.testclient import TestClient
from test_tag_model import app


TEST_DSN = os.getenv("SPENDING_TEST_DATABASE_URL", "")


@unittest.skipUnless(TEST_DSN, "Set SPENDING_TEST_DATABASE_URL to a disposable local test DB")
class TransactionTypeApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        config = psycopg2.extensions.parse_dsn(TEST_DSN)
        host = config.get("host", "")
        if not config.get("dbname", "").endswith("_test") or not (
            host in ("localhost", "127.0.0.1") or host.startswith("/tmp/")
        ):
            raise RuntimeError("Integration tests require an explicitly named local _test database")
        cls.patches = [patch.object(app, "DATABASE_URL", TEST_DSN),
                       patch.object(app, "_pool", None), patch.object(app, "LOCAL_DEV", False)]
        for item in cls.patches:
            item.start()
        app.init_db()
        app.init_db()
        cls.client = TestClient(app.app)

    @classmethod
    def tearDownClass(cls):
        cls.client.close()
        app.app.dependency_overrides.clear()
        if app._pool:
            app._pool.closeall()
        for item in reversed(cls.patches):
            item.stop()

    def setUp(self):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE users, upload_jobs RESTART IDENTITY CASCADE")
                cur.execute("INSERT INTO users(email,name) VALUES('type@example.test','Type owner'),"
                            "('foreign@example.test','Foreign owner') RETURNING id")
                self.uid, self.foreign_uid = [item[0] for item in cur.fetchall()]
                cur.execute("INSERT INTO tags(user_id,name,excluded_from_spending) "
                            "VALUES(%s,'Purpose',FALSE),(%s,'Transfers',TRUE) RETURNING id",
                            (self.uid, self.uid))
                self.purpose_tag, self.transfer_tag = [item[0] for item in cur.fetchall()]
                cur.execute("INSERT INTO tags(user_id,name,group_tag_id) "
                            "VALUES(%s,'Wire',%s) RETURNING id", (self.uid, self.transfer_tag))
                self.wire_tag = cur.fetchone()[0]
        self.user = {"id": self.uid, "role": "owner", "is_owner": True,
                     "email": "type@example.test"}
        app.app.dependency_overrides[app.get_current_user] = lambda: self.user

    def insert(self, amount, *, kind=None, tag=None, owner=None, review=False, status="active"):
        with app.db() as conn:
            with conn.cursor() as cur:
                cur.execute("""INSERT INTO transactions(user_id,date,description,amount,source,dedup_key,
                               status,primary_tag_id,transaction_type,needs_review)
                               VALUES(%s,'2026-09-01','SYNTHETIC',%s,'Example',%s,%s,%s,%s,%s)
                               RETURNING id""",
                            (owner or self.uid, amount, f"test-{amount}-{kind}-{tag}", status,
                             tag, kind, review))
                return cur.fetchone()[0]

    def test_nullable_migration_and_analytics_preserve_legacy_stats(self):
        expense = self.insert(100, tag=self.purpose_tag)
        self.insert(-20, kind="refund", tag=self.purpose_tag)
        self.insert(-500, kind="income")
        self.insert(80, kind="transfer", tag=self.transfer_tag)
        self.insert(15, kind="transfer", tag=self.wire_tag)
        self.insert(999, kind="expense", status="deduped")
        self.insert(999, kind="expense", owner=self.foreign_uid)
        txs = self.client.get("/api/transactions").json()["transactions"]
        untyped = next(item for item in txs if item["id"] == expense)
        self.assertIsNone(untyped["transaction_type"])
        self.assertEqual(untyped["type_revision"], 0)
        stats = self.client.get("/api/stats").json()
        analytics = self.client.get("/api/analytics").json()
        self.assertEqual(stats["total"], -420.0)
        self.assertEqual(analytics["legacy_total"], stats["total"])
        self.assertEqual((analytics["gross_charges"], analytics["refunds"], analytics["net_spend"]),
                         (0.0, 20.0, -20.0))
        self.assertEqual((analytics["untyped_count"], analytics["excluded_count"]), (1, 2))
        self.assertEqual(self.client.get("/api/analytics?transaction_type=refund").json()["net_spend"], -20.0)
        self.assertEqual(self.client.get("/api/transactions?transaction_type=unreviewed").json()["total"], 1)
        self.assertEqual(self.client.get("/api/analytics?transaction_type=invalid").status_code, 422)

    def test_explicit_edit_clear_stale_revision_sign_warning_and_permissions(self):
        own = self.insert(-15)
        foreign = self.insert(10, owner=self.foreign_uid)
        payload = {"transaction_type": "expense", "expected_revision": 0}
        self.assertEqual(self.client.put(f"/api/transactions/{foreign}/type", json=payload).status_code, 404)
        self.user["role"] = "read"
        self.assertEqual(self.client.put(f"/api/transactions/{own}/type", json=payload).status_code, 403)
        self.user["role"] = "owner"
        self.assertEqual(self.client.put(f"/api/transactions/{own}/type",
                                         json={"transaction_type": "fee", "expected_revision": 0}).status_code, 422)
        first = self.client.put(f"/api/transactions/{own}/type", json=payload)
        self.assertEqual(first.status_code, 200, first.text)
        self.assertEqual(first.json()["type_sign_issue"], "expense_credit")
        self.assertEqual(first.json()["type_revision"], 1)
        self.assertEqual(self.client.put(f"/api/transactions/{own}/type", json=payload).status_code, 409)
        clear = self.client.put(f"/api/transactions/{own}/type",
                                json={"transaction_type": None, "expected_revision": 1})
        self.assertEqual((clear.status_code, clear.json()["type_revision"]), (200, 2))
        self.assertIsNone(clear.json()["transaction_type"])
        row = self.client.get("/api/transactions").json()["transactions"]
        self.assertEqual(next(item for item in row if item["id"] == own)["amount"], -15)

    def test_preview_is_read_only_capped_and_reports_ambiguity(self):
        self.insert(30, tag=self.purpose_tag)
        self.insert(-12, tag=self.purpose_tag, review=True)
        self.insert(90, tag=self.transfer_tag)
        self.insert(-4, kind="refund")
        result = self.client.get("/api/transaction-type-preview?limit=2").json()
        self.assertEqual(result["untyped_count"], 3)
        self.assertEqual(result["already_typed_count"], 1)
        self.assertEqual(result["candidate_counts"], {"transfer": 1, "expense": 1})
        self.assertEqual(result["ambiguous_reasons"], {"credit_could_be_refund_income_or_transfer": 1})
        self.assertEqual(len(result["sample"]), 2)
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["applied"], 0)
        self.assertEqual(self.client.get("/api/transaction-type-preview?limit=101").status_code, 422)
        self.assertEqual(self.client.get("/api/transactions?transaction_type=unreviewed").json()["total"], 3)


if __name__ == "__main__":
    unittest.main()

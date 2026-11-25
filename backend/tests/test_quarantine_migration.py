"""测试隔离区存量迁移：旧批次 → 逐行处置清单、新旧入口合一、断点恢复。

场景锚点：旧批次中两行在同一规则下隔离，第一行已有可信回炉确认，
第二行只有隔离原值；迁移后只允许再次领取第二行。
"""
from __future__ import annotations

import pytest

from app.lake_store import LakeStore, lake_store
from app.pipeline.chain_core import run_chain
from app.pipeline.chain_runner import get_chain_steps
from app.services.quarantine_zone import QuarantineZoneService

TABLE = "mig_tbl"
BATCH = "BATCH-OLD-1"
RULE = "QR-001"
RULE_NAME = "金额阈值超额"
MIGRATION_ID = f"MIG-{TABLE}-{BATCH}"
DISP1 = f"DISP-{TABLE}-{BATCH}-L001"
DISP2 = f"DISP-{TABLE}-{BATCH}-L002"

ROW1 = {"user_id": "u1", "action": "purchase", "amount": "12000", "partition_dt": "20261101"}
ROW2 = {"user_id": "u2", "action": "purchase", "amount": "15000", "partition_dt": "20261101"}


@pytest.fixture
def lake() -> LakeStore:
    return LakeStore()


@pytest.fixture
def svc(lake: LakeStore) -> QuarantineZoneService:
    return QuarantineZoneService(lake)


def seed_old_batch(lake: LakeStore, table: str = TABLE, batch: str = BATCH) -> str:
    """旧批两行同规则隔离；第一行有可信回炉确认，返回第一行凭证号。"""
    lake.store_batch(batch, table, "OLTP_DB", [dict(ROW1), dict(ROW2)],
                     "2026-11-01T00:00:00", "2026-11-01T23:59:59")
    lake.quarantine_rows(batch, RULE, RULE_NAME, [dict(ROW1), dict(ROW2)],
                         lake_table=table)
    legacy_row1 = lake.get_quarantine_line(table, batch, 1)
    receipt = lake.confirm_line_reprocess(table, batch, 1, dict(ROW1),
                                          via="legacy_entry", legacy_row=legacy_row1)
    lake.stamp_quarantine_line(table, batch, 1,
                               _retry_confirmed=True, _retry_via="legacy_entry",
                               _retry_voucher=receipt["voucher"])
    return receipt["voucher"]


class TestMigrationLedger:
    """存量记录迁移成逐行可回查的处置清单。"""

    def test_migration_builds_traceable_line_ledger(self, svc, lake):
        voucher1 = seed_old_batch(lake)
        result = svc.migrate_legacy_batch(TABLE, BATCH)
        assert result["status"] == "done"
        assert result["total_lines"] == 2

        lines = {l["line_no"]: l for l in svc.list_disposition_lines(TABLE, BATCH)}
        assert set(lines) == {1, 2}
        # 两行都记录了当时规则
        assert lines[1]["rule_id"] == RULE and lines[1]["rule_name"] == RULE_NAME
        assert lines[2]["rule_id"] == RULE and lines[2]["rule_name"] == RULE_NAME
        # 第一行已回炉并保留既有凭证；第二行只有隔离原值 → 待处置
        assert lines[1]["status"] == "已回炉"
        assert lines[1]["retry_voucher"] == voucher1
        assert lines[2]["status"] == "待处置"
        assert lines[2]["original_row"] == ROW2
        # 迁移确认可回查
        assert lines[1]["migration_id"] == MIGRATION_ID
        assert lines[2]["migration_id"] == MIGRATION_ID

    def test_migration_idempotent(self, svc, lake):
        seed_old_batch(lake)
        svc.migrate_legacy_batch(TABLE, BATCH)
        again = svc.migrate_legacy_batch(TABLE, BATCH)
        assert again["status"] == "done"
        assert len(svc.list_disposition_lines(TABLE, BATCH)) == 2

    def test_only_second_line_claimable_after_migration(self, svc, lake):
        seed_old_batch(lake)
        svc.migrate_legacy_batch(TABLE, BATCH)
        # 第一行已确认回炉，不允许再次领取
        line1, message1 = svc.claim_disposition_line(DISP1)
        assert line1 is None and "待处置" in message1
        # 第二行可以领取
        line2, message2 = svc.claim_disposition_line(DISP2, operator="oncall_a")
        assert line2 is not None and line2["status"] == "处置中"
        assert line2["claimed_by"] == "oncall_a"

    def test_first_line_ingest_and_rule_queryable(self, svc, lake):
        voucher1 = seed_old_batch(lake)
        svc.migrate_legacy_batch(TABLE, BATCH)
        # 既有入湖凭证仍可核验，且关联同一份旧记录
        check = svc.verify_lake_receipt(voucher1)
        assert check["valid"] and check["linked"]
        assert check["disposition"]["disposition_id"] == DISP1
        assert check["receipt"]["snapshot_id"]
        snap = lake.get_snapshot(TABLE, check["receipt"]["snapshot_id"])
        assert snap is not None and snap.status == "available"
        # 当时规则仍可回查
        assert check["disposition"]["rule_id"] == RULE


class TestPendingReview:
    """缺少可信回炉凭证的记录进入待核，保留旧内容。"""

    def test_untrusted_confirmation_goes_to_pending_review(self, svc, lake):
        lake.store_batch("BATCH-OLD-9", TABLE, "OLTP_DB", [dict(ROW1)],
                         "2026-11-03T00:00:00", "2026-11-03T23:59:59")
        lake.quarantine_rows("BATCH-OLD-9", RULE, RULE_NAME, [dict(ROW1)],
                             lake_table=TABLE)
        lake.stamp_quarantine_line(TABLE, "BATCH-OLD-9", 1,
                                   _retry_confirmed=True, _retry_via="legacy_entry",
                                   _retry_voucher="RCPT-FAKE-000")
        svc.migrate_legacy_batch(TABLE, "BATCH-OLD-9")
        line = svc.get_disposition_line(f"DISP-{TABLE}-BATCH-OLD-9-L001")
        assert line["status"] == "待核"
        # 保留旧内容：原值与旧记录原文（含不可信凭证）都在
        assert line["original_row"] == ROW1
        assert line["legacy_row"]["_retry_voucher"] == "RCPT-FAKE-000"
        # 待核行不能走回炉接口
        result, message = svc.retry_reprocess_line(line["disposition_id"])
        assert result is None and "待核" in message


class TestBatchScopedByLakeTable:
    """按湖表核对旧批号：另表同号的逐行记录继续归属原表。"""

    def test_same_batch_number_in_another_table_stays_separate(self, svc, lake):
        seed_old_batch(lake, table=TABLE, batch=BATCH)
        other_row = {"account_id": "", "amount": "88.00", "partition_dt": "20261101"}
        lake.store_batch(BATCH, "mig_other", "OLTP_PAY", [dict(other_row)],
                         "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        lake.quarantine_rows(BATCH, "QR-901", "账户为空", [dict(other_row)],
                             lake_table="mig_other")

        # 只迁移本表旧批：另表同号记录不受影响
        svc.migrate_legacy_batch(TABLE, BATCH)
        assert len(svc.list_disposition_lines(TABLE, BATCH)) == 2
        assert svc.list_disposition_lines("mig_other", BATCH) == []
        assert len(lake.list_quarantine(BATCH, "mig_other")) == 1

        # 另表自行迁移，逐行记录归属原表
        svc.migrate_legacy_batch("mig_other", BATCH)
        other_lines = svc.list_disposition_lines("mig_other", BATCH)
        assert len(other_lines) == 1
        assert other_lines[0]["lake_table"] == "mig_other"
        assert other_lines[0]["disposition_id"].startswith("DISP-mig_other-")
        assert other_lines[0]["rule_id"] == "QR-901"


class TestDualEntryMergeAndRecovery:
    """旧入口送第二行回炉 + 迁移启动；一路写到一半停机，恢复后两路一致。"""

    def test_crash_midway_then_recovery_views_consistent(self, svc, lake):
        seed_old_batch(lake)
        # 迁移写到第一行后停机
        crashed = svc.migrate_legacy_batch(TABLE, BATCH, crash_after_line=1)
        assert crashed["status"] == "crashed"
        assert svc.get_disposition_line(DISP1)["status"] == "已回炉"
        assert svc.get_disposition_line(DISP2) is None
        # 停机期间旧入口把第二行送回炉，自己也写到一半停机（确认落日志、清单未同步）
        legacy_result = svc.legacy_send_to_reprocess(
            TABLE, BATCH, 2, crash_before_ledger_sync=True)
        assert legacy_result["ok"] and legacy_result["crashed"]
        voucher2 = legacy_result["receipt"]["voucher"]

        # 恢复：断点续跑 + 两路核对
        recovered = svc.recover_migration(MIGRATION_ID)
        assert recovered["ok"]
        views = recovered["views"]
        assert views["consistent"], views["inconsistencies"]
        ledger = {l["line_no"]: l for l in views["ledger_view"]}
        # 两路都看到：第一行既有确认、第二行旧入口确认已合一进清单
        assert ledger[1]["status"] == "已回炉"
        assert ledger[2]["status"] == "已回炉"
        assert ledger[2]["retry_voucher"] == voucher2
        assert ledger[2]["confirmed_via"] == "legacy_entry"

    def test_dual_entry_confirmation_merges_single_ingest(self, svc, lake):
        seed_old_batch(lake)
        svc.migrate_legacy_batch(TABLE, BATCH)
        # 旧入口送第二行回炉
        old_result = svc.legacy_send_to_reprocess(TABLE, BATCH, 2)
        voucher2 = old_result["receipt"]["voucher"]
        # 新入口（回炉接口）对同一行再确认 → 合一到同一凭证
        retry_result, _ = svc.retry_reprocess_line(DISP2)
        assert retry_result["merged"]
        assert retry_result["receipt"]["voucher"] == voucher2
        # 反向再来一次旧入口也仍是同一凭证
        again = svc.legacy_send_to_reprocess(TABLE, BATCH, 2)
        assert again["receipt"]["voucher"] == voucher2
        # 后续入湖结果不能多一行
        rebatches = [b for b in lake.list_batches(TABLE)
                     if b.batch_id == f"rebatch-{TABLE}-{BATCH}-2"]
        assert len(rebatches) == 1 and rebatches[0].row_count == 1
        assert len(lake.list_receipts(TABLE, BATCH, 2)) == 1

    def test_retry_first_then_legacy_entry_still_single(self, svc, lake):
        seed_old_batch(lake)
        svc.migrate_legacy_batch(TABLE, BATCH)
        retry_result, _ = svc.retry_reprocess_line(DISP2)
        voucher2 = retry_result["receipt"]["voucher"]
        old_result = svc.legacy_send_to_reprocess(TABLE, BATCH, 2)
        assert old_result["receipt"]["voucher"] == voucher2
        assert len(lake.list_receipts(TABLE, BATCH, 2)) == 1


class TestLegacyEntryRestore:
    """还原旧入口后，从原隔离清单能追到同一行。"""

    def test_restore_legacy_entry_traces_same_line(self, svc, lake):
        seed_old_batch(lake)
        svc.migrate_legacy_batch(TABLE, BATCH)
        # 旧入口弹出第二行送回炉
        svc.legacy_send_to_reprocess(TABLE, BATCH, 2)
        assert lake.get_quarantine_line(TABLE, BATCH, 2) is None
        # 还原旧入口：原隔离清单两行都在，且都能追到同一份处置清单行
        view = svc.restore_legacy_entry_view(TABLE, BATCH)
        assert [row["_quarantine_line"] for row in view] == [1, 2]
        row2 = next(row for row in view if row["_quarantine_line"] == 2)
        assert row2["_disposition_id"] == DISP2
        traced = svc.get_disposition_line(row2["_disposition_id"])
        assert traced["original_row"] == ROW2
        assert traced["legacy_row"]["_quarantine_batch"] == BATCH
        # 还原是幂等的，不会重复放行
        again = svc.restore_legacy_entry_view(TABLE, BATCH)
        assert len(again) == 2


class TestRetryApiReceipt:
    """从回炉接口重试第二行，返回的入湖凭证可关联到同一份旧记录。"""

    def test_retry_returns_receipt_linked_to_same_old_record(self, svc, lake):
        seed_old_batch(lake)
        svc.migrate_legacy_batch(TABLE, BATCH)
        result, message = svc.retry_reprocess_line(DISP2)
        receipt = result["receipt"]
        assert receipt["voucher"]
        assert receipt["disposition_id"] == DISP2
        # 凭证可关联回同一份旧记录
        check = svc.verify_lake_receipt(receipt["voucher"])
        assert check["valid"] and check["linked"]
        assert check["disposition"]["disposition_id"] == DISP2
        assert check["legacy_row"]["_quarantine_batch"] == BATCH
        assert check["legacy_row"]["_quarantine_line"] == 2
        assert check["legacy_row"]["_quarantine_rule"] == RULE
        # 再重试一次仍返回同一凭证，不多入湖一行
        again, _ = svc.retry_reprocess_line(DISP2)
        assert again["merged"]
        assert again["receipt"]["voucher"] == receipt["voucher"]
        assert len(lake.list_receipts(TABLE, BATCH, 2)) == 1


class TestMigrationChain:
    """迁移流水线链路：旧批扫描 → 逐行迁移 → 两路核对。"""

    def test_migration_chain_runs(self):
        lake_store.store_batch("BATCH-CM-1", "chain_mig_tbl", "OLTP",
                               [{"k": "v1"}, {"k": "v2"}],
                               "2026-11-01T00:00:00", "2026-11-01T23:59:59")
        lake_store.quarantine_rows("BATCH-CM-1", "QR-1", "规则一",
                                   [{"k": "v1"}, {"k": "v2"}], lake_table="chain_mig_tbl")
        result = run_chain(
            "chain_quarantine_disposition_migration",
            "chain_mig_tbl",
            get_chain_steps("chain_quarantine_disposition_migration"),
        )
        assert result.success
        assert result.final_output["legacy_batch_count"] >= 1
        assert result.final_output["migrated_line_total"] == 2
        assert result.final_output["all_consistent"]


class TestLaunchReadiness:
    """上线时要能应对仍需处理的旧记录和已确认的回炉。"""

    def test_confirmed_before_migration_still_migrates(self, svc, lake):
        """旧入口在迁移前已弹出并确认第二行：迁移按确认日志补建清单行。"""
        seed_old_batch(lake)
        old_result = svc.legacy_send_to_reprocess(TABLE, BATCH, 2)
        voucher2 = old_result["receipt"]["voucher"]
        assert lake.get_quarantine_line(TABLE, BATCH, 2) is None
        # 上线扫描：已确认回炉（行已弹出）的批次仍在待迁移清单里
        assert BATCH in svc.scan_legacy_batches(TABLE)
        # 迁移启动：第一行来自旧清单，第二行来自确认日志
        result = svc.migrate_legacy_batch(TABLE, BATCH)
        assert result["status"] == "done" and result["total_lines"] == 2
        line2 = svc.get_disposition_line(DISP2)
        assert line2["status"] == "已回炉"
        assert line2["retry_voucher"] == voucher2
        assert line2["original_row"] == ROW2

    def test_unstamped_legacy_rows_adopted_by_batch_registry(self, svc, lake):
        """按湖表核对旧批号：归属不明的旧行按批次登记信息归入本表。"""
        lake.store_batch("BATCH-OLD-7", TABLE, "OLTP_DB", [dict(ROW1)],
                         "2026-11-02T00:00:00", "2026-11-02T23:59:59")
        # 旧数据没有湖表戳记（lake_table 缺省且批次尚未登记时入库的场景）
        lake._quarantine[("unknown", "BATCH-OLD-7")] = [{
            **ROW1, "_quarantine_batch": "BATCH-OLD-7", "_quarantine_table": "unknown",
            "_quarantine_line": 1, "_quarantine_rule": RULE,
            "_quarantine_rule_name": RULE_NAME, "_quarantine_time": 1.0,
        }]
        svc.migrate_legacy_batch(TABLE, "BATCH-OLD-7")
        line = svc.get_disposition_line(f"DISP-{TABLE}-BATCH-OLD-7-L001")
        assert line is not None and line["status"] == "待处置"
        assert line["lake_table"] == TABLE

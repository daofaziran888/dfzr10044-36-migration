"""湖表运行态示例数据：隔离区旧批次存量场景。

播种内容（供迁移流水线与回炉入口切换演练）：
- ods_user_behavior / BATCH-ODS-001：两行在同一规则 QR-001 下隔离，
  第一行已有可信的回炉确认（旧入口签发），第二行只有隔离原值
- ods_user_behavior / BATCH-ODS-003：一行声称已回炉但凭证不可信（迁移后应进待核）
- ods_payment / BATCH-ODS-001：另表同号，逐行记录继续归属原表

只在服务启动时播种一次；测试用例请自行构造夹具，勿依赖本模块。
"""
from __future__ import annotations

from app.lake_store import lake_store

LEGACY_TABLE = "ods_user_behavior"
LEGACY_BATCH = "BATCH-ODS-001"
LEGACY_RULE = "QR-001"
LEGACY_RULE_NAME = "金额阈值超额"
REVIEW_BATCH = "BATCH-ODS-003"
OTHER_TABLE = "ods_payment"
OTHER_RULE = "QR-901"
OTHER_RULE_NAME = "账户为空"

ROW1 = {"event_time": "2026-11-01", "user_id": "u107", "action": "purchase",
        "amount": "12000.00", "partition_dt": "20261101"}
ROW2 = {"event_time": "2026-11-01", "user_id": "u109", "action": "purchase",
        "amount": "15000.00", "partition_dt": "20261101"}
ROW_REVIEW = {"event_time": "2026-11-03", "user_id": "u220", "action": "purchase",
              "amount": "20000.00", "partition_dt": "20261103"}
ROW_OTHER_TABLE = {"pay_time": "2026-11-01", "account_id": "",
                   "amount": "88.00", "partition_dt": "20261101"}


def seed_quarantine_legacy_scenario() -> dict[str, object]:
    """播种隔离区旧批存量；已存在则幂等跳过。"""
    if lake_store.list_quarantine(LEGACY_BATCH, LEGACY_TABLE):
        return {"seeded": False, "reason": "legacy quarantine already present"}

    # 旧批 BATCH-ODS-001：两行在同一规则 QR-001 下隔离
    lake_store.store_batch(LEGACY_BATCH, LEGACY_TABLE, "OLTP_DB", [ROW1, ROW2],
                           "2026-11-01T00:00:00", "2026-11-01T23:59:59")
    lake_store.quarantine_rows(LEGACY_BATCH, LEGACY_RULE, LEGACY_RULE_NAME,
                               [dict(ROW1), dict(ROW2)], lake_table=LEGACY_TABLE)

    # 第一行：旧入口已送回炉并拿到可信入湖凭证（当时规则 QR-001）
    first = lake_store.get_quarantine_line(LEGACY_TABLE, LEGACY_BATCH, 1)
    cleaned_first = {k: v for k, v in first.items() if not str(k).startswith("_")}
    receipt = lake_store.confirm_line_reprocess(
        LEGACY_TABLE, LEGACY_BATCH, 1, cleaned_first,
        via="legacy_entry", legacy_row=first)
    lake_store.stamp_quarantine_line(
        LEGACY_TABLE, LEGACY_BATCH, 1,
        _retry_confirmed=True, _retry_via="legacy_entry",
        _retry_voucher=receipt["voucher"])

    # BATCH-ODS-003：一行声称已回炉但凭证不可信 → 迁移后应进入待核
    lake_store.store_batch(REVIEW_BATCH, LEGACY_TABLE, "OLTP_DB", [ROW_REVIEW],
                           "2026-11-03T00:00:00", "2026-11-03T23:59:59")
    lake_store.quarantine_rows(REVIEW_BATCH, LEGACY_RULE, LEGACY_RULE_NAME,
                               [dict(ROW_REVIEW)], lake_table=LEGACY_TABLE)
    lake_store.stamp_quarantine_line(
        LEGACY_TABLE, REVIEW_BATCH, 1,
        _retry_confirmed=True, _retry_via="legacy_entry",
        _retry_voucher="RCPT-UNTRUSTED-000")

    # 另表同号：ods_payment 也有 BATCH-ODS-001，逐行记录继续归属原表
    lake_store.store_batch(LEGACY_BATCH, OTHER_TABLE, "OLTP_PAY", [ROW_OTHER_TABLE],
                           "2026-11-01T00:00:00", "2026-11-01T23:59:59")
    lake_store.quarantine_rows(LEGACY_BATCH, OTHER_RULE, OTHER_RULE_NAME,
                               [dict(ROW_OTHER_TABLE)], lake_table=OTHER_TABLE)

    return {
        "seeded": True,
        "legacy_batch": LEGACY_BATCH,
        "first_voucher": receipt["voucher"],
    }

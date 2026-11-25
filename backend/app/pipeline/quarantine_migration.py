"""隔离区存量迁移流水线：旧批次处置清单化、断点续跑、新旧入口合一。

链路步骤：
1. legacy_batch_scan —— 按湖表核对旧批号，列出仍需处理的旧批存量
2. disposition_lines_migrate —— 逐行迁移到处置清单（检查点可断点续跑）
3. dual_entry_reconcile —— 合并新旧入口的有效确认，核对两路处置结果
"""
from __future__ import annotations

from app.pipeline.chain_core import ChainContext
from app.services.quarantine_zone import QuarantineZoneService


def _step_legacy_batch_scan(ctx: ChainContext) -> ChainContext:
    """扫描旧批存量：上线时要同时应对仍需处理的旧记录和已确认的回炉。"""
    svc = QuarantineZoneService()
    batches = svc.scan_legacy_batches(ctx.lake_table)
    ctx.output["legacy_batches"] = batches
    ctx.output["legacy_batch_count"] = len(batches)
    return ctx


def _step_disposition_lines_migrate(ctx: ChainContext) -> ChainContext:
    """逐行迁移：旧批次按规则字段保存的存量行迁入逐行处置清单。"""
    svc = QuarantineZoneService()
    results = [
        svc.migrate_legacy_batch(ctx.lake_table, batch_id)
        for batch_id in ctx.output.get("legacy_batches", [])
    ]
    ctx.output["migration_results"] = results
    ctx.output["migrated_line_total"] = sum(r.get("total_lines", 0) for r in results)
    return ctx


def _step_dual_entry_reconcile(ctx: ChainContext) -> ChainContext:
    """两路核对：新旧入口对同一行的有效确认合一，入湖不多一行。"""
    svc = QuarantineZoneService()
    reports = [
        svc.reconcile_batch_views(ctx.lake_table, batch_id)
        for batch_id in ctx.output.get("legacy_batches", [])
    ]
    ctx.output["reconcile_reports"] = reports
    ctx.output["all_consistent"] = all(r["consistent"] for r in reports)
    return ctx


MIGRATION_CHAIN_STEPS = [
    ("legacy_batch_scan", _step_legacy_batch_scan),
    ("disposition_lines_migrate", _step_disposition_lines_migrate),
    ("dual_entry_reconcile", _step_dual_entry_reconcile),
]

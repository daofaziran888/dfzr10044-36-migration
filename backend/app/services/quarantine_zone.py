"""隔离区领域服务：存放违规批次行、待处置/回炉/留存管理。

存量迁移能力：
- 把按旧批次+规则字段保存的隔离记录迁移成逐行可回查的处置清单
- 迁移流水线按行落检查点，写到一半停机可断点续跑，重复执行幂等
- 新旧回炉入口共用同一确认日志：同一行的有效确认合一，入湖不多一行
- 缺少可信回炉凭证的记录进入待核并保留旧内容
- 还原旧入口后，从原隔离清单仍能追到同一行
"""
from __future__ import annotations

import copy
import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import DISPOSITION_STATUSES, DispositionLine, LakeStore, lake_store


class QuarantineZoneService(BaseModuleService):
    MODULE = "quarantine_zone"
    ENTITY = "隔离记录"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["隔离编号", "所属湖表", "来源批次", "违规规则", "违规行数", "隔离目录", "处置结论", "隔离时间"]
    REQUIRED_FIELDS = ["隔离编号", "所属湖表", "来源批次"]
    STATUS_ORDER = ["已隔离", "待处置", "已回炉", "已留存"]
    ACTION_RULES = {"提交处置": "待处置", "回炉重入": "已回炉", "永久留存": "已留存"}
    NEGATIVE_ACTIONS = []

    def __init__(self, lake: LakeStore | None = None) -> None:
        # 默认使用全局湖表存储；测试可注入独立实例保证隔离
        self._lake = lake or lake_store

    # ============================================================
    # 既有能力：隔离、整批回炉、留存、汇总
    # ============================================================

    def isolate_violations(self, batch_id: str, rule_id: str, rule_name: str,
                           violation_rows: list[dict[str, Any]]) -> dict[str, Any]:
        """将违规数据行迁入隔离区。"""
        quarantined = self._lake.quarantine_rows(batch_id, rule_id, rule_name, violation_rows)
        return {
            "batch_id": batch_id,
            "rule_id": rule_id,
            "quarantine_count": len(quarantined),
            "status": "isolated",
        }

    def release_for_retry(self, batch_id: str) -> dict[str, Any]:
        """回炉重入：从隔离区取出数据行备重新入湖。"""
        rows = self._lake.release_quarantine(batch_id)
        return {
            "batch_id": batch_id,
            "released_count": len(rows),
            "released_rows": rows,
        }

    def dispose_permanently(self, batch_id: str) -> dict[str, Any]:
        """永久留存：标记处置结论为留存，不释放。"""
        saved = self._lake.list_quarantine(batch_id)
        return {
            "batch_id": batch_id,
            "disposition": "permanent_archive",
            "saved_count": len(saved),
        }

    def summary_by_lake_table(self, lake_table: str) -> dict[str, Any]:
        """汇总指定湖表的隔离区统计。"""
        all_quarantined = self._lake.list_quarantine()
        table_quarantined: dict[str, int] = {}
        for qr in all_quarantined:
            table = qr.get("_quarantine_table")
            if table is None:
                br = self._lake.get_batch(qr.get("_quarantine_batch", ""))
                table = br.lake_table if br else None
            if table == lake_table:
                rule = qr.get("_quarantine_rule", "unknown")
                table_quarantined[rule] = table_quarantined.get(rule, 0) + 1
        return {
            "lake_table": lake_table,
            "total_quarantined": sum(table_quarantined.values()),
            "by_rule": table_quarantined,
        }

    # ============================================================
    # 迁移流水线：旧批次存量 → 逐行处置清单
    # ============================================================

    @staticmethod
    def migration_id_for(lake_table: str, batch_id: str) -> str:
        return f"MIG-{lake_table}-{batch_id}"

    @staticmethod
    def disposition_id_for(lake_table: str, batch_id: str, line_no: int) -> str:
        return f"DISP-{lake_table}-{batch_id}-L{line_no:03d}"

    def scan_legacy_batches(self, lake_table: str) -> list[str]:
        """按湖表核对旧批号：列出该湖表下仍有旧隔离存量的批号。

        旧入口已弹出但确认日志尚在的批次也算待迁移存量，
        上线时既要应对仍需处理的旧记录，也要应对已确认的回炉。
        """
        batches = {
            row.get("_quarantine_batch", "")
            for row in self._lake.list_quarantine(lake_table=lake_table)
        }
        for confirmation in self._lake.list_line_confirmations(lake_table=lake_table):
            if self._lake.get_disposition(
                lake_table, confirmation["batch_id"], confirmation["line_no"]
            ) is None:
                batches.add(confirmation["batch_id"])
        batches.discard("")
        return sorted(batches)

    def migrate_legacy_batch(
        self,
        lake_table: str,
        batch_id: str,
        crash_after_line: int | None = None,
    ) -> dict[str, Any]:
        """迁移流水线（单批）：把旧批次按规则字段保存的存量行逐行迁入处置清单。

        - 已带可信回炉凭证的行 → 已回炉，保留既有入湖凭证与当时规则
        - 声称已回炉但凭证不可信的行 → 待核，保留旧内容
        - 只有隔离原值的行 → 待处置，可再次领取
        每迁移一行推进一次检查点；crash_after_line 用于演练写到一半停机。
        """
        migration_id = self.migration_id_for(lake_table, batch_id)
        checkpoint = self._lake.get_migration_checkpoint(migration_id)
        if checkpoint and checkpoint.get("status") == "done":
            return self._migration_summary(migration_id, lake_table, batch_id,
                                           status="done", note="该批已迁移，幂等跳过")

        self._adopt_unknown_table_rows(lake_table, batch_id)
        legacy_rows = self._lake.list_quarantine(batch_id, lake_table)
        legacy_by_line = {
            int(row.get("_quarantine_line", 0)): row for row in legacy_rows
        }
        confirmations = {
            c["line_no"]: c
            for c in self._lake.list_line_confirmations(lake_table, batch_id)
        }
        line_nos = sorted(set(legacy_by_line) | set(confirmations))

        if checkpoint is None:
            checkpoint = self._lake.start_migration_checkpoint(
                migration_id, lake_table, batch_id, total_lines=len(line_nos))

        migrated: list[int] = []
        for line_no in line_nos:
            if line_no < checkpoint["next_line"]:
                continue  # 断点之前的行已迁移，续跑跳过
            if self._lake.get_disposition(lake_table, batch_id, line_no) is None:
                line = self._build_disposition_line(
                    migration_id, lake_table, batch_id, line_no,
                    legacy_by_line.get(line_no), confirmations.get(line_no),
                )
                self._lake.upsert_disposition(line)
                migrated.append(line_no)
            disposition_id = self.disposition_id_for(lake_table, batch_id, line_no)
            if line_no in legacy_by_line:
                # 原隔离清单标注处置编号，旧入口视图可追到同一行
                self._lake.stamp_quarantine_line(
                    lake_table, batch_id, line_no,
                    _disposition_id=disposition_id, _migration_id=migration_id,
                )
            self._lake.advance_migration_checkpoint(migration_id, line_no + 1)
            if crash_after_line is not None and line_no == crash_after_line:
                self._lake.mark_migration_status(migration_id, "crashed")
                return self._migration_summary(
                    migration_id, lake_table, batch_id,
                    status="crashed", note=f"迁移写到第 {line_no} 行后停机")

        merged = self._merge_confirmations_into_ledger(lake_table, batch_id)
        self._lake.mark_migration_status(migration_id, "done")
        summary = self._migration_summary(migration_id, lake_table, batch_id, status="done")
        summary["merged_confirmations"] = merged
        return summary

    def migrate_all_legacy_batches(self, lake_table: str) -> dict[str, Any]:
        """切换回炉入口前，用迁移流水线处理该湖表的全部旧批次。"""
        results = [
            self.migrate_legacy_batch(lake_table, batch_id)
            for batch_id in self.scan_legacy_batches(lake_table)
        ]
        return {
            "lake_table": lake_table,
            "migrated_batches": len(results),
            "results": results,
        }

    def recover_migration(self, migration_id: str) -> dict[str, Any]:
        """停机恢复：按检查点断点续跑，并核对两路看到的处置结果。"""
        checkpoint = self._lake.get_migration_checkpoint(migration_id)
        if checkpoint is None:
            return {"ok": False, "error": f"迁移检查点 {migration_id} 不存在"}
        result = self.migrate_legacy_batch(checkpoint["lake_table"], checkpoint["batch_id"])
        views = self.reconcile_batch_views(checkpoint["lake_table"], checkpoint["batch_id"])
        return {"ok": True, "migration": result, "views": views}

    def reconcile_batch_views(self, lake_table: str, batch_id: str) -> dict[str, Any]:
        """核对两路处置结果：旧入口（原隔离清单）与新入口（处置清单）。

        先把共用确认日志合一进清单，再逐行核对两路视图。
        """
        merged = self._merge_confirmations_into_ledger(lake_table, batch_id)
        ledger = [line.to_dict() for line in self._lake.list_dispositions(lake_table, batch_id)]
        legacy_view = self._lake.list_quarantine(batch_id, lake_table)
        confirmations = self._lake.list_line_confirmations(lake_table, batch_id)

        inconsistencies: list[str] = []
        ledger_by_line = {line["line_no"]: line for line in ledger}
        for confirmation in confirmations:
            line = ledger_by_line.get(confirmation["line_no"])
            if line is None:
                inconsistencies.append(f"第{confirmation['line_no']}行确认缺少清单行")
            elif line["status"] != "已回炉" or line["retry_voucher"] != confirmation["voucher"]:
                inconsistencies.append(f"第{confirmation['line_no']}行的新旧入口确认未合一")
        for line in ledger:
            legacy_row = self._lake.get_quarantine_line(lake_table, batch_id, line["line_no"])
            if legacy_row is None and line["status"] != "已回炉":
                inconsistencies.append(f"第{line['line_no']}行旧清单缺失且未确认回炉")
        return {
            "consistent": not inconsistencies,
            "inconsistencies": inconsistencies,
            "merged_confirmations": merged,
            "ledger_view": ledger,
            "legacy_view": legacy_view,
        }

    # ============================================================
    # 处置清单：查询、领取、回炉接口
    # ============================================================

    def list_disposition_lines(
        self,
        lake_table: str | None = None,
        batch_id: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        """逐行处置清单：迁移确认与当前处置状态都可按行回查。"""
        return [
            line.to_dict()
            for line in self._lake.list_dispositions(lake_table, batch_id, status)
        ]

    def get_disposition_line(self, disposition_id: str) -> dict[str, Any] | None:
        line = self._lake.get_disposition_by_id(disposition_id)
        return line.to_dict() if line is not None else None

    def claim_disposition_line(
        self, disposition_id: str, operator: str = ""
    ) -> tuple[dict[str, Any] | None, str]:
        """领取处置行：只有待处置行允许再次领取。"""
        line = self._lake.get_disposition_by_id(disposition_id)
        if line is None:
            return None, f"处置行 {disposition_id} 不存在"
        if line.status != "待处置":
            return None, f"只有待处置行可以领取（当前状态：{line.status}）"
        line.status = "处置中"
        line.claimed_by = operator or "oncall"
        line.history.append({
            "event": "领取处置", "operator": line.claimed_by, "at": time.time(),
        })
        self._lake.upsert_disposition(line)
        return line.to_dict(), "已领取，可执行回炉"

    def retry_reprocess_line(
        self, disposition_id: str, via: str = "retry_api"
    ) -> tuple[dict[str, Any] | None, str]:
        """回炉接口（新入口）：重试一行，返回可关联同一份旧记录的入湖凭证。

        幂等：该行若已被任一入口有效确认，直接返回既有凭证，入湖不多一行。
        """
        line = self._lake.get_disposition_by_id(disposition_id)
        if line is None:
            return None, f"处置行 {disposition_id} 不存在"
        if line.status == "已回炉" and line.retry_voucher:
            receipt = self._lake.get_receipt(line.retry_voucher)
            return {
                "line": line.to_dict(), "receipt": receipt, "merged": True,
            }, "该行已确认回炉，返回既有入湖凭证"
        if line.status == "待核":
            return None, "该行缺少可信回炉凭证，仍在待核，需先人工核实"
        if line.status == "已留存":
            return None, "该行已永久留存，不能再回炉"

        receipt = self._lake.confirm_line_reprocess(
            line.lake_table, line.batch_id, line.line_no,
            line.original_row, via=via, legacy_row=line.legacy_row,
        )
        merged = bool(receipt.get("merged"))
        line.status = "已回炉"
        line.retry_voucher = receipt["voucher"]
        line.confirmed_via = via
        line.history.append({
            "event": "回炉接口确认", "via": via,
            "voucher": receipt["voucher"], "at": time.time(),
        })
        self._lake.upsert_disposition(line)
        self._lake.attach_receipt_disposition(receipt["voucher"], line.disposition_id)
        receipt = self._lake.get_receipt(receipt["voucher"])
        return {
            "line": line.to_dict(), "receipt": receipt, "merged": merged,
        }, "回炉确认完成，入湖凭证已关联同一份旧记录"

    # ============================================================
    # 旧入口：逐行送回炉、还原旧入口视图
    # ============================================================

    def legacy_send_to_reprocess(
        self,
        lake_table: str,
        batch_id: str,
        line_no: int,
        crash_before_ledger_sync: bool = False,
    ) -> dict[str, Any]:
        """旧入口：把隔离行送回去回炉。

        确认写入新旧入口共用的确认日志，天然合一；
        crash_before_ledger_sync 演练旧入口写到一半停机（确认已落日志、清单未同步）。
        """
        legacy_row = self._lake.pop_quarantine_line(lake_table, batch_id, line_no)
        confirmation = self._lake.get_line_confirmation(lake_table, batch_id, line_no)
        if legacy_row is None and confirmation is None:
            line = self._lake.get_disposition(lake_table, batch_id, line_no)
            return {
                "ok": False,
                "message": f"旧清单中不存在第 {line_no} 行",
                "disposition": line.to_dict() if line else None,
            }
        source = legacy_row or confirmation.get("legacy_row") or confirmation.get("row") or {}
        cleaned = {k: v for k, v in source.items() if not str(k).startswith("_")}
        receipt = self._lake.confirm_line_reprocess(
            lake_table, batch_id, line_no, cleaned,
            via="legacy_entry", legacy_row=source,
        )
        synced = False
        if not crash_before_ledger_sync:
            synced = self._sync_ledger_after_confirm(lake_table, batch_id, line_no, receipt)
        return {
            "ok": True,
            "receipt": receipt,
            "ledger_synced": synced,
            "crashed": crash_before_ledger_sync and not synced,
        }

    def restore_legacy_entry_view(
        self, lake_table: str, batch_id: str
    ) -> list[dict[str, Any]]:
        """还原旧入口：重建原隔离清单，已弹出的行按原行号放回，
        每行都带处置编号，从原隔离清单能追到同一行。"""
        current = {
            int(row.get("_quarantine_line", 0)): row
            for row in self._lake.list_quarantine(batch_id, lake_table)
        }
        for line in self._lake.list_dispositions(lake_table, batch_id):
            if line.line_no not in current and line.legacy_row:
                restored = copy.deepcopy(line.legacy_row)
                restored["_disposition_id"] = line.disposition_id
                restored["_restored"] = True
                self._lake.reinsert_quarantine_row(lake_table, batch_id, restored)
                current[line.line_no] = restored
        for confirmation in self._lake.list_line_confirmations(lake_table, batch_id):
            if confirmation["line_no"] not in current and confirmation.get("legacy_row"):
                restored = copy.deepcopy(confirmation["legacy_row"])
                line = self._lake.get_disposition(lake_table, batch_id, confirmation["line_no"])
                restored["_disposition_id"] = line.disposition_id if line else None
                restored["_restored"] = True
                self._lake.reinsert_quarantine_row(lake_table, batch_id, restored)
                current[confirmation["line_no"]] = restored
        return [current[key] for key in sorted(current)]

    def legacy_quarantine_view(self, lake_table: str, batch_id: str) -> list[dict[str, Any]]:
        """原隔离清单（旧入口当前视图），含迁移标注的处置编号。"""
        return self._lake.list_quarantine(batch_id, lake_table)

    # ============================================================
    # 入湖凭证核验
    # ============================================================

    def verify_lake_receipt(self, voucher: str) -> dict[str, Any]:
        """核验入湖凭证：必须能关联回同一份旧记录。"""
        receipt = self._lake.get_receipt(voucher)
        if receipt is None:
            return {"valid": False, "voucher": voucher}
        line = self._lake.get_disposition(
            receipt["lake_table"], receipt["batch_id"], receipt["line_no"])
        return {
            "valid": True,
            "receipt": receipt,
            "disposition": line.to_dict() if line else None,
            "legacy_row": copy.deepcopy(line.legacy_row) if line else receipt.get("legacy_ref"),
            "linked": line is not None and line.retry_voucher == voucher,
        }

    # ============================================================
    # 内部：迁移行构建、确认合一、断点汇总
    # ============================================================

    def _adopt_unknown_table_rows(self, lake_table: str, batch_id: str) -> None:
        """按湖表核对旧批号：把归属不明但批次登记在本表下的旧行归入本表。"""
        for row in list(self._lake.list_quarantine(batch_id, "unknown")):
            br = self._lake.get_batch(batch_id, lake_table)
            if br is None or br.lake_table != lake_table:
                continue
            popped = self._lake.pop_quarantine_line(
                "unknown", batch_id, int(row.get("_quarantine_line", 0)))
            if popped is not None:
                popped["_quarantine_table"] = lake_table
                self._lake.reinsert_quarantine_row(lake_table, batch_id, popped)

    def _build_disposition_line(
        self,
        migration_id: str,
        lake_table: str,
        batch_id: str,
        line_no: int,
        legacy_row: dict[str, Any] | None,
        confirmation: dict[str, Any] | None,
    ) -> DispositionLine:
        """把一条旧记录翻译成处置清单行，按可信凭证决定初始状态。"""
        disposition_id = self.disposition_id_for(lake_table, batch_id, line_no)
        now = time.time()
        source_row = legacy_row or (confirmation or {}).get("legacy_row") or {}
        original = {k: v for k, v in source_row.items() if not str(k).startswith("_")}
        voucher = (confirmation or {}).get("voucher") or source_row.get("_retry_voucher")
        claimed = bool(source_row.get("_retry_confirmed")) or bool(source_row.get("_retry_voucher"))
        history: list[dict[str, Any]] = [{
            "event": "迁移入清单", "migration_id": migration_id, "at": now,
        }]

        if voucher and self._lake.verify_receipt(voucher):
            status = "已回炉"
            retry_voucher = voucher
            confirmed_via = (
                (confirmation or {}).get("via")
                or source_row.get("_retry_via")
                or "legacy_entry"
            )
            self._lake.attach_receipt_disposition(voucher, disposition_id)
            history.append({"event": "可信回炉凭证确认", "voucher": voucher, "at": now})
        elif claimed or voucher:
            # 声称已回炉但凭证不可信：进入待核，保留旧内容
            status = "待核"
            retry_voucher = None
            confirmed_via = None
            history.append({
                "event": "缺少可信回炉凭证，转入待核",
                "dubious_voucher": voucher, "at": now,
            })
        else:
            status = "待处置"
            retry_voucher = None
            confirmed_via = None

        return DispositionLine(
            disposition_id=disposition_id,
            lake_table=lake_table,
            batch_id=batch_id,
            line_no=line_no,
            rule_id=str(source_row.get("_quarantine_rule", "") or ""),
            rule_name=str(source_row.get("_quarantine_rule_name", "") or ""),
            original_row=original,
            legacy_row=copy.deepcopy(source_row),
            quarantine_time=source_row.get("_quarantine_time"),
            status=status,
            retry_voucher=retry_voucher,
            confirmed_via=confirmed_via,
            migration_id=migration_id,
            migrated_at=now,
            history=history,
            updated_at=now,
        )

    def _merge_confirmations_into_ledger(
        self, lake_table: str, batch_id: str
    ) -> list[dict[str, Any]]:
        """新旧入口确认合一：共用确认日志里的有效确认并入处置清单。

        同一行只保留第一份有效确认；清单缺失的行按确认日志补建。
        """
        merged: list[dict[str, Any]] = []
        for confirmation in self._lake.list_line_confirmations(lake_table, batch_id):
            line_no = confirmation["line_no"]
            line = self._lake.get_disposition(lake_table, batch_id, line_no)
            if line is None:
                line = self._build_disposition_line(
                    self.migration_id_for(lake_table, batch_id),
                    lake_table, batch_id, line_no,
                    confirmation.get("legacy_row"), confirmation,
                )
                self._lake.upsert_disposition(line)
                merged.append({"line_no": line_no, "voucher": confirmation["voucher"],
                               "via": confirmation["via"], "rebuilt": True})
                continue
            if line.status == "已回炉":
                # 既有有效确认优先，重复确认归并到既有凭证
                self._lake.attach_receipt_disposition(
                    confirmation["voucher"], line.disposition_id)
                continue
            line.status = "已回炉"
            line.retry_voucher = confirmation["voucher"]
            line.confirmed_via = confirmation["via"]
            line.history.append({
                "event": "合并新旧入口确认", "voucher": confirmation["voucher"],
                "via": confirmation["via"], "at": time.time(),
            })
            self._lake.upsert_disposition(line)
            self._lake.attach_receipt_disposition(confirmation["voucher"], line.disposition_id)
            merged.append({"line_no": line_no, "voucher": confirmation["voucher"],
                           "via": confirmation["via"], "rebuilt": False})
        return merged

    def _sync_ledger_after_confirm(
        self, lake_table: str, batch_id: str, line_no: int, receipt: dict[str, Any]
    ) -> bool:
        """旧入口确认落日志后，同步到处置清单（若该行已迁移）。"""
        line = self._lake.get_disposition(lake_table, batch_id, line_no)
        if line is None:
            return False
        if line.status != "已回炉":
            line.status = "已回炉"
            line.retry_voucher = receipt["voucher"]
            line.confirmed_via = receipt["via"]
            line.history.append({
                "event": "旧入口确认回炉", "voucher": receipt["voucher"],
                "at": time.time(),
            })
            self._lake.upsert_disposition(line)
        self._lake.attach_receipt_disposition(receipt["voucher"], line.disposition_id)
        return True

    def _migration_summary(
        self,
        migration_id: str,
        lake_table: str,
        batch_id: str,
        status: str,
        note: str | None = None,
    ) -> dict[str, Any]:
        lines = self._lake.list_dispositions(lake_table, batch_id)
        summary: dict[str, Any] = {
            "migration_id": migration_id,
            "lake_table": lake_table,
            "batch_id": batch_id,
            "status": status,
            "total_lines": len(lines),
            "by_status": {
                name: sum(1 for line in lines if line.status == name)
                for name in DISPOSITION_STATUSES
            },
            "checkpoint": self._lake.get_migration_checkpoint(migration_id),
        }
        if note:
            summary["note"] = note
        return summary

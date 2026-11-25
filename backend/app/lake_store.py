"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按（湖表, 批次）隔离，批号按湖表核对
- 处置清单：隔离存量迁移后的逐行处置凭证，可按行回查
- 回炉确认日志：新旧入口共用的行级确认记录，同一行只入湖一次
- 迁移检查点：迁移流水线写到一半停机后可断点续跑
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 处置清单 -----------------------------

# 处置清单行状态：待处置 → 处置中 → 已回炉；待核/已留存为旁路终态
DISPOSITION_STATUSES = ("待处置", "处置中", "待核", "已回炉", "已留存")


@dataclass
class DispositionLine:
    """处置清单行：旧批次隔离存量迁移后的逐行处置凭证。

    - disposition_id 由（湖表, 旧批号, 行号）决定，迁移幂等
    - original_row 保留隔离原值；legacy_row 保留旧记录原文（含旧确认字段）
    - rule_id/rule_name 记录当时规则，规则后续变更不影响回查
    """
    disposition_id: str
    lake_table: str
    batch_id: str
    line_no: int
    rule_id: str
    rule_name: str
    original_row: dict[str, Any]
    legacy_row: dict[str, Any]
    quarantine_time: float | None
    status: str  # 待处置 | 处置中 | 待核 | 已回炉 | 已留存
    retry_voucher: str | None = None
    confirmed_via: str | None = None  # legacy_entry | retry_api | migration
    migration_id: str | None = None
    migrated_at: float | None = None
    claimed_by: str | None = None
    history: list[dict[str, Any]] = field(default_factory=list)
    updated_at: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "disposition_id": self.disposition_id,
            "lake_table": self.lake_table,
            "batch_id": self.batch_id,
            "line_no": self.line_no,
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "original_row": copy.deepcopy(self.original_row),
            "legacy_row": copy.deepcopy(self.legacy_row),
            "quarantine_time": self.quarantine_time,
            "status": self.status,
            "retry_voucher": self.retry_voucher,
            "confirmed_via": self.confirmed_via,
            "migration_id": self.migration_id,
            "migrated_at": self.migrated_at,
            "claimed_by": self.claimed_by,
            "history": copy.deepcopy(self.history),
            "updated_at": self.updated_at,
        }


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据（按 湖表+批号 归置，另表同号互不干扰）
    - quarantine: 隔离区数据（按 湖表+批号 归置）
    - dispositions: 处置清单（旧批隔离存量迁移后的逐行凭证）
    - ingest_receipts / line_confirmations: 回炉入湖凭证与新旧入口共用确认日志
    - migration_checkpoints: 迁移流水线断点
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：(lake_table, batch_id) -> BatchRecord
        self._batches: dict[tuple[str, str], BatchRecord] = {}

        # 隔离区：(lake_table, batch_id) -> list[dict]（被拦截的数据行）
        self._quarantine: dict[tuple[str, str], list[dict[str, Any]]] = {}

        # 处置清单：(lake_table, batch_id) -> {line_no -> DispositionLine}
        self._dispositions: dict[tuple[str, str], dict[int, DispositionLine]] = {}

        # 入湖凭证登记簿：voucher -> 凭证详情（回炉重入的落湖证明）
        self._ingest_receipts: dict[str, dict[str, Any]] = {}

        # 行级回炉确认日志（新旧入口共用，有效确认合一）：
        # (lake_table, batch_id, line_no) -> 确认记录
        self._line_confirmations: dict[tuple[str, str, int], dict[str, Any]] = {}

        # 行级再入湖索引：(lake_table, batch_id, line_no) -> voucher
        # 同一行只允许入湖一次，后续确认都归并到既有凭证
        self._reingest_index: dict[tuple[str, str, int], str] = {}

        # 迁移检查点：migration_id -> {next_line, status, ...}
        self._migration_checkpoints: dict[str, dict[str, Any]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        # 批号只在所属湖表内唯一：另表同号的批次各自归置
        self._batches[(lake_table, batch_id)] = br
        return br

    def get_batch(self, batch_id: str, lake_table: str | None = None) -> BatchRecord | None:
        """按批号查批次；给出湖表时精确匹配（按湖表核对旧批号）。"""
        if lake_table is not None:
            return self._batches.get((lake_table, batch_id))
        matches = [br for (tbl, bid), br in self._batches.items() if bid == batch_id]
        if not matches:
            return None
        return matches[0]

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [br for (tbl, _), br in self._batches.items() if tbl == lake_table]

    def update_batch_status(
        self, batch_id: str, status: str, snapshot_id: str | None = None,
        lake_table: str | None = None,
    ) -> BatchRecord | None:
        br = self.get_batch(batch_id, lake_table)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def _resolve_quarantine_table(self, batch_id: str, lake_table: str | None) -> str:
        """确定隔离行归属的湖表：显式参数优先，其次按批次登记信息核对。"""
        if lake_table:
            return lake_table
        br = self.get_batch(batch_id)
        return br.lake_table if br is not None else "unknown"

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]], lake_table: str | None = None,
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。

        每行盖 _quarantine_line 行号（批内稳定递增，弹出不重排），
        作为后续逐行处置清单与新旧入口确认合一的行标识。
        """
        table = self._resolve_quarantine_table(batch_id, lake_table)
        existing = self._quarantine.setdefault((table, batch_id), [])
        next_line = max(
            (int(r.get("_quarantine_line", 0)) for r in existing), default=0
        ) + 1
        quarantined = []
        for offset, row in enumerate(rows):
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_table"] = table
            qr["_quarantine_line"] = next_line + offset
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        existing.extend(quarantined)
        return quarantined

    def list_quarantine(
        self, batch_id: str | None = None, lake_table: str | None = None
    ) -> list[dict[str, Any]]:
        """列出隔离行；湖表+批号都给定时只取该组（按湖表核对旧批号）。"""
        if batch_id is not None and lake_table is not None:
            return list(self._quarantine.get((lake_table, batch_id), []))
        result: list[dict[str, Any]] = []
        for (table, bid), rows in self._quarantine.items():
            if batch_id is not None and bid != batch_id:
                continue
            if lake_table is not None and table != lake_table:
                continue
            result.extend(rows)
        return result

    def get_quarantine_line(
        self, lake_table: str, batch_id: str, line_no: int
    ) -> dict[str, Any] | None:
        for row in self._quarantine.get((lake_table, batch_id), []):
            if int(row.get("_quarantine_line", 0)) == line_no:
                return row
        return None

    def stamp_quarantine_line(
        self, lake_table: str, batch_id: str, line_no: int, **fields: Any
    ) -> dict[str, Any] | None:
        """给旧清单行补盖字段（如旧入口的回炉确认、迁移后的处置编号）。"""
        row = self.get_quarantine_line(lake_table, batch_id, line_no)
        if row is not None:
            row.update(fields)
        return row

    def pop_quarantine_line(
        self, lake_table: str, batch_id: str, line_no: int
    ) -> dict[str, Any] | None:
        """旧入口逐行取出隔离行；其余行号保持不变。"""
        rows = self._quarantine.get((lake_table, batch_id), [])
        for index, row in enumerate(rows):
            if int(row.get("_quarantine_line", 0)) == line_no:
                return rows.pop(index)
        return None

    def reinsert_quarantine_row(
        self, lake_table: str, batch_id: str, row: dict[str, Any]
    ) -> dict[str, Any] | None:
        """还原旧入口视图：把已弹出的旧记录按原行号放回原隔离清单。"""
        rows = self._quarantine.setdefault((lake_table, batch_id), [])
        line_no = int(row.get("_quarantine_line", 0))
        if any(int(r.get("_quarantine_line", 0)) == line_no for r in rows):
            return None
        restored = dict(row)
        rows.append(restored)
        return restored

    def release_quarantine(
        self, batch_id: str, lake_table: str | None = None
    ) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行（旧行为，按批整批弹出）。"""
        keys = [
            key for key in self._quarantine
            if key[1] == batch_id and (lake_table is None or key[0] == lake_table)
        ]
        cleaned: list[dict[str, Any]] = []
        for key in keys:
            for row in self._quarantine.pop(key, []):
                cleaned_row = {k: v for k, v in row.items() if not k.startswith("_quarantine_")}
                cleaned.append(cleaned_row)
        return cleaned

    # ---- 处置清单 ----

    def upsert_disposition(self, line: DispositionLine) -> DispositionLine:
        line.updated_at = time.time()
        self._dispositions.setdefault((line.lake_table, line.batch_id), {})[line.line_no] = line
        return line

    def get_disposition(
        self, lake_table: str, batch_id: str, line_no: int
    ) -> DispositionLine | None:
        return self._dispositions.get((lake_table, batch_id), {}).get(line_no)

    def get_disposition_by_id(self, disposition_id: str) -> DispositionLine | None:
        for lines in self._dispositions.values():
            for line in lines.values():
                if line.disposition_id == disposition_id:
                    return line
        return None

    def list_dispositions(
        self,
        lake_table: str | None = None,
        batch_id: str | None = None,
        status: str | None = None,
    ) -> list[DispositionLine]:
        result: list[DispositionLine] = []
        for (table, bid), lines in self._dispositions.items():
            if lake_table is not None and table != lake_table:
                continue
            if batch_id is not None and bid != batch_id:
                continue
            result.extend(lines.values())
        if status is not None:
            result = [line for line in result if line.status == status]
        return sorted(result, key=lambda line: (line.lake_table, line.batch_id, line.line_no))

    # ---- 回炉确认日志与入湖凭证 ----

    def confirm_line_reprocess(
        self,
        lake_table: str,
        batch_id: str,
        line_no: int,
        row: dict[str, Any],
        via: str,
        legacy_row: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """行级回炉确认（新旧入口共用漏斗）。

        同一（湖表, 旧批号, 行号）只入湖一次：重复确认归并到既有凭证，
        后续入湖结果不会多出一行。
        """
        key = (lake_table, batch_id, line_no)
        existing_voucher = self._reingest_index.get(key)
        if existing_voucher is not None:
            merged = dict(self._ingest_receipts[existing_voucher])
            merged["merged"] = True
            merged["duplicate_prevented"] = True
            return merged

        origin = self.get_batch(batch_id, lake_table)
        window_start = origin.window_start if origin else "2026-11-01T00:00:00"
        window_end = origin.window_end if origin else "2026-11-01T23:59:59"
        rebatch_id = f"rebatch-{lake_table}-{batch_id}-{line_no}"
        if self.get_batch(rebatch_id, lake_table) is None:
            self.store_batch(rebatch_id, lake_table, "QUARANTINE_RETRY",
                             [dict(row)], window_start, window_end)
        self.update_batch_status(rebatch_id, "ingesting", lake_table=lake_table)

        partition_key = str(row.get("partition_dt") or "20261101")
        pf = self.register_file(lake_table, partition_key, rows=1,
                                bytes_=max(1, len(str(row))))
        parent = self.get_snapshot_at_time(lake_table, time.time())
        file_ids = [f.file_id for f in self.list_files(lake_table)]
        snap = self.create_snapshot(
            lake_table, file_ids=file_ids,
            parent_snapshot=parent.snapshot_id if parent else None,
            added_rows=1, deleted_rows=0,
        )
        self.mark_snapshot_available(lake_table, snap.snapshot_id)
        self.update_batch_status(rebatch_id, "ingested", snap.snapshot_id,
                                 lake_table=lake_table)

        voucher = f"RCPT-{uuid.uuid4().hex[:12].upper()}"
        issued_at = time.time()
        receipt = {
            "voucher": voucher,
            "lake_table": lake_table,
            "batch_id": batch_id,
            "line_no": line_no,
            "disposition_id": None,  # 迁移到处置清单后回填，关联同一份旧记录
            "rebatch_id": rebatch_id,
            "snapshot_id": snap.snapshot_id,
            "file_id": pf.file_id,
            "row_hash": hashlib.md5(
                str(sorted((row or {}).items())).encode()
            ).hexdigest(),
            "via": via,
            "issued_at": issued_at,
            "legacy_ref": copy.deepcopy(legacy_row) if legacy_row else None,
            "merged": False,
        }
        self._ingest_receipts[voucher] = receipt
        self._reingest_index[key] = voucher
        self._line_confirmations[key] = {
            "lake_table": lake_table,
            "batch_id": batch_id,
            "line_no": line_no,
            "row": copy.deepcopy(row),
            "legacy_row": copy.deepcopy(legacy_row) if legacy_row else None,
            "voucher": voucher,
            "via": via,
            "confirmed_at": issued_at,
        }
        return dict(receipt)

    def get_receipt(self, voucher: str) -> dict[str, Any] | None:
        receipt = self._ingest_receipts.get(voucher)
        return dict(receipt) if receipt is not None else None

    def verify_receipt(self, voucher: str | None) -> bool:
        """可信回炉凭证核验：只有本引擎签发过的凭证可信。"""
        return bool(voucher) and voucher in self._ingest_receipts

    def attach_receipt_disposition(self, voucher: str, disposition_id: str) -> None:
        """把入湖凭证回填到处置清单行，凭证即可关联到同一份旧记录。"""
        receipt = self._ingest_receipts.get(voucher)
        if receipt is not None:
            receipt["disposition_id"] = disposition_id

    def list_receipts(
        self,
        lake_table: str | None = None,
        batch_id: str | None = None,
        line_no: int | None = None,
    ) -> list[dict[str, Any]]:
        result = []
        for receipt in self._ingest_receipts.values():
            if lake_table is not None and receipt["lake_table"] != lake_table:
                continue
            if batch_id is not None and receipt["batch_id"] != batch_id:
                continue
            if line_no is not None and receipt["line_no"] != line_no:
                continue
            result.append(dict(receipt))
        return result

    def get_line_confirmation(
        self, lake_table: str, batch_id: str, line_no: int
    ) -> dict[str, Any] | None:
        confirmation = self._line_confirmations.get((lake_table, batch_id, line_no))
        return dict(confirmation) if confirmation is not None else None

    def list_line_confirmations(
        self, lake_table: str | None = None, batch_id: str | None = None
    ) -> list[dict[str, Any]]:
        result = []
        for (table, bid, _), confirmation in self._line_confirmations.items():
            if lake_table is not None and table != lake_table:
                continue
            if batch_id is not None and bid != batch_id:
                continue
            result.append(dict(confirmation))
        return sorted(result, key=lambda c: (c["lake_table"], c["batch_id"], c["line_no"]))

    # ---- 迁移检查点 ----

    def start_migration_checkpoint(
        self, migration_id: str, lake_table: str, batch_id: str, total_lines: int
    ) -> dict[str, Any]:
        checkpoint = {
            "migration_id": migration_id,
            "lake_table": lake_table,
            "batch_id": batch_id,
            "total_lines": total_lines,
            "next_line": 1,
            "status": "running",  # running | crashed | done
            "started_at": time.time(),
            "updated_at": time.time(),
        }
        self._migration_checkpoints[migration_id] = checkpoint
        return dict(checkpoint)

    def get_migration_checkpoint(self, migration_id: str) -> dict[str, Any] | None:
        checkpoint = self._migration_checkpoints.get(migration_id)
        return dict(checkpoint) if checkpoint is not None else None

    def advance_migration_checkpoint(self, migration_id: str, next_line: int) -> None:
        checkpoint = self._migration_checkpoints.get(migration_id)
        if checkpoint is not None:
            checkpoint["next_line"] = next_line
            checkpoint["updated_at"] = time.time()

    def mark_migration_status(self, migration_id: str, status: str) -> None:
        checkpoint = self._migration_checkpoints.get(migration_id)
        if checkpoint is not None:
            checkpoint["status"] = status
            checkpoint["updated_at"] = time.time()

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()
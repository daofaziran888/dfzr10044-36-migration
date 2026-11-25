"""10条真实跨模块数据链路实现。

每条链路横跨3+模块，实现湖表治理全生命周期的核心业务流转。

链路清单：
1. chain_register_evolve: 湖表目录登记→结构演进版本创建→分区规划采纳
2. chain_ingest_quality_quarantine: 入湖批次→质量规则校验→违规入隔离区→可回炉
3. chain_watermark_batch_snapshot: 水位创建→批次落湖→快照生成→水位推进
4. chain_write_commit_conflict: 写入作业←并发提交←冲突重试←乐观锁
5. chain_partition_evolve_coexist: 分区演化启动→双分区并存→新旧分区消费→切换完成
6. chain_merge_recovery_compact: 小文件归并→检查点保存→中断恢复→压实确认
7. chain_stats_column_compact: 统计画像采样→列统计计算→倾斜检测→压实触发
8. chain_snapshot_timetravel_expire: 快照标记可用→时间旅行回查→过期策略清理
9. chain_tier_migration_watermark: 存储分层生效→冷热迁移→水位追踪→消费视图发布
10. chain_retention_audit_reclaim: 保留窗口→审计标记→到期回收→释放字节
11. chain_quarantine_disposition_migration: 旧批扫描→逐行迁移处置清单→新旧入口合一核对
"""
from __future__ import annotations

import hashlib
import time
import uuid
from typing import Any

from app.lake_store import lake_store, BatchRecord, LakeSnapshot, PartitionFile
from app.pipeline.chain_core import ChainContext, StepFunc
from app.pipeline.quarantine_migration import MIGRATION_CHAIN_STEPS


# ============================================================
# 链路1：湖表登记→结构演进→分区规划
# ============================================================

def _step_lake_catalog_register(ctx: ChainContext) -> ChainContext:
    """湖表目录登记：注册一张新湖表到目录，生成共生文件清单。"""
    lk = ctx.lake_table
    pf = lake_store.register_file(lk, "default", rows=0, bytes_=0)
    lake_store.create_snapshot(
        lk, file_ids=[pf.file_id],
        parent_snapshot=None, added_rows=0, deleted_rows=0,
    )
    ctx.output["catalog_entry"] = {
        "table": lk,
        "format": "PARQUET",
        "storage_path": f"/lake/{lk}",
        "registered_at": time.time(),
    }
    ctx.output["initial_file"] = pf
    return ctx


def _step_table_schema_version(ctx: ChainContext) -> ChainContext:
    """结构演进版本创建：为该湖表登记第一个列结构版本。"""
    lk = ctx.lake_table
    ctx.output["schema_version"] = {
        "version_id": f"v1-{lk}",
        "columns": [
            {"name": "event_time", "type": "TIMESTAMP", "nullable": False},
            {"name": "user_id", "type": "STRING", "nullable": False},
            {"name": "action", "type": "STRING", "nullable": False},
            {"name": "amount", "type": "DECIMAL(18,2)", "nullable": True},
            {"name": "partition_dt", "type": "STRING", "nullable": False},
        ],
        "compatibility": "BACKWARD_COMPATIBLE",
        "parent_version": None,
        "reason": "初始表结构登记",
    }
    return ctx


def _step_partition_plan_adopt(ctx: ChainContext) -> ChainContext:
    """分区规划采纳：基于结构创建分区方案并采纳。"""
    lk = ctx.lake_table
    ctx.output["partition_plan"] = {
        "plan_id": f"plan-{lk}",
        "partition_key": "partition_dt",
        "partition_granularity": "DAY",
        "expected_partitions": 365,
        "rows_per_partition_estimate": 500000,
        "hot_cold_decision": "HOT_7d_COLD_ARCHIVE",
        "status": "已采纳",
    }
    return ctx


# ============================================================
# 链路2：入湖批次→质量规则→隔离区处置
# ============================================================

def _step_ingest_batch_receive(ctx: ChainContext) -> ChainContext:
    """入湖批次接收：模拟一批数据行入湖。"""
    lk = ctx.lake_table
    batch_id = f"batch-{lk}-{uuid.uuid4().hex[:6]}"
    rows = [
        {"event_time": "2026-11-01", "user_id": "u1", "action": "login", "amount": "100.00", "partition_dt": "20261101"},
        {"event_time": "2026-11-01", "user_id": "u2", "action": "purchase", "amount": "250.00", "partition_dt": "20261101"},
        {"event_time": "2026-11-01", "user_id": "u3", "action": "login", "amount": None, "partition_dt": "20261101"},
        {"event_time": "2026-11-01", "user_id": "u1", "action": "login", "amount": "100.00", "partition_dt": "20261101"},  # 重复行
        {"event_time": "", "user_id": "", "action": "", "amount": "", "partition_dt": "20261101"},  # 空行
        {"event_time": "2026-11-01", "user_id": "u4", "action": "transfer", "amount": "500.00", "partition_dt": "20261101"},
    ]
    lake_store.store_batch(batch_id, lk, "OLTP_DB", rows, "2026-11-01T00:00:00", "2026-11-01T23:59:59")
    ctx.output["batch_id"] = batch_id
    ctx.output["row_count"] = len(rows)
    ctx.output["source_system"] = "OLTP_DB"
    return ctx


def _step_quality_rule_check(ctx: ChainContext) -> ChainContext:
    """质量规则校验：对批次做去重、非空校验、金额阈值判定。"""
    batch_id = ctx.output.get("batch_id")
    br = lake_store.get_batch(batch_id)
    if br is None:
        ctx.error_skip = True
        ctx.output["quality_error"] = "batch not found"
        return ctx

    rows = br.rows
    # 去重
    seen = set()
    deduped: list[dict[str, Any]] = []
    duplicates = 0
    for r in rows:
        key = hashlib.md5(str(sorted(r.items())).encode()).hexdigest()
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)
            deduped.append(r)

    # 非空校验：event_time/user_id/action 不为空
    valid: list[dict[str, Any]] = []
    empty_rows: list[dict[str, Any]] = []
    for r in deduped:
        if r.get("event_time") and r.get("user_id") and r.get("action"):
            valid.append(r)
        else:
            empty_rows.append(r)

    # 金额范围阈值：amount 在 0~10000
    beyond_threshold: list[dict[str, Any]] = []
    within_threshold: list[dict[str, Any]] = []
    for r in valid:
        try:
            amount_val = float(r.get("amount") or 0)
            if 0 <= amount_val <= 10000:
                within_threshold.append(r)
            else:
                beyond_threshold.append(r)
        except (ValueError, TypeError):
            beyond_threshold.append(r)

    ctx.output["quality"] = {
        "total_input": len(rows),
        "after_dedup": len(deduped),
        "duplicates_removed": duplicates,
        "empty_rows_removed": len(empty_rows),
        "beyond_threshold_removed": len(beyond_threshold),
        "clean_rows": len(within_threshold),
    }
    ctx.output["clean_data"] = within_threshold
    ctx.output["quarantine_data"] = empty_rows + beyond_threshold
    return ctx


def _step_quarantine_dispose(ctx: ChainContext) -> ChainContext:
    """隔离区处置：违规行进入隔离区，标记为待处置。"""
    batch_id = ctx.output.get("batch_id")
    quarantine_data = ctx.output.get("quarantine_data", [])
    if quarantine_data:
        lake_store.quarantine_rows(batch_id, "QR-AMOUNT", "金额阈值超额", quarantine_data)
    ctx.output["quarantine_count"] = len(quarantine_data)
    ctx.output["disposal"] = "isolated"
    return ctx


# ============================================================
# 链路3：水位推进→批次写入→快照生成
# ============================================================

def _step_watermark_setup(ctx: ChainContext) -> ChainContext:
    """水位初始化。"""
    lk = ctx.lake_table
    lake_store.set_watermark(lk, "event_time", "2026-10-31T00:00:00", "append")
    ctx.output["watermark"] = lake_store.get_watermark(lk)
    return ctx


def _step_batch_ingest(ctx: ChainContext) -> ChainContext:
    """批次数据入湖写入（去重后有效行）。"""
    lk = ctx.lake_table
    clean_data = ctx.output.get("clean_data", [])
    batch_id = f"batch-ingest-{lk}-{uuid.uuid4().hex[:6]}"
    lake_store.store_batch(batch_id, lk, "DIRECT", clean_data,
                           "2026-11-01T00:00:00", "2026-11-01T23:59:59")
    lake_store.update_batch_status(batch_id, "ingesting")
    ctx.output["ingest_batch_id"] = batch_id
    ctx.output["ingest_rows"] = len(clean_data)
    return ctx


def _step_snapshot_generate(ctx: ChainContext) -> ChainContext:
    """快照生成：为有效数据行创建分区文件并生成快照。"""
    lk = ctx.lake_table
    clean_data = ctx.output.get("clean_data", [])
    batch_id = ctx.output.get("ingest_batch_id")

    # 注册分区文件
    pf = lake_store.register_file(lk, "20261101", rows=len(clean_data),
                                   bytes_=len(clean_data) * 256)
    # 创建快照
    prev_snap = lake_store.get_snapshot_at_time(lk, time.time())
    snap = lake_store.create_snapshot(
        lk, file_ids=[pf.file_id],
        parent_snapshot=prev_snap.snapshot_id if prev_snap else None,
        added_rows=len(clean_data),
        deleted_rows=0,
    )
    lake_store.mark_snapshot_available(lk, snap.snapshot_id)
    lake_store.update_batch_status(batch_id, "ingested", snap.snapshot_id)
    ctx.output["snapshot"] = snap
    ctx.output["snapshot_id"] = snap.snapshot_id
    return ctx


def _step_watermark_advance(ctx: ChainContext) -> ChainContext:
    """水位推进：批次落湖后水位前行。"""
    lk = ctx.lake_table
    lake_store.advance_watermark(lk, "2026-11-01T23:59:59")
    lake_store.commit_watermark(lk)
    ctx.output["final_watermark"] = lake_store.get_watermark(lk)
    return ctx


# ============================================================
# 链路4：写入作业→并发提交→冲突重试
# ============================================================

def _step_write_job_lock_acquire(ctx: ChainContext) -> ChainContext:
    """写入作业获取全局写锁。"""
    lk = ctx.lake_table
    lock = lake_store.acquire_write_lock(lk)
    ctx.output["lock_acquired"] = True
    ctx.output["lock_object"] = lock
    return ctx


def _step_concurrent_commit_attempt(ctx: ChainContext) -> ChainContext:
    """模拟并发提交：如果锁被占用则冲突重试。"""
    lk = ctx.lake_table
    # 取锁后对现有文件快照做冲突检测
    files = lake_store.list_files(lk)
    snap_list = lake_store.list_snapshots(lk)
    # 基于文件数量和快照链做版本冲突检测
    has_conflict = False
    if len(files) > 0 and len(snap_list) > 0:
        latest_snap = snap_list[-1]
        registered_files = set(f.file_id for f in files)
        snap_files = set(latest_snap.file_ids)
        has_conflict = registered_files != snap_files

    ctx.output["commit_conflict"] = has_conflict
    ctx.output["file_count_before"] = len(files)
    return ctx


def _step_conflict_retry_resolve(ctx: ChainContext) -> ChainContext:
    """冲突重试：有冲突则重新取锁并写新快照。"""
    lk = ctx.lake_table
    has_conflict = ctx.output.get("commit_conflict", False)
    retried = False
    if has_conflict:
        # 重新注册一个合并文件然后创建新快照
        pf = lake_store.register_file(lk, "20261101", rows=50, bytes_=12800)
        snap = lake_store.create_snapshot(
            lk, file_ids=[pf.file_id], parent_snapshot=None,
            added_rows=50, deleted_rows=0,
        )
        lake_store.mark_snapshot_available(lk, snap.snapshot_id)
        retried = True
    ctx.output["conflict_retried"] = retried
    return ctx


def _step_write_lock_release(ctx: ChainContext) -> ChainContext:
    """释放写锁。"""
    lk = ctx.lake_table
    lake_store.release_write_lock(lk)
    ctx.output["lock_released"] = True
    return ctx


# ============================================================
# 链路5：分区演化→双分区并存→切换完成
# ============================================================

def _step_partition_evolve_start(ctx: ChainContext) -> ChainContext:
    """启动分区演化：将 partition_dt (DAY) 演化到 partition_hh (HOUR)。"""
    lk = ctx.lake_table
    # 为旧分区键和新分区键各注册文件
    old_pf = lake_store.register_file(lk, "20261101", rows=100, bytes_=25600)
    new_pf = lake_store.register_file(lk, "2026110100", rows=25, bytes_=6400)
    ctx.output["old_partition_files"] = [old_pf]
    ctx.output["new_partition_files"] = [new_pf]
    ctx.output["evolve_stage"] = "dual_coexist"
    return ctx


def _step_dual_coexist_read(ctx: ChainContext) -> ChainContext:
    """双分区并存期间读取：查询同时命中新旧分区文件。"""
    lk = ctx.lake_table
    all_files = lake_store.list_files(lk)
    old_files = [f for f in all_files if len(f.partition_key) == 8]
    new_files = [f for f in all_files if len(f.partition_key) == 10]
    ctx.output["old_file_count"] = len(old_files)
    ctx.output["new_file_count"] = len(new_files)
    ctx.output["total_in_coexist"] = len(all_files)
    return ctx


def _step_evolve_switch_complete(ctx: ChainContext) -> ChainContext:
    """完成切换：归档旧分区文件，快照绑定新分区。"""
    lk = ctx.lake_table
    old_files = ctx.output.get("old_partition_files", [])
    old_ids = [f.file_id for f in old_files]
    lake_store.remove_files(lk, old_ids)
    # 为新状态创建快照
    pf = lake_store.register_file(lk, "2026110100", rows=75, bytes_=19200)
    snap = lake_store.create_snapshot(
        lk, file_ids=[pf.file_id], added_rows=75, deleted_rows=0)
    lake_store.mark_snapshot_available(lk, snap.snapshot_id)
    ctx.output["switch_snapshot"] = snap.snapshot_id
    return ctx


# ============================================================
# 链路6：小文件归并→检查点恢复→压实确认
# ============================================================

def _step_small_files_register(ctx: ChainContext) -> ChainContext:
    """注册多个碎片化小文件。"""
    lk = ctx.lake_table
    files: list[PartitionFile] = []
    for i in range(6):
        pf = lake_store.register_file(lk, "20261101", rows=5, bytes_=1280)
        files.append(pf)
    ctx.output["small_files"] = files
    ctx.output["small_file_count"] = 6
    return ctx


def _step_merge_with_checkpoint(ctx: ChainContext) -> ChainContext:
    """归并小文件推导出目标文件，保存检查点。"""
    lk = ctx.lake_table
    small_ids = [f.file_id for f in ctx.output.get("small_files", [])]
    merge_id = f"merge-{lk}-{uuid.uuid4().hex[:6]}"
    # 推导归并目标：6个小文件归并为2个目标文件
    target_files: list[dict[str, Any]] = []
    for i in range(2):
        target_files.append({
            "rows": 15,
            "bytes": 3840,
            "source_partition": "20261101",
            "segment": i,
        })
    lake_store.save_merge_checkpoint(merge_id, small_ids)
    ctx.output["merge_id"] = merge_id
    ctx.output["target_count"] = len(target_files)
    ctx.output["checkpoint_saved"] = True
    return ctx


def _step_merge_interrupt_recovery(ctx: ChainContext) -> ChainContext:
    """模拟归并中断恢复：检查点存在，加载partial文件清单继续。"""
    lk = ctx.lake_table
    merge_id = ctx.output.get("merge_id")
    checkpoint = lake_store.get_merge_checkpoint(merge_id)
    if checkpoint is None:
        ctx.error_skip = True
        return ctx
    old_ids = checkpoint["partial_file_ids"]
    # 注册归并后新文件
    for i in range(2):
        lake_store.register_file(lk, "20261101", rows=15, bytes_=3840)
    ctx.output["recovered_old_count"] = len(old_ids)
    ctx.output["recovery_source"] = "checkpoint"
    return ctx


def _step_compact_confirm(ctx: ChainContext) -> ChainContext:
    """压实确认：归并新文件写入后压实任务标记完成。"""
    lk = ctx.lake_table
    # 使用原子替换删除小文件、保留归并后大文件
    all_files = lake_store.list_files(lk, "20261101")
    small_to_remove = [f.file_id for f in all_files if f.file_bytes <= 1500]
    # 原子替换（只保留大文件）
    remaining = [f for f in all_files if f.file_bytes > 1500]
    lake_store.replace_file_list(lk, [f.file_id for f in all_files], remaining)
    ctx.output["files_before_compact"] = len(all_files)
    ctx.output["files_after_compact"] = len(remaining)
    ctx.output["compact_success"] = len(remaining) < len(all_files)
    return ctx


# ============================================================
# 链路7：统计画像→列统计→压实触发
# ============================================================

def _step_stats_profile_sample(ctx: ChainContext) -> ChainContext:
    """统计画像采样：给湖表达成行数/字节估算与倾斜度判定。"""
    lk = ctx.lake_table
    files = lake_store.list_files(lk)
    total_rows = sum(f.file_rows for f in files)
    total_bytes = sum(f.file_bytes for f in files)
    # 按分区计数：统计各分区行数分布
    partition_counts: dict[str, int] = {}
    for f in files:
        partition_counts[f.partition_key] = partition_counts.get(f.partition_key, 0) + f.file_rows
    # 倾斜度判定：最大分区/总行数 > 0.5 视为倾斜
    max_partition_rows = max(partition_counts.values()) if partition_counts else 0
    skew = max_partition_rows / total_rows if total_rows > 0 else 0
    ctx.output["stats_profile"] = {
        "total_rows_estimate": total_rows,
        "total_bytes_estimate": total_bytes,
        "partition_skew": skew,
        "is_skewed": skew > 0.5,
        "sample_rate": 0.1,
    }
    return ctx


def _step_column_stat_compute(ctx: ChainContext) -> ChainContext:
    """列统计计算：对指定列做空值比例与去重基数计算。"""
    lk = ctx.lake_table
    # 从批次数据获取要统计的列
    batches = lake_store.list_batches(lk)
    all_rows: list[dict[str, Any]] = []
    for br in batches:
        all_rows.extend(br.rows)

    # 计算 amount 列的空值比例
    amount_values = [r.get("amount") for r in all_rows if "amount" in r]
    null_count = sum(1 for v in amount_values if v is None or v == "")
    total_count = len(amount_values)
    ctx.output["column_stat"] = {
        "column": "amount",
        "null_ratio": null_count / total_count if total_count > 0 else 0,
        "distinct_count": len(set(amount_values)),
        "min_value": min((v for v in amount_values if v), default=None),
        "max_value": max((v for v in amount_values if v), default=None),
    }
    return ctx


def _step_compact_trigger_by_stats(ctx: ChainContext) -> ChainContext:
    """基于统计画像触发压实：若倾斜度高则触发压实。"""
    lk = ctx.lake_table
    profile = ctx.output.get("stats_profile", {})
    is_skewed = profile.get("is_skewed", False)
    if is_skewed:
        # 倾斜→触发压实重均衡
        ctx.output["compact_triggered"] = True
        ctx.output["compact_reason"] = "partition_skew_threshold"
    else:
        ctx.output["compact_triggered"] = False
    return ctx


# ============================================================
# 链路8：快照管理→时间旅行读取→过期策略清理
# ============================================================

def _step_snapshot_mark_available(ctx: ChainContext) -> ChainContext:
    """快照标记可用：确保快照状态为 available。"""
    lk = ctx.lake_table
    snaps = lake_store.list_snapshots(lk)
    for snap in snaps:
        if snap.status == "generating":
            lake_store.mark_snapshot_available(lk, snap.snapshot_id)
    available = [s for s in lake_store.list_snapshots(lk) if s.status == "available"]
    ctx.output["available_snapshots"] = len(available)
    ctx.output["snapshot_ids"] = [s.snapshot_id for s in available]
    return ctx


def _step_time_travel_read(ctx: ChainContext) -> ChainContext:
    """时间旅行读取：按历史时间点获取最近可用快照。"""
    lk = ctx.lake_table
    # 使用一个过去的 timestamp（减 10 秒）做回查
    past_time = time.time()
    snap = lake_store.get_snapshot_at_time(lk, past_time)
    if snap:
        files = [lake_store.get_file(lk, fid) for fid in snap.file_ids]
        total_rows = sum(f.file_rows for f in files if f)
        ctx.output["timetravel_snapshot"] = snap.snapshot_id
        ctx.output["timetravel_rows"] = total_rows
        ctx.output["timetravel_parent"] = snap.parent_snapshot
    else:
        ctx.output["timetravel_snapshot"] = None
        ctx.output["timetravel_rows"] = 0
    return ctx


def _step_expire_policy_clean(ctx: ChainContext) -> ChainContext:
    """过期策略清理：将最老快照标记过期并释放空间。"""
    lk = ctx.lake_table
    snaps = lake_store.list_snapshots(lk)
    available = [s for s in snaps if s.status == "available"]
    if len(available) > 2:
        oldest = available[0]
        lake_store.mark_snapshot_expired(lk, oldest.snapshot_id)
        ctx.output["expired_snapshot"] = oldest.snapshot_id
        ctx.output["keep_count"] = len(available) - 1
    else:
        ctx.output["expired_snapshot"] = None
        ctx.output["keep_count"] = len(available)
    return ctx


# ============================================================
# 链路9：存储分层→冷热迁移→消费视图发布
# ============================================================

def _step_storage_tier_hot_cold(ctx: ChainContext) -> ChainContext:
    """存储分层判定：热层 7 天，超期进入冷层。"""
    lk = ctx.lake_table
    ctx.output["tier_decision"] = {
        "hot_days": 7,
        "cold_strategy": "ARCHIVE_SNAPPY",
        "migration_window_start": "2026-11-22T00:00:00",
        "migration_window_end": "2026-11-23T00:00:00",
    }
    return ctx


def _step_migration_execute(ctx: ChainContext) -> ChainContext:
    """冷热迁移执行：超期分区从热层迁到冷层。"""
    lk = ctx.lake_table
    files = lake_store.list_files(lk)
    for f in files:
        # 模拟压缩体积变化
        f.file_bytes = max(1, f.file_bytes // 3)
    ctx.output["migrated_file_count"] = len(files)
    ctx.output["post_migration_bytes"] = sum(f.file_bytes for f in files)
    return ctx


def _step_publish_view_refresh(ctx: ChainContext) -> ChainContext:
    """消费视图发布：绑定最新可用快照，刷新下游视图。"""
    lk = ctx.lake_table
    snaps = lake_store.list_snapshots(lk)
    available = [s for s in snaps if s.status == "available"]
    if available:
        latest = available[-1]
        ctx.output["view_snapshot"] = latest.snapshot_id
        ctx.output["view_columns"] = ["event_time", "user_id", "action", "amount"]
        ctx.output["downstream_consumer"] = "ADHOC_QUERY"
    else:
        ctx.output["view_snapshot"] = None
        ctx.output["view_columns"] = []
        ctx.output["downstream_consumer"] = "ADHOC_QUERY"
    return ctx


# ============================================================
# 链路10：保留窗口→审计标记→到期回收
# ============================================================

def _step_retention_window_define(ctx: ChainContext) -> ChainContext:
    """保留窗口定义：按天粒度保留90天。"""
    lk = ctx.lake_table
    ctx.output["retention"] = {
        "granularity": "DAY",
        "retention_days": 90,
        "audit_tagged": True,
        "expire_partitions": 3,
    }
    return ctx


def _step_audit_mark_window(ctx: ChainContext) -> ChainContext:
    """审计标记窗口：标记保留窗口受审计稽核约束。"""
    lk = ctx.lake_table
    retention = ctx.output.get("retention", {})
    if retention.get("audit_tagged"):
        ctx.output["audit_mark"] = {
            "audit_required": True,
            "audit_reason": "合规要求保留历史操作轨迹",
            "block_reclaim_until_audit": True,
        }
    return ctx


def _step_reclaim_expired(ctx: ChainContext) -> ChainContext:
    """到期回收：解除审计标记后回收过期分区。"""
    lk = ctx.lake_table
    audit = ctx.output.get("audit_mark", {})
    if audit.get("block_reclaim_until_audit"):
        # 模拟先解除审计标记
        ctx.output["audit_cleared"] = True
    # 回收过期分区文件
    expired_files = lake_store.list_files(lk, "20261101")
    expired_ids = [f.file_id for f in expired_files if f.file_bytes < 2000]
    removed = lake_store.remove_files(lk, expired_ids)
    ctx.output["reclaimed_file_count"] = removed
    ctx.output["reclaimed_bytes"] = sum(f.file_bytes for f in expired_files if f.file_id in expired_ids)
    return ctx


# ============================================================
# 链路段组合接口
# ============================================================

CHAIN_REGISTRY: dict[str, list[tuple[str, StepFunc]]] = {
    "chain_register_evolve": [
        ("lake_catalog_register", _step_lake_catalog_register),
        ("table_schema_version", _step_table_schema_version),
        ("partition_plan_adopt", _step_partition_plan_adopt),
    ],
    "chain_ingest_quality_quarantine": [
        ("ingest_batch_receive", _step_ingest_batch_receive),
        ("quality_rule_check", _step_quality_rule_check),
        ("quarantine_dispose", _step_quarantine_dispose),
    ],
    "chain_watermark_batch_snapshot": [
        ("watermark_setup", _step_watermark_setup),
        ("batch_ingest", _step_batch_ingest),
        ("snapshot_generate", _step_snapshot_generate),
        ("watermark_advance", _step_watermark_advance),
    ],
    "chain_write_commit_conflict": [
        ("write_lock_acquire", _step_write_job_lock_acquire),
        ("concurrent_commit_attempt", _step_concurrent_commit_attempt),
        ("conflict_retry_resolve", _step_conflict_retry_resolve),
        ("write_lock_release", _step_write_lock_release),
    ],
    "chain_partition_evolve_coexist": [
        ("partition_evolve_start", _step_partition_evolve_start),
        ("dual_coexist_read", _step_dual_coexist_read),
        ("evolve_switch_complete", _step_evolve_switch_complete),
    ],
    "chain_merge_recovery_compact": [
        ("small_files_register", _step_small_files_register),
        ("merge_with_checkpoint", _step_merge_with_checkpoint),
        ("merge_interrupt_recovery", _step_merge_interrupt_recovery),
        ("compact_confirm", _step_compact_confirm),
    ],
    "chain_stats_column_compact": [
        ("stats_profile_sample", _step_stats_profile_sample),
        ("column_stat_compute", _step_column_stat_compute),
        ("compact_trigger_by_stats", _step_compact_trigger_by_stats),
    ],
    "chain_snapshot_timetravel_expire": [
        ("snapshot_mark_available", _step_snapshot_mark_available),
        ("time_travel_read", _step_time_travel_read),
        ("expire_policy_clean", _step_expire_policy_clean),
    ],
    "chain_tier_migration_watermark": [
        ("storage_tier_hot_cold", _step_storage_tier_hot_cold),
        ("migration_execute", _step_migration_execute),
        ("publish_view_refresh", _step_publish_view_refresh),
    ],
    "chain_retention_audit_reclaim": [
        ("retention_window_define", _step_retention_window_define),
        ("audit_mark_window", _step_audit_mark_window),
        ("reclaim_expired", _step_reclaim_expired),
    ],
    "chain_quarantine_disposition_migration": MIGRATION_CHAIN_STEPS,
}


def get_chain_steps(name: str) -> list[tuple[str, StepFunc]]:
    """获取指定链路步骤列表；不存在则返回空。"""
    return CHAIN_REGISTRY.get(name, [])
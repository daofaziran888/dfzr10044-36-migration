"""测试10条跨模块集成链路的串联行为。

覆盖真实场景：登记→演进→入湖→质量→隔离→水位→快照→时间旅行→过期→归并→压实→分层→视图等。
"""
from __future__ import annotations

import pytest

from app.pipeline.chain_core import run_chain, ChainResult
from app.pipeline.chain_runner import get_chain_steps, CHAIN_REGISTRY
from app.lake_store import lake_store


class TestChainRegistry:
    def test_all_11_chains_registered(self):
        assert len(CHAIN_REGISTRY) == 11
        for name in CHAIN_REGISTRY:
            assert name.startswith("chain_")
            assert len(CHAIN_REGISTRY[name]) >= 3

    def test_get_chain_steps_non_existent(self):
        assert get_chain_steps("nonexistent") == []


class TestChainRegisterEvolve:
    """测试链路1：湖表目录登记→结构演进版本创建→分区规划采纳。"""
    def test_register_evolve_full(self):
        result = run_chain(
            "chain_register_evolve",
            "test_chain1",
            get_chain_steps("chain_register_evolve"),
        )
        assert result.success
        assert result.error_count == 0
        assert "catalog_entry" in result.final_output
        assert "schema_version" in result.final_output
        assert "partition_plan" in result.final_output
        plan = result.final_output["partition_plan"]
        assert plan["status"] == "已采纳"
        # 验证湖表已登记到存储引擎
        files = lake_store.list_files("test_chain1")
        assert len(files) >= 1


class TestChainIngestQualityQuarantine:
    """测试链路2：入湖批次→质量规则→违规入隔离区→可回炉。"""
    def test_ingest_quality_quarantine_full(self):
        result = run_chain(
            "chain_ingest_quality_quarantine",
            "test_chain2",
            get_chain_steps("chain_ingest_quality_quarantine"),
        )
        assert result.success
        quality = result.final_output.get("quality", {})
        assert "total_input" in quality
        assert "duplicates_removed" in quality
        assert "empty_rows_removed" in quality
        # 验证隔离区有数据
        quarantined = lake_store.list_quarantine()
        assert len(quarantined) >= 1
        # 去重计数至少为1
        assert quality["duplicates_removed"] >= 1


class TestChainWatermarkBatchSnapshot:
    """测试链路3：水位创建→批次入湖→快照生成→水位推进。"""
    def test_watermark_batch_snapshot_full(self):
        # 先跑入湖和隔离获 clean_data
        from app.pipeline.chain_runner import _step_ingest_batch_receive, _step_quality_rule_check, _step_quarantine_dispose
        from app.pipeline.chain_core import ChainContext
        ctx = ChainContext(lake_table="test_chain3")
        ctx = _step_ingest_batch_receive(ctx)
        ctx = _step_quality_rule_check(ctx)
        ctx = _step_quarantine_dispose(ctx)

        # 跑水位+批次+快照链路
        result = run_chain(
            "chain_watermark_batch_snapshot",
            "test_chain3",
            get_chain_steps("chain_watermark_batch_snapshot"),
        )
        assert result.success
        assert "snapshot_id" in result.final_output
        # 验证水位已推进
        wm_key = result.final_output.get("final_watermark")
        # 检查存储引擎
        wm = lake_store.get_watermark("test_chain3")
        assert wm is not None
        assert wm.committed_value is not None


class TestChainWriteCommitConflict:
    """测试链路4：写入作业←并发提交←冲突重试←乐观锁。"""
    def test_write_conflict_chain(self):
        result = run_chain(
            "chain_write_commit_conflict",
            "test_chain4",
            get_chain_steps("chain_write_commit_conflict"),
        )
        assert result.success
        assert "lock_acquired" in result.final_output
        assert result.final_output["lock_acquired"]


class TestChainPartitionEvolveCoexist:
    """测试链路5：分区演化启动→双分区并存→新旧分区消费→切换完成。"""
    def test_partition_evolve_full(self):
        result = run_chain(
            "chain_partition_evolve_coexist",
            "test_chain5",
            get_chain_steps("chain_partition_evolve_coexist"),
        )
        assert result.success
        assert result.final_output.get("evolve_stage") == "dual_coexist"
        assert result.final_output.get("total_in_coexist", 0) >= 2
        assert "switch_snapshot" in result.final_output


class TestChainMergeRecoveryCompact:
    """测试链路6：小文件归并→检查点保存→中断恢复→压实确认。"""
    def test_merge_recovery_full(self):
        result = run_chain(
            "chain_merge_recovery_compact",
            "test_chain6",
            get_chain_steps("chain_merge_recovery_compact"),
        )
        assert result.success
        assert result.final_output.get("checkpoint_saved")
        assert result.final_output.get("compact_success")
        assert result.final_output.get("files_after_compact", 999) < result.final_output.get("files_before_compact", 0)


class TestChainStatsColumnCompact:
    """测试链路7：统计画像采样→列统计计算→倾斜检测→压实触发。"""
    def test_stats_column_full(self):
        # 先注册一些文件
        lake_store.register_file("test_chain7", "20261101", rows=1000, bytes_=256000)
        lake_store.register_file("test_chain7", "20261101", rows=500, bytes_=128000)
        lake_store.register_file("test_chain7", "20261102", rows=50, bytes_=12800)
        result = run_chain(
            "chain_stats_column_compact",
            "test_chain7",
            get_chain_steps("chain_stats_column_compact"),
        )
        assert result.success
        assert "stats_profile" in result.final_output
        profile = result.final_output["stats_profile"]
        assert profile["total_rows_estimate"] >= 1550
        assert "column_stat" in result.final_output


class TestChainSnapshotTimetravelExpire:
    """测试链路8：快照标记可用→时间旅行回查→过期策略清理。"""
    def test_snapshot_timetravel_full(self):
        # 先注册内容和可用快照
        pf = lake_store.register_file("test_chain8", "20261101", rows=200, bytes_=51200)
        lake_store.create_snapshot("test_chain8", [pf.file_id], added_rows=200, deleted_rows=0)
        for snap in lake_store.list_snapshots("test_chain8"):
            lake_store.mark_snapshot_available("test_chain8", snap.snapshot_id)

        result = run_chain(
            "chain_snapshot_timetravel_expire",
            "test_chain8",
            get_chain_steps("chain_snapshot_timetravel_expire"),
        )
        assert result.success
        assert result.final_output["available_snapshots"] >= 1
        assert "timetravel_snapshot" in result.final_output
        assert result.final_output["timetravel_snapshot"] is not None


class TestChainTierMigrationView:
    """测试链路9：存储分层生效→冷热迁移→消费视图发布。"""
    def test_tier_migration_full(self):
        pf = lake_store.register_file("test_chain9", "20261101", rows=100, bytes_=25600)
        snap = lake_store.create_snapshot("test_chain9", [pf.file_id], added_rows=100)
        lake_store.mark_snapshot_available("test_chain9", snap.snapshot_id)
        result = run_chain(
            "chain_tier_migration_watermark",
            "test_chain9",
            get_chain_steps("chain_tier_migration_watermark"),
        )
        assert result.success
        assert "tier_decision" in result.final_output
        assert "migrated_file_count" in result.final_output
        assert "view_snapshot" in result.final_output


class TestChainRetentionAuditReclaim:
    """测试链路10：保留窗口→审计标记→到期回收。"""
    def test_retention_audit_full(self):
        lake_store.register_file("test_chain10", "20261101", rows=50, bytes_=12800)
        lake_store.register_file("test_chain10", "20261101", rows=30, bytes_=7680)
        result = run_chain(
            "chain_retention_audit_reclaim",
            "test_chain10",
            get_chain_steps("chain_retention_audit_reclaim"),
        )
        assert result.success
        assert "retention" in result.final_output
        assert result.final_output["retention"]["audit_tagged"]
        assert result.final_output["audit_cleared"]
        assert "reclaimed_file_count" in result.final_output


class TestLateDataBatch:
    """测试迟到批次场景：水位已推进超过某批次窗口，迟到数据到达。"""
    def test_late_batch_detection(self):
        from app.services.ingest_watermark import IngestWatermarkService
        svc = IngestWatermarkService()
        svc.create_watermark("late_table", "event_time", "2026-11-02T00:00:00")
        lake_store.store_batch("late-batch", "late_table", "src", [{"k": "v"}],
                               "2026-11-01T00:00:00", "2026-11-01T12:00:00")
        lagging = svc.detect_lagging_batches("late_table", "2026-11-02T00:00:00")
        assert len(lagging) >= 1
        assert "late-batch" in lagging


class TestSnapshotVisibility:
    """测试快照可见性：生成中快照不可见，available快照可见。"""
    def test_generating_snapshot_not_visible(self):
        store = lake_store
        pf = store.register_file("vis_test", "20261101", rows=100, bytes_=25600)
        snap = store.create_snapshot("vis_test", [pf.file_id])
        # 未标记 available 时时间旅行不应返回
        result = store.get_snapshot_at_time("vis_test", 9999999999)
        assert result is None or result.status != "available"
        # 标记后应有
        store.mark_snapshot_available("vis_test", snap.snapshot_id)
        result2 = store.get_snapshot_at_time("vis_test", 9999999999)
        assert result2 is not None


class TestMergeInterruptRecovery:
    """测试归并中断恢复：检查点保存在中途断掉后可恢复继续。"""
    def test_interrupt_and_recover(self):
        from app.services.small_file_merge import SmallFileMergeService
        svc = SmallFileMergeService()
        store = lake_store
        for i in range(4):
            store.register_file("rec_tbl", "20261101", rows=5, bytes_=1000)
        files = store.list_files("rec_tbl", "20261101")
        merge_id = "merge-rec-t1"
        store.save_merge_checkpoint(merge_id, [f.file_id for f in files])
        # 恢复
        result = svc.recover_from_checkpoint(merge_id, "rec_tbl")
        assert result is not None
        assert result["recovered"]
        assert result["recover_from_partial"] == 4
"""隔离区迁移与回炉接口：处置清单、迁移流水线、断点恢复、回炉重试、旧入口视图。

与通用资源路由并存：这里承载隔离区的行级领域操作，
通用 /api/quarantine_zone 路由继续提供模块条目列表。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services.quarantine_zone import QuarantineZoneService

router = APIRouter(prefix="/api/quarantine", tags=["隔离区迁移与回炉"])
service = QuarantineZoneService()


class MigratePayload(BaseModel):
    """迁移请求：给出批号迁移单批，缺省迁移该湖表全部旧批。"""
    lake_table: str
    batch_id: str | None = None


class BatchScopePayload(BaseModel):
    lake_table: str
    batch_id: str


class RecoverPayload(BaseModel):
    migration_id: str


class ClaimPayload(BaseModel):
    operator: str = ""


class LegacyReleasePayload(BaseModel):
    lake_table: str
    batch_id: str
    line_no: int


@router.get("/dispositions")
def list_dispositions(
    lake_table: str | None = Query(default=None),
    batch_id: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> dict[str, Any]:
    """逐行处置清单：迁移确认与当前处置状态都可回查。"""
    items = service.list_disposition_lines(lake_table, batch_id, status)
    return {"items": items, "total": len(items)}


@router.get("/dispositions/{disposition_id}")
def get_disposition(disposition_id: str) -> dict[str, Any]:
    line = service.get_disposition_line(disposition_id)
    if line is None:
        raise HTTPException(status_code=404, detail=f"处置行 {disposition_id} 不存在")
    return line


@router.post("/migrate")
def migrate(payload: MigratePayload) -> dict[str, Any]:
    """迁移流水线：把旧批次按规则字段保存的存量行迁入逐行处置清单。"""
    if payload.batch_id:
        return service.migrate_legacy_batch(payload.lake_table, payload.batch_id)
    return service.migrate_all_legacy_batches(payload.lake_table)


@router.post("/recover")
def recover(payload: RecoverPayload) -> dict[str, Any]:
    """停机恢复：按检查点断点续跑，并核对两路看到的处置结果。"""
    result = service.recover_migration(payload.migration_id)
    if not result.get("ok"):
        raise HTTPException(status_code=404, detail=result.get("error", "迁移不存在"))
    return result


@router.post("/dispositions/{disposition_id}/claim")
def claim_disposition(disposition_id: str, payload: ClaimPayload | None = None) -> dict[str, Any]:
    """领取处置行：只有待处置行允许再次领取。"""
    line, message = service.claim_disposition_line(
        disposition_id, payload.operator if payload else "")
    return {"ok": line is not None, "message": message, "line": line}


@router.post("/dispositions/{disposition_id}/retry")
def retry_disposition(disposition_id: str) -> dict[str, Any]:
    """回炉接口（新入口）：重试一行，返回可关联同一份旧记录的入湖凭证。"""
    result, message = service.retry_reprocess_line(disposition_id)
    if result is None:
        return {"ok": False, "message": message}
    return {"ok": True, "message": message, **result}


@router.get("/legacy")
def legacy_view(lake_table: str = Query(...), batch_id: str = Query(...)) -> dict[str, Any]:
    """原隔离清单（旧入口当前视图），含迁移标注的处置编号。"""
    rows = service.legacy_quarantine_view(lake_table, batch_id)
    return {"items": rows, "total": len(rows)}


@router.post("/legacy/restore")
def legacy_restore(payload: BatchScopePayload) -> dict[str, Any]:
    """还原旧入口：重建原隔离清单，每行可追到同一份处置清单行。"""
    rows = service.restore_legacy_entry_view(payload.lake_table, payload.batch_id)
    return {"ok": True, "items": rows, "total": len(rows)}


@router.post("/legacy/release")
def legacy_release(payload: LegacyReleasePayload) -> dict[str, Any]:
    """旧入口：把某一行送回炉；确认写入新旧入口共用日志。"""
    return service.legacy_send_to_reprocess(
        payload.lake_table, payload.batch_id, payload.line_no)


@router.get("/reconcile")
def reconcile(lake_table: str = Query(...), batch_id: str = Query(...)) -> dict[str, Any]:
    """两路核对：旧入口（原隔离清单）与新入口（处置清单）看到的处置结果。"""
    return service.reconcile_batch_views(lake_table, batch_id)


@router.get("/receipts/{voucher}")
def receipt(voucher: str) -> dict[str, Any]:
    """核验入湖凭证：必须能关联回同一份旧记录。"""
    result = service.verify_lake_receipt(voucher)
    if not result.get("valid"):
        raise HTTPException(status_code=404, detail="入湖凭证不存在或不可信")
    return result

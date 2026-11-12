"""代理与请求控制接口：维护代理配置，并提供代理池选择、恢复、排队与历史留档。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.proxy import ProxyService

router = APIRouter(prefix="/api/proxy", tags=["代理与请求控制"])

service = ProxyService()

LIST_FIELDS = ["代理编号", "代理地址", "代理端口", "协议类型", "认证凭据", "并发上限", "冷却时间", "是否可用"]
STATUSES = ["草稿", "待校验", "已启用", "已停用"]
OUTCOMES = ["success", "error", "timeout", "null", "retry_exhausted"]


class AcquirePayload(BaseModel):
    """申请代理额度：request_key 相同的重复调用即恢复，保证幂等不多占额度。"""

    request_key: str | None = None
    task_ref: str | None = None
    at: Any = None  # 可选注入时钟（epoch 秒或 ISO 时间），用于验收序列回放


class InterruptPayload(BaseModel):
    request_key: str
    at: Any = None


class ReleasePayload(BaseModel):
    request_key: str
    outcome: str | None = "success"
    error_message: str | None = None
    retry_count: int = 0
    at: Any = None


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按代理编号检索"),
    status: str | None = Query(default=None, description="草稿、待校验、已启用、已停用"),
    page: int = 1,
    size: int = 20,
    at: Any = Query(default=None, description="可选观测时间，用于冷却状态判定"),
) -> PageResult[dict]:
    """按代理编号与状态过滤代理与请求控制列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size, now=at)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/pool")
def pool_snapshot(at: Any = Query(default=None)) -> dict[str, Any]:
    """代理池工作台：各代理实时额度/失败次数/冷却截止、等待队列与最近分配历史。"""
    return service.pool_snapshot(at=at)


@router.post("/pool/refresh")
def pool_refresh(payload: EntryPayload | None = None) -> dict[str, Any]:
    """恢复 tick：重估冷却到期的单次探测机会并按 FIFO 给等待队列补位，工作台刷新即触发。"""
    at = payload.values.get("at") if payload and payload.values else None
    return service.pool_refresh(at=at)


@router.get("/allocation-history", response_model=PageResult[dict])
def allocation_history(page: int = 1, size: int = 20) -> PageResult[dict]:
    """历史分配记录：按当时代理与原因留档，供工作台与审计追溯。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.history(page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/pool/acquire", response_model=ActionResult)
def acquire(payload: AcquirePayload) -> ActionResult:
    """申请代理额度：按并发额度、失败次数、冷却截止时间排序；额度不足进队列保留位置。"""
    result = service.acquire(payload.request_key, payload.task_ref, at=payload.at)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("reason", "申请被拒绝"), entry=result)
    state_label = {"active": "已分配额度", "queued": "已进入等待队列", "rejected": "已拒绝"}.get(
        result.get("state", ""), result.get("state", "")
    )
    return ActionResult(ok=True, message=state_label, entry=result)


@router.post("/pool/interrupt", response_model=ActionResult)
def interrupt(payload: InterruptPayload) -> ActionResult:
    """任务中断：排队请求保留排队位置，活跃请求释放额度并唤醒队首补位。"""
    result = service.interrupt(payload.request_key, at=payload.at)
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("reason", "中断失败"), entry=result)
    return ActionResult(ok=True, message=result.get("reason", "任务已中断"), entry=result)


@router.post("/pool/release", response_model=ActionResult)
def release(payload: ReleasePayload) -> ActionResult:
    """请求结束归还额度；异常、超时、空值、重试耗尽分别留档，失败请求不占用额度。"""
    outcome = (payload.outcome or "").strip().lower() or None
    if outcome is not None and outcome not in OUTCOMES:
        raise HTTPException(
            status_code=400,
            detail=f"结果取值需为 {', '.join(OUTCOMES)} 之一；未知结果按空值边界处理请传 null",
        )
    result = service.release(
        payload.request_key,
        outcome,
        payload.error_message,
        retry_count=max(payload.retry_count, 0),
        at=payload.at,
    )
    if not result.get("ok"):
        return ActionResult(ok=False, message=result.get("reason", "释放失败"), entry=result)
    return ActionResult(ok=True, message=result.get("reason", "额度已释放"), entry=result)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出代理与请求控制清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "proxy", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条代理配置明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"代理配置 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条代理配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="代理配置已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条代理配置执行提交校验、启用配置、停用配置；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)

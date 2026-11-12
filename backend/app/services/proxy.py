"""代理与请求控制业务规则：状态流转、字段校验、代理池选择与恢复口径都收在这里。"""
from __future__ import annotations

from typing import Any

from app.pool import pool, parse_ts
from app.store import store

MODULE = "proxy"
REQUIRED_FIELDS = ["代理编号", "代理地址", "代理端口"]
STATUS_ORDER = ["草稿", "待校验", "已启用", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "启用配置": "已启用", "停用配置": "已停用"}
NEGATIVE_ACTIONS = ["停用配置"]
# 代理配置清单可维护的业务字段；并发上限、冷却时间在登记时按数值口径校验。
NUMERIC_FIELDS = {"代理端口", "并发上限", "冷却时间"}


class ProxyService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
        now: float | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("代理编号", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        page_rows = rows[start:start + size]
        # 清单继续展示配置额度，同时带上池运行时的真实占用与剩余，避免“显示有额度实则已满”。
        snap = pool.snapshot(now=now)
        runtime = {item["id"]: item for item in snap["proxies"]}
        enriched: list[dict[str, Any]] = []
        for row in page_rows:
            view = dict(row)
            view.update({
                "在途占用": runtime.get(int(row["id"]), {}).get("在途占用", 0),
                "剩余额度": runtime.get(int(row["id"]), {}).get("剩余额度", 0),
                "失败次数": runtime.get(int(row["id"]), {}).get("失败次数", 0),
                "池状态": runtime.get(int(row["id"]), {}).get("池状态", "未启用"),
                "冷却中": runtime.get(int(row["id"]), {}).get("冷却中", False),
            })
            enriched.append(view)
        return enriched, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        for field in ["代理编号", "代理地址", "代理端口", "协议类型", "认证凭据", "并发上限", "冷却时间", "是否可用"]:
            entry[field] = values.get(field)
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"代理配置 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于代理与请求控制可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"代理配置已{action}"

    # ------------------------------------------------------------ 代理池操作
    def pool_snapshot(self, at: Any = None) -> dict[str, Any]:
        return pool.snapshot(now=parse_ts(at))

    def pool_refresh(self, at: Any = None) -> dict[str, Any]:
        return pool.pump(now=parse_ts(at))

    def acquire(self, request_key: str | None, task_ref: str | None, at: Any = None) -> dict[str, Any]:
        return pool.acquire(request_key, task_ref=task_ref, now=parse_ts(at))

    def interrupt(self, request_key: str, at: Any = None) -> dict[str, Any]:
        return pool.interrupt(request_key, now=parse_ts(at))

    def release(
        self,
        request_key: str,
        outcome: str | None,
        error_message: str | None = None,
        retry_count: int = 0,
        at: Any = None,
    ) -> dict[str, Any]:
        return pool.release(
            request_key,
            outcome=outcome,
            error_message=error_message,
            retry_count=retry_count,
            now=parse_ts(at),
        )

    def history(self, page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        return pool.history(page=page, size=size)

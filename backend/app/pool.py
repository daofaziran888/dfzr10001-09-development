"""代理池选择与恢复运行时。

设计口径（对应需求）：
- 选择排序：先看并发可用额度，再看失败次数（冲突时失败更少者优先），再看冷却截止时间；
  草稿、待校验、已停用、不可用、额度满、冷却中的代理都会被排除，并在分配记录里留下跳过原因，
  不会只凭单一状态字段决定。
- 冷却恢复：冷却到期的代理每次恢复只开放一次探测机会（probe_in_flight 标记），
  探测请求不额外多占额度，探测成功才解除冷却，失败则按冷却时长重新冷却。
- 排队：额度不足时请求进入 FIFO 队列保留排队位置；任务中断只打标记、不移队；
  恢复（再次 acquire 同一 request_key）按原位置继续，重复恢复不会多占额度（幂等）。
- 边界：异常、超时、空值、失败重试耗尽分别记录原因；失败请求一律释放额度，不允许占用。

时间统一用 epoch 秒（float），调用方可通过 now 参数注入时钟，方便验收序列回放。
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any, Callable

from app.store import store

MODULE = "proxy"
HISTORY_MODULE = "proxy_allocation"
LOG_MODULE = "log"

# 跳过/分配原因口径：选择时不能只看一个状态，每种排除路径都有独立原因留档。
REASON_NO_CANDIDATE = "无可用代理：候选均被状态、可用性、额度或冷却条件排除"
REASON_QUEUED = "额度不足：请求进入队列保留排队位置"
REASON_ALREADY_ACTIVE = "幂等命中：请求已占用额度，重复恢复不多占额度"
REASON_QUEUED_RESTORED = "任务恢复：请求仍在队列，按原排队位置继续等待"
REASON_INTERRUPTED = "任务中断：请求保留排队位置，额度未释放"
REASON_RELEASED = "请求结束：额度已释放"
REASON_INVALID_OUTCOME = "空值边界：缺少请求结果，未释放任何额度"
REASON_UNKNOWN_REQUEST = "请求不存在或已结束，无需释放额度"
REASON_HISTORY = "历史分配留档"

OUTCOME_SUCCESS = "success"
OUTCOME_ERROR = "error"
OUTCOME_TIMEOUT = "timeout"
OUTCOME_NULL = "null"
OUTCOME_RETRY_EXHAUSTED = "retry_exhausted"
FAILURE_OUTCOMES = {OUTCOME_ERROR, OUTCOME_TIMEOUT, OUTCOME_NULL, OUTCOME_RETRY_EXHAUSTED}
OUTCOME_LABELS = {
    OUTCOME_SUCCESS: "成功",
    OUTCOME_ERROR: "异常",
    OUTCOME_TIMEOUT: "超时",
    OUTCOME_NULL: "空值",
    OUTCOME_RETRY_EXHAUSTED: "重试耗尽",
}

# 可重试边界与重试策略的衔接：重试耗尽是独立边界，不与普通异常混为一谈。
RETRYABLE_OUTCOMES = {OUTCOME_ERROR, OUTCOME_TIMEOUT, OUTCOME_NULL}


def parse_ts(value: Any, *, default: float | None = None) -> float | None:
    """把接口传入的时间（epoch 秒或 ISO 字符串）解析成 epoch 秒；非法值回退默认值。"""
    if value is None or value == "":
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return float(text)
        except ValueError:
            pass
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return default
    return default


def _as_int(value: Any) -> int | None:
    """宽松解析整数字段；占位文本、None、空串等脏数据返回 None，由调用方按边界处理。"""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(text)
    except ValueError:
        try:
            return int(float(text))
        except ValueError:
            return None


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    return str(value).strip().lower() in {"true", "1", "yes", "y", "是", "可用", "启用"}


class ProxyPool:
    """进程内代理池：所有可变状态都在锁内变更，FastAPI 同步接口下线程安全。"""

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._lock = threading.RLock()
        self._clock = clock
        # 代理运行时，按代理 id 索引；每次从配置表刷新上限等静态口径。
        self._runtime: dict[int, dict[str, Any]] = {}
        # request_key -> 请求记录（活跃或排队）。
        self._requests: dict[str, dict[str, Any]] = {}
        # 排队请求 key 的 FIFO 序列。
        self._queue: list[str] = []
        self._seq = 0
        self._history_id = 0
        self._log_id = 0

    # ------------------------------------------------------------------ 基础工具
    def _now(self, now: float | None) -> float:
        return float(now) if now is not None else float(self._clock())

    def _ensure_runtime(self, proxy_id: int, now: float) -> dict[str, Any]:
        state = self._runtime.get(proxy_id)
        if state is None:
            state = {
                "in_use": 0,
                "fail_count": 0,
                "cooldown_until": None,
                "probe_in_flight": False,
            }
            self._runtime[proxy_id] = state
        return state

    def _configs(self) -> list[dict[str, Any]]:
        return store.rows(MODULE)

    def _proxy_limit(self, row: dict[str, Any]) -> int | None:
        """并发上限解析：脏数据（空值/非数字/非正数）视为配置不可用，绝不默认放大额度。"""
        limit = _as_int(row.get("并发上限"))
        if limit is None or limit <= 0:
            return None
        return limit

    def _proxy_cooldown_seconds(self, row: dict[str, Any]) -> int:
        seconds = _as_int(row.get("冷却时间"))
        if seconds is None or seconds <= 0:
            return 300
        return seconds

    def _is_enabled(self, row: dict[str, Any]) -> bool:
        # 选择时不能只看一个状态：状态必须已启用，且“是否可用”为真，二者同时满足才进候选。
        return row.get("status") == "已启用" and _as_bool(row.get("是否可用"))

    def _snapshot_proxy(self, row: dict[str, Any], now: float) -> dict[str, Any]:
        limit = self._proxy_limit(row)
        state = self._ensure_runtime(int(row["id"]), now)
        in_use = int(state["in_use"])
        cooldown_until = state["cooldown_until"]
        cooling = cooldown_until is not None and cooldown_until > now
        probe_ready = cooldown_until is not None and cooldown_until <= now and not state["probe_in_flight"]
        if limit is None:
            pool_state = "配置异常"
            remaining = 0
        elif not self._is_enabled(row):
            pool_state = "未启用"
            remaining = 0
        elif cooling:
            pool_state = "冷却中"
            remaining = 0
        elif state["probe_in_flight"]:
            pool_state = "探测中"
            remaining = max(limit - in_use, 0)
        elif probe_ready:
            pool_state = "待探测"
            remaining = 1
        elif in_use >= limit:
            pool_state = "额度已满"
            remaining = 0
        else:
            pool_state = "可分配"
            remaining = limit - in_use
        return {
            "id": int(row["id"]),
            "代理编号": row.get("代理编号"),
            "status": row.get("status"),
            "是否可用": _as_bool(row.get("是否可用")),
            "并发上限": limit,
            "在途占用": in_use,
            "剩余额度": remaining,
            "失败次数": int(state["fail_count"]),
            "冷却截止时间": cooldown_until,
            "冷却中": cooling,
            "探测进行中": bool(state["probe_in_flight"]),
            "池状态": pool_state,
        }

    # ------------------------------------------------------------------ 留档
    def _next_id(self, module: str) -> int:
        rows = store.rows(module)
        return max((int(row.get("id", 0)) for row in rows), default=0) + 1

    def _record(
        self,
        *,
        request_key: str | None,
        task_ref: str | None,
        proxy_id: int | None,
        proxy_code: str | None,
        action: str,
        outcome: str | None,
        reason: str,
        now: float,
    ) -> dict[str, Any]:
        """历史分配记录按当时代理与原因留档，同时同步一条采集日志。"""
        history_rows = store.rows(HISTORY_MODULE)
        history_id = self._next_id(HISTORY_MODULE)
        entry = {
            "id": history_id,
            "分配编号": f"PA-{history_id:06d}",
            "请求标识": request_key,
            "关联任务": task_ref,
            "代理id": proxy_id,
            "代理编号": proxy_code,
            "动作": action,
            "结果": OUTCOME_LABELS.get(outcome or "", outcome or ""),
            "原因": reason,
            "发生时间": datetime.fromtimestamp(now).isoformat(timespec="seconds"),
            "时间戳": now,
        }
        history_rows.append(entry)

        # 同步采集日志：分配结果与采集日志保持一致，工作台刷新额度时也能在日志页追溯。
        log_rows = store.rows(LOG_MODULE)
        log_id = self._next_id(LOG_MODULE)
        if outcome == OUTCOME_SUCCESS:
            log_status = "成功"
        elif outcome in FAILURE_OUTCOMES:
            log_status = "完全失败"
        elif action == "排队":
            log_status = "部分失败"
        else:
            log_status = "成功"
        log_rows.append({
            "id": log_id,
            "status": log_status,
            "pending": action in {"排队", "中断"},
            "abnormal": outcome in FAILURE_OUTCOMES or action == "排队",
            "日志编号": f"LOG-P{log_id:06d}",
            "关联任务": task_ref or request_key or "-",
            "开始时间": datetime.fromtimestamp(now).isoformat(timespec="seconds"),
            "结束时间": datetime.fromtimestamp(now).isoformat(timespec="seconds"),
            "采集行数": 0,
            "成功条数": 0,
            "失败条数": 1 if outcome in FAILURE_OUTCOMES else 0,
            "错误信息": f"[{proxy_code or '无代理'}] {action}：{reason}",
        })
        return entry

    # ------------------------------------------------------------------ 选择
    def _evaluate(self, row: dict[str, Any], now: float) -> tuple[dict[str, Any], str | None]:
        """返回（代理快照, 跳过原因）；跳过原因为 None 表示可参与排序。"""
        proxy_id = int(row["id"])
        state = self._ensure_runtime(proxy_id, now)
        snap = self._snapshot_proxy(row, now)
        limit = snap["并发上限"]
        if limit is None:
            return snap, f"{snap['代理编号']} 跳过：并发上限为空或非法（空值边界）"
        if row.get("status") != "已启用":
            return snap, f"{snap['代理编号']} 跳过：配置状态为{snap['status']}，未启用"
        if not snap["是否可用"]:
            return snap, f"{snap['代理编号']} 跳过：是否可用为否"
        if state["cooldown_until"] is not None and state["cooldown_until"] > now:
            return snap, (
                f"{snap['代理编号']} 跳过：冷却中，截止 {datetime.fromtimestamp(state['cooldown_until']).isoformat(timespec='seconds')}"
            )
        if state["probe_in_flight"]:
            return snap, f"{snap['代理编号']} 跳过：冷却恢复探测机会已开放且尚未结束"
        if state["in_use"] >= limit:
            return snap, f"{snap['代理编号']} 跳过：并发额度已满（{state['in_use']}/{limit}）"
        return snap, None

    def _select(self, now: float) -> tuple[dict[str, Any] | None, list[str]]:
        """按并发额度、失败次数、冷却截止时间排序选择；冲突时失败次数更少者优先。"""
        candidates: list[tuple[dict[str, Any], dict[str, Any]]] = []
        skips: list[str] = []
        for row in self._configs():
            snap, skip = self._evaluate(row, now)
            if skip:
                skips.append(skip)
                continue
            state = self._runtime[int(row["id"])]
            candidates.append((snap, state))
        if not candidates:
            return None, skips
        # 排序键：剩余额度降序 -> 失败次数升序（冲突优先）-> 冷却截止时间升序（从未冷却者最优先）-> id 升序保序。
        candidates.sort(
            key=lambda item: (
                -item[0]["剩余额度"],
                item[0]["失败次数"],
                item[0]["冷却截止时间"] if item[0]["冷却截止时间"] is not None else -1.0,
                item[0]["id"],
            )
        )
        return candidates[0][0], skips

    def _assign(self, request: dict[str, Any], snap: dict[str, Any], *, is_probe: bool, now: float) -> None:
        proxy_id = snap["id"]
        state = self._runtime[proxy_id]
        state["in_use"] += 1
        if is_probe:
            # 冷却到期只开放一次探测机会：标记后，其他请求在探测结束前不能再选它。
            state["probe_in_flight"] = True
        request.update({
            "state": "active",
            "proxy_id": proxy_id,
            "代理编号": snap["代理编号"],
            "assigned_at": now,
            "is_probe": is_probe,
        })

    def _drain_queue(self, now: float) -> None:
        """额度释放后按 FIFO 唤醒排队请求；中断中的请求跳过但保留位置。"""
        while self._queue:
            key = self._queue[0]
            request = self._requests.get(key)
            if request is None:
                self._queue.pop(0)
                continue
            if request.get("interrupted"):
                return  # 队首中断：保留位置，且不能越过它把额度分给后面的请求
            snap, _ = self._select(now)
            if snap is None:
                return
            self._queue.pop(0)
            is_probe = self._runtime[snap["id"]]["cooldown_until"] is not None and self._runtime[snap["id"]]["cooldown_until"] <= now
            self._assign(request, snap, is_probe=is_probe, now=now)
            reason = "排队恢复：按原排队位置获得额度" + ("（冷却到期单次探测机会）" if is_probe else "")
            self._record(
                request_key=key,
                task_ref=request.get("task_ref"),
                proxy_id=snap["id"],
                proxy_code=snap["代理编号"],
                action="排队恢复分配",
                outcome=None,
                reason=reason,
                now=now,
            )

    # ------------------------------------------------------------------ 对外操作
    def acquire(
        self,
        request_key: str | None,
        *,
        task_ref: str | None = None,
        now: float | None = None,
    ) -> dict[str, Any]:
        """申请代理额度。重复申请同一 request_key 即恢复/重试，保证幂等不多占。"""
        now = self._now(now)
        with self._lock:
            key = str(request_key or "").strip()
            if not key:
                # 空值边界：没有请求标识无法做幂等与排队，直接拒绝且不占任何额度。
                self._record(
                    request_key=None,
                    task_ref=task_ref,
                    proxy_id=None,
                    proxy_code=None,
                    action="申请拒绝",
                    outcome=OUTCOME_NULL,
                    reason="空值边界：请求标识为空，无法登记排队或分配额度",
                    now=now,
                )
                return {"ok": False, "state": "rejected", "reason": "请求标识不能为空", "at": now}

            existing = self._requests.get(key)
            if existing is not None and existing.get("state") == "active":
                # 并发重复恢复：请求已持有额度，原样返回，绝不重复占用。
                return {
                    "ok": True,
                    "state": "active",
                    "request_key": key,
                    "proxy_id": existing.get("proxy_id"),
                    "代理编号": existing.get("代理编号"),
                    "reason": REASON_ALREADY_ACTIVE,
                    "at": now,
                }
            if existing is not None and existing.get("state") == "queued":
                existing["interrupted"] = False  # 恢复中断任务：位置不变，仅清除中断标记。
                self._drain_queue(now)
                refreshed = self._requests.get(key)
                if refreshed and refreshed.get("state") == "queued":
                    return {
                        "ok": True,
                        "state": "queued",
                        "request_key": key,
                        "queue_position": self._queue.index(key) + 1,
                        "reason": REASON_QUEUED_RESTORED,
                        "at": now,
                    }
                return {
                    "ok": True,
                    "state": "active",
                    "request_key": key,
                    "proxy_id": refreshed.get("proxy_id"),
                    "代理编号": refreshed.get("代理编号"),
                    "reason": "任务恢复：按原排队位置获得额度",
                    "at": now,
                }

            self._seq += 1
            request = {
                "request_key": key,
                "task_ref": task_ref,
                "seq": self._seq,
                "state": "queued",
                "interrupted": False,
                "proxy_id": None,
                "代理编号": None,
                "is_probe": False,
                "queued_at": now,
            }
            self._requests[key] = request
            self._queue.append(key)

            snap, skips = self._select(now)
            if snap is None:
                reason = REASON_NO_CANDIDATE + "；" + "；".join(skips) if skips else REASON_NO_CANDIDATE
                self._record(
                    request_key=key,
                    task_ref=task_ref,
                    proxy_id=None,
                    proxy_code=None,
                    action="排队",
                    outcome=None,
                    reason=REASON_QUEUED + "；" + reason,
                    now=now,
                )
                return {
                    "ok": True,
                    "state": "queued",
                    "request_key": key,
                    "queue_position": self._queue.index(key) + 1,
                    "reason": REASON_QUEUED,
                    "skips": skips,
                    "at": now,
                }

            # 新请求排在队列尾部；只有它是队首且有额度时才立即分配，保证 FIFO 公平。
            if self._queue[0] != key:
                self._record(
                    request_key=key,
                    task_ref=task_ref,
                    proxy_id=None,
                    proxy_code=None,
                    action="排队",
                    outcome=None,
                    reason=REASON_QUEUED + "：前方仍有等待请求",
                    now=now,
                )
                return {
                    "ok": True,
                    "state": "queued",
                    "request_key": key,
                    "queue_position": self._queue.index(key) + 1,
                    "reason": REASON_QUEUED,
                    "at": now,
                }

            self._queue.pop(0)
            state = self._runtime[snap["id"]]
            is_probe = state["cooldown_until"] is not None and state["cooldown_until"] <= now
            self._assign(request, snap, is_probe=is_probe, now=now)
            reason = "按并发额度、失败次数、冷却截止时间排序选中" + ("；冷却到期单次探测机会" if is_probe else "")
            self._record(
                request_key=key,
                task_ref=task_ref,
                proxy_id=snap["id"],
                proxy_code=snap["代理编号"],
                action="分配",
                outcome=None,
                reason=reason,
                now=now,
            )
            return {
                "ok": True,
                "state": "active",
                "request_key": key,
                "proxy_id": snap["id"],
                "代理编号": snap["代理编号"],
                "is_probe": is_probe,
                "reason": reason,
                "at": now,
            }

    def interrupt(self, request_key: str, *, now: float | None = None) -> dict[str, Any]:
        """任务中断：排队请求保留位置；活跃请求释放额度并唤醒队首，全程留档。"""
        now = self._now(now)
        with self._lock:
            key = str(request_key or "").strip()
            request = self._requests.get(key)
            if not key or request is None:
                return {"ok": False, "state": "not_found", "reason": REASON_UNKNOWN_REQUEST, "at": now}
            if request.get("state") == "queued":
                request["interrupted"] = True  # 不移队，恢复时原位置继续。
                self._record(
                    request_key=key,
                    task_ref=request.get("task_ref"),
                    proxy_id=None,
                    proxy_code=None,
                    action="中断",
                    outcome=None,
                    reason=REASON_INTERRUPTED + f"，队列位置 {self._queue.index(key) + 1}",
                    now=now,
                )
                return {
                    "ok": True,
                    "state": "interrupted_queued",
                    "request_key": key,
                    "queue_position": self._queue.index(key) + 1,
                    "reason": REASON_INTERRUPTED,
                    "at": now,
                }
            # 活跃任务中断：释放占用（中断不应继续占额度），再尝试补位。
            proxy_id = request.get("proxy_id")
            proxy_code = request.get("代理编号")
            state = self._runtime[proxy_id]
            state["in_use"] = max(state["in_use"] - 1, 0)
            if request.get("is_probe"):
                state["probe_in_flight"] = False
            self._requests.pop(key, None)
            self._record(
                request_key=key,
                task_ref=request.get("task_ref"),
                proxy_id=proxy_id,
                proxy_code=proxy_code,
                action="中断",
                outcome=None,
                reason=REASON_INTERRUPTED + "；" + REASON_RELEASED,
                now=now,
            )
            self._drain_queue(now)
            return {"ok": True, "state": "interrupted_released", "request_key": key, "reason": REASON_RELEASED, "at": now}

    def release(
        self,
        request_key: str,
        *,
        outcome: str | None = OUTCOME_SUCCESS,
        error_message: str | None = None,
        retry_count: int = 0,
        now: float | None = None,
    ) -> dict[str, Any]:
        """请求结束归还额度。异常/超时/空值/重试耗尽分别处理，失败一律不继续占额度。"""
        now = self._now(now)
        with self._lock:
            key = str(request_key or "").strip()
            request = self._requests.get(key)
            if not key or request is None or request.get("state") != "active":
                # 空值/重复释放边界：没有在途请求，不改动任何计数。
                self._record(
                    request_key=key or None,
                    task_ref=None,
                    proxy_id=None,
                    proxy_code=None,
                    action="释放忽略",
                    outcome=OUTCOME_NULL if not outcome else outcome,
                    reason=REASON_INVALID_OUTCOME if not outcome else REASON_UNKNOWN_REQUEST,
                    now=now,
                )
                return {"ok": False, "state": "not_found", "reason": REASON_UNKNOWN_REQUEST, "at": now}

            normalized = (outcome or "").strip().lower()
            if normalized not in {OUTCOME_SUCCESS, *FAILURE_OUTCOMES}:
                # 未识别/空结果按空值边界处理：仍释放额度，但原因单独留档，不伪装成成功。
                normalized = OUTCOME_NULL
            safe_retries = retry_count if isinstance(retry_count, int) and retry_count >= 0 else 0
            proxy_id = request.get("proxy_id")
            proxy_code = request.get("代理编号")
            state = self._runtime[proxy_id]
            row = store.find(MODULE, proxy_id)
            was_probe = bool(request.get("is_probe"))

            state["in_use"] = max(state["in_use"] - 1, 0)
            self._requests.pop(key, None)

            if normalized == OUTCOME_SUCCESS:
                if was_probe:
                    # 探测成功：解除冷却并收回探测标记，代理恢复正常候选。
                    state["cooldown_until"] = None
                    state["probe_in_flight"] = False
                reason = "请求成功，额度已释放" + ("；冷却探测成功，代理恢复可用" if was_probe else "")
                action = "成功释放"
            else:
                state["fail_count"] += 1
                # 探测失败也要立刻放开探测标记，否则该代理永远无法再获得探测机会。
                state["probe_in_flight"] = False
                cooldown_seconds = self._proxy_cooldown_seconds(row or {})
                state["cooldown_until"] = now + cooldown_seconds
                label = OUTCOME_LABELS[normalized]
                extra = f"，错误信息：{error_message}" if error_message else ""
                if normalized == OUTCOME_RETRY_EXHAUSTED:
                    reason = f"失败重试边界：重试 {safe_retries} 次后仍失败（{label}），额度释放并进入冷却 {cooldown_seconds}s{extra}"
                else:
                    reason = f"{label}边界：请求失败不占额度，累计失败 {state['fail_count']} 次，冷却至 {datetime.fromtimestamp(state['cooldown_until']).isoformat(timespec='seconds')}{extra}"
                action = "失败释放"
                if was_probe:
                    reason = "冷却探测失败，重新进入冷却；" + reason

            self._record(
                request_key=key,
                task_ref=request.get("task_ref"),
                proxy_id=proxy_id,
                proxy_code=proxy_code,
                action=action,
                outcome=normalized,
                reason=reason + (f"；{error_message}" if error_message and normalized == OUTCOME_SUCCESS else ""),
                now=now,
            )
            self._drain_queue(now)
            return {
                "ok": True,
                "state": "released",
                "request_key": key,
                "proxy_id": proxy_id,
                "代理编号": proxy_code,
                "outcome": normalized,
                "reason": reason,
                "at": now,
            }

    def pump(self, *, now: float | None = None) -> dict[str, Any]:
        """工作台刷新/恢复 tick：重估冷却到期的探测机会，并按 FIFO 给排队请求补位。"""
        now = self._now(now)
        with self._lock:
            self._drain_queue(now)
            return self.snapshot(now=now)

    # ------------------------------------------------------------------ 查询
    def snapshot(self, *, now: float | None = None) -> dict[str, Any]:
        """工作台数据：代理额度、等待队列、最近分配历史一次取齐。"""
        now = self._now(now)
        with self._lock:
            proxies = [self._snapshot_proxy(row, now) for row in self._configs()]
            queue = []
            for pos, key in enumerate(self._queue, start=1):
                request = self._requests.get(key)
                if request is None:
                    continue
                queue.append({
                    "position": pos,
                    "request_key": key,
                    "关联任务": request.get("task_ref"),
                    "interrupted": bool(request.get("interrupted")),
                    "queued_at": request.get("queued_at"),
                })
            history = list(reversed(store.rows(HISTORY_MODULE)))[:50]
            return {
                "at": now,
                "proxies": proxies,
                "queue": queue,
                "history": history,
                "active_requests": sum(1 for item in self._requests.values() if item.get("state") == "active"),
                "queued_requests": len(queue),
            }

    def history(self, *, page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        with self._lock:
            rows = list(reversed(store.rows(HISTORY_MODULE)))
            total = len(rows)
            start = max(page - 1, 0) * size
            return rows[start:start + size], total

    def reset(self) -> None:
        """清空运行时（测试隔离用，不影响配置表与已留档历史）。"""
        with self._lock:
            self._runtime.clear()
            self._requests.clear()
            self._queue.clear()
            self._seq = 0


pool = ProxyPool()

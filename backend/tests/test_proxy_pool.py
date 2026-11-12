"""代理池选择与恢复的验收测试。

运行（在 backend 目录）：
    PYTHONPATH=. .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest
from datetime import datetime

from app.pool import (
    OUTCOME_ERROR,
    OUTCOME_NULL,
    OUTCOME_RETRY_EXHAUSTED,
    OUTCOME_TIMEOUT,
    pool,
)
from app.seed import SEED_ROWS
from app.store import store

PROXY_MODULE = "proxy"
HISTORY_MODULE = "proxy_allocation"
LOG_MODULE = "log"


def at(minute: int, second: int = 0) -> float:
    """验收序列时钟：2026-11-12 11:MM:SS 的 epoch 秒。"""
    return datetime(2026, 11, 12, 11, minute, second).timestamp()


def reset_state() -> None:
    """每个用例都从干净的种子数据与池运行时开始。"""
    store._tables = {name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()}
    pool.reset()
    for row in store.rows(PROXY_MODULE):
        row["status"] = "已启用"
        row["是否可用"] = True


def proxy_id(code: str) -> int:
    for row in store.rows(PROXY_MODULE):
        if row.get("代理编号") == code:
            return int(row["id"])
    raise KeyError(code)


def cool(pid: int, until: float) -> None:
    state = pool._ensure_runtime(pid, until)
    state["cooldown_until"] = until


def heal_all() -> None:
    """测试辅助：清除所有冷却与探测标记，模拟代理已完成恢复，便于下一轮选择。"""
    for state in pool._runtime.values():
        state["cooldown_until"] = None
        state["probe_in_flight"] = False


def in_use(pid: int) -> int:
    return pool._runtime[pid]["in_use"]


class ProxyPoolAcceptanceTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_state()
        self.p1 = proxy_id("PROX-0001")
        self.p2 = proxy_id("PROX-0002")
        self.p3 = proxy_id("PROX-0003")
        # 验收序列初始态势：PROX-0001 冷却至 11:06，PROX-0003 冷却至 11:10。
        cool(self.p1, at(6))
        cool(self.p3, at(10))

    # ------------------------------------------------------------ 验收序列 I
    def test_acceptance_sequence_i(self) -> None:
        # 11:00-11:05：只有 PROX-0002 可分配，4 个采集任务把它的 4 个并发额度占满。
        for i in range(1, 5):
            result = pool.acquire(f"base-{i}", task_ref=f"TASK-B{i}", now=at(5 if i == 4 else i))
            self.assertEqual(result["state"], "active")
            self.assertEqual(result["proxy_id"], self.p2)
        self.assertEqual(in_use(self.p2), 4)

        # PROX-0001 的等待请求在 11:05 到达：PROX-0002 满、其余冷却中，只能排队且不增加任何占用。
        waiting = pool.acquire("WAIT-001", task_ref="TASK-WAIT", now=at(5))
        self.assertEqual(waiting["state"], "queued")
        self.assertEqual(waiting["queue_position"], 1)
        self.assertEqual(in_use(self.p1), 0)
        self.assertEqual(in_use(self.p3), 0)

        # 11:07 前刷新：冷却均未到期，队列不动，PROX-0003 不增加占用计数。
        pump = pool.pump(now=at(5, 59))
        self.assertEqual([item["request_key"] for item in pump["queue"]], ["WAIT-001"])
        self.assertEqual(in_use(self.p3), 0)

        # 11:06 冷却到期：刷新恢复，等待请求先拿到 PROX-0001 的单次探测额度。
        restored = pool.pump(now=at(6))
        wait_state = pool._requests["WAIT-001"]
        self.assertEqual(wait_state["state"], "active")
        self.assertEqual(wait_state["proxy_id"], self.p1)
        self.assertTrue(wait_state["is_probe"])
        self.assertEqual(in_use(self.p1), 1)
        self.assertTrue(pool._runtime[self.p1]["probe_in_flight"])
        # PROX-0003 在 11:07 前仍处于冷却，且从未被分配、占用计数保持 0。
        self.assertEqual(in_use(self.p3), 0)
        self.assertTrue(pool.snapshot(now=at(6, 59))["proxies"][2]["冷却中"])
        assigned_proxy_ids = {
            row["代理id"] for row in store.rows(HISTORY_MODULE) if row["动作"] in {"分配", "排队恢复分配"}
        }
        self.assertNotIn(self.p3, assigned_proxy_ids)

    def test_cooldown_before_deadline_never_selected(self) -> None:
        for i in range(4):
            pool.acquire(f"base-{i}", now=at(0))
        pool.acquire("WAIT-001", now=at(5))
        # 截止时间之前的任意时刻刷新都不应唤醒。
        for second in (at(5), at(5, 30), at(5, 59)):
            pool.pump(now=second)
            self.assertEqual(pool._requests["WAIT-001"]["state"], "queued")

    # ------------------------------------------------------------ 单次探测机会
    def test_probe_offers_only_one_chance_and_success_clears_cooldown(self) -> None:
        # 让 p3 退出候选（停用），先占满健康代理 PROX-0002 的 4 个额度，
        # 再令 PROX-0001 冷却到期，使其成为唯一可分配候选。
        store.find(PROXY_MODULE, self.p3)["status"] = "已停用"
        for i in range(4):
            r = pool.acquire(f"fill-{i}", now=at(1))
            self.assertEqual(r["proxy_id"], self.p2)
        pool._runtime[self.p1]["cooldown_until"] = None
        cool(self.p1, at(2))
        first = pool.acquire("probe-1", now=at(2))
        self.assertEqual(first["proxy_id"], self.p1)
        self.assertTrue(first["is_probe"])
        # 探测机会只有一次：其他请求再来时 PROX-0001 处于“探测中”，没有候选只能排队。
        second = pool.acquire("probe-2", now=at(2, 1))
        self.assertEqual(second["state"], "queued")
        # 探测成功：冷却解除、探测标记收回，排队请求随即获得 PROX-0001 的正常额度。
        pool.release("probe-1", outcome="success", now=at(2, 2))
        state = pool._runtime[self.p1]
        self.assertIsNone(state["cooldown_until"])
        self.assertFalse(state["probe_in_flight"])
        self.assertEqual(pool._requests["probe-2"]["state"], "active")
        self.assertEqual(pool._requests["probe-2"]["proxy_id"], self.p1)
        self.assertFalse(pool._requests["probe-2"]["is_probe"])

    def test_probe_failure_re_enters_cooldown(self) -> None:
        store.find(PROXY_MODULE, self.p3)["status"] = "已停用"
        for i in range(4):
            pool.acquire(f"fill-{i}", now=at(1))
        pool._runtime[self.p1]["cooldown_until"] = None
        cool(self.p1, at(2))
        pool.acquire("probe-1", now=at(2))
        result = pool.release("probe-1", outcome=OUTCOME_ERROR, error_message="connection reset", now=at(2, 5))
        state = pool._runtime[self.p1]
        self.assertEqual(state["cooldown_until"], at(2, 5) + 300)
        self.assertFalse(state["probe_in_flight"])
        self.assertEqual(state["fail_count"], 1)
        # 重新冷却期间所有代理都不可选，等待请求继续排队且不会再给 PROX-0001 探测机会。
        nxt = pool.acquire("probe-2", now=at(2, 6))
        self.assertEqual(nxt["state"], "queued")
        self.assertNotIn("probe-2", {k for k, v in pool._requests.items() if v.get("proxy_id") == self.p1})
        self.assertIn("冷却探测失败", result["reason"])

    # ------------------------------------------------------------ 失败次数优先
    def test_conflict_prefers_lower_fail_count(self) -> None:
        # 只保留 p1/p2，先令二者各占 1 个额度（剩余额度持平），再给 p2 预置更高失败计数。
        store.find(PROXY_MODULE, self.p3)["status"] = "已停用"
        heal_all()  # 解除 setUp 预置的冷却，让 p1/p2 都成为健康候选
        first = pool.acquire("seed-1", now=at(0))  # 初始额度相同，按 id 落到 p1
        self.assertEqual(first["proxy_id"], self.p1)
        second = pool.acquire("seed-2", now=at(0, 1))  # 此时 p2 剩余额度更多，落到 p2
        self.assertEqual(second["proxy_id"], self.p2)
        # 测试辅助：直接构造同额度、不同失败次数的冲突局面（失败计数的累计路径由其他用例覆盖）。
        pool._runtime[self.p1]["fail_count"] = 0
        pool._runtime[self.p2]["fail_count"] = 2
        pool._runtime[self.p1]["in_use"] = 2
        pool._runtime[self.p2]["in_use"] = 2
        self.assertEqual(pool.snapshot(now=at(0, 2))["proxies"][0]["剩余额度"], 2)
        tie = pool.acquire("tie-break", now=at(0, 2))
        self.assertEqual(tie["proxy_id"], self.p1)  # 冲突时失败次数更少者优先

    # ------------------------------------------------------------ 排队、中断与恢复
    def test_interrupt_keeps_queue_position_and_blocks_fairness_break(self) -> None:
        for i in range(4):
            pool.acquire(f"base-{i}", now=at(0))
        pool.acquire("wait-1", now=at(1))
        pool.acquire("wait-2", now=at(1, 1))
        # wait-1 中断但保留队首位置。
        interrupted = pool.interrupt("wait-1", now=at(1, 2))
        self.assertEqual(interrupted["state"], "interrupted_queued")
        self.assertEqual(interrupted["queue_position"], 1)
        self.assertTrue(pool._requests["wait-1"]["interrupted"])
        # 释放一个额度：队首中断中，wait-2 不能越过它补位。
        pool.release("base-0", outcome="success", now=at(1, 3))
        self.assertEqual(pool._requests["wait-2"]["state"], "queued")
        self.assertEqual(in_use(self.p2), 3)
        # 恢复 wait-1：原位置优先获得额度，随后再释放额度才轮到 wait-2。
        restored = pool.acquire("wait-1", now=at(1, 4))
        self.assertEqual(restored["state"], "active")
        self.assertFalse(pool._requests["wait-1"]["interrupted"])
        self.assertEqual(pool._requests["wait-2"]["state"], "queued")
        pool.release("base-1", outcome="success", now=at(1, 5))
        self.assertEqual(pool._requests["wait-2"]["state"], "active")

    def test_queued_restore_is_idempotent_and_keeps_single_position(self) -> None:
        for i in range(4):
            pool.acquire(f"base-{i}", now=at(0))
        pool.acquire("wait-1", now=at(1))
        pool.interrupt("wait-1", now=at(1, 1))
        again = pool.acquire("wait-1", now=at(1, 2))
        self.assertEqual(again["state"], "queued")
        self.assertEqual(len(pool._queue), 1)  # 重复恢复不会产生第二个排队位
        self.assertEqual(again["queue_position"], 1)

    def test_active_restore_does_not_consume_extra_quota(self) -> None:
        result = pool.acquire("dup-1", now=at(0))
        pid = result["proxy_id"]
        before = in_use(pid)
        again = pool.acquire("dup-1", now=at(0, 1))
        self.assertEqual(again["state"], "active")
        self.assertEqual(again["proxy_id"], pid)
        self.assertEqual(in_use(pid), before)  # 并发重复恢复不多占额度
        self.assertIn("幂等命中", again["reason"])

    def test_active_interrupt_releases_quota_and_wakes_queue(self) -> None:
        active = pool.acquire("run-1", now=at(0))
        pid = active["proxy_id"]
        pool.acquire("wait-1", now=at(0, 1))
        before = in_use(pid)
        pool.interrupt("run-1", now=at(0, 2))
        self.assertEqual(in_use(pid), before - 1)
        self.assertNotIn("run-1", pool._requests)
        self.assertEqual(pool._requests["wait-1"]["state"], "active")

    # ------------------------------------------------------------ 失败边界不占额度
    def test_failure_outcomes_release_quota_with_distinct_reasons(self) -> None:
        outcomes = [OUTCOME_ERROR, OUTCOME_TIMEOUT, OUTCOME_NULL, OUTCOME_RETRY_EXHAUSTED]
        for index, outcome in enumerate(outcomes):
            heal_all()  # 每轮独立：清掉上一轮失败造成的冷却，保证本轮能分配到代理
            key = f"fail-{outcome}"
            result = pool.acquire(key, now=at(index))
            self.assertEqual(result["state"], "active")
            pid = result["proxy_id"]
            before = in_use(pid)
            released = pool.release(key, outcome=outcome, error_message="boom", now=at(index, 1))
            self.assertEqual(released["outcome"], outcome)
            self.assertEqual(in_use(pid), before - 1)  # 失败请求不占用额度
            self.assertGreaterEqual(pool._runtime[pid]["fail_count"], 1)
            self.assertIsNotNone(pool._runtime[pid]["cooldown_until"])
        reasons = {row["结果"] + row["原因"] for row in store.rows(HISTORY_MODULE) if row["动作"] == "失败释放"}
        self.assertTrue(any("异常边界" in item for item in reasons))
        self.assertTrue(any("超时边界" in item for item in reasons))
        self.assertTrue(any("空值边界" in item for item in reasons))
        self.assertTrue(any("失败重试边界" in item for item in reasons))

    def test_unknown_outcome_treated_as_null_boundary(self) -> None:
        result = pool.acquire("weird", now=at(0))
        pid = result["proxy_id"]
        released = pool.release("weird", outcome="not-a-real-outcome", now=at(0, 1))
        self.assertEqual(released["outcome"], OUTCOME_NULL)
        self.assertIn("空值边界", released["reason"])
        self.assertEqual(in_use(pid), 0)

    def test_retry_chain_reacquires_quota_and_records_exhaustion(self) -> None:
        def total_in_use() -> int:
            return sum(item["in_use"] for item in pool._runtime.values())

        # 第一次申请失败（超时）：额度立即释放，不允许失败请求占着额度等重试。
        first = pool.acquire("retry-req", task_ref="TASK-RETRY", now=at(0))
        pool.release("retry-req", outcome=OUTCOME_TIMEOUT, now=at(0, 1))
        self.assertEqual(total_in_use(), 0)
        # 冷却恢复后用同一 request_key 重试：重新获得额度，是一次新的占用而非叠加；
        # 选择仍按排序口径，可能切换到失败次数更少的代理。
        heal_all()
        retried = pool.acquire("retry-req", now=at(0, 2))
        self.assertEqual(retried["state"], "active")
        self.assertEqual(total_in_use(), 1)
        # 再次失败且重试耗尽：独立边界原因，带重试次数留档。
        exhausted = pool.release(
            "retry-req", outcome=OUTCOME_RETRY_EXHAUSTED, error_message="all attempts failed",
            retry_count=3, now=at(0, 3),
        )
        self.assertEqual(exhausted["outcome"], OUTCOME_RETRY_EXHAUSTED)
        self.assertIn("重试 3 次", exhausted["reason"])
        self.assertEqual(total_in_use(), 0)  # 耗尽后同样不占额度
        rows = [r for r in store.rows(HISTORY_MODULE) if r["请求标识"] == "retry-req"]
        self.assertTrue(any("失败重试边界" in r["原因"] for r in rows))

    def test_null_request_key_is_rejected_without_quota_change(self) -> None:
        result = pool.acquire("  ", task_ref="TASK-X", now=at(0))
        self.assertFalse(result["ok"])
        self.assertEqual(sum(item["in_use"] for item in pool._runtime.values()), 0)
        self.assertEqual(pool._queue, [])

    def test_release_unknown_request_changes_nothing(self) -> None:
        result = pool.release("ghost", outcome=OUTCOME_ERROR, now=at(0))
        self.assertFalse(result["ok"])
        self.assertEqual(sum(item["in_use"] for item in pool._runtime.values()), 0)

    # ------------------------------------------------------------ 选择口径与留档
    def test_selection_uses_more_than_one_status_field(self) -> None:
        # 已启用但“是否可用=否”：必须排除。
        store.find(PROXY_MODULE, self.p1)["是否可用"] = False
        picked = pool.acquire("r1", now=at(0))
        self.assertNotEqual(picked.get("proxy_id"), self.p1)
        # 状态回到草稿，即使是否可用为真也必须排除。
        store.find(PROXY_MODULE, self.p1)["是否可用"] = True
        store.find(PROXY_MODULE, self.p1)["status"] = "草稿"
        pool.release("r1", outcome="success", now=at(0, 1))
        picked2 = pool.acquire("r2", now=at(0, 2))
        self.assertNotEqual(picked2.get("proxy_id"), self.p1)

    def test_invalid_concurrency_limit_never_grants_quota(self) -> None:
        for dirty in (None, "", "占位文本", 0, -3):
            reset_state()
            for row in store.rows(PROXY_MODULE):
                row["status"] = "已启用"
                row["是否可用"] = True
            store.find(PROXY_MODULE, self.p1)["并发上限"] = dirty
            result = pool.acquire("dirty", now=at(0))
            self.assertNotEqual(result.get("proxy_id"), self.p1, msg=f"脏上限 {dirty!r} 不应获得额度")
            snap = next(item for item in pool.snapshot(now=at(0))["proxies"] if item["id"] == self.p1)
            self.assertEqual(snap["池状态"], "配置异常")
            self.assertEqual(snap["剩余额度"], 0)

    def test_history_and_collection_log_are_synced(self) -> None:
        pool.acquire("logged", task_ref="TASK-LOG", now=at(0))
        pool.release("logged", outcome=OUTCOME_TIMEOUT, error_message="read timeout", now=at(0, 1))
        history = [row for row in store.rows(HISTORY_MODULE) if row["请求标识"] == "logged"]
        self.assertTrue(history)
        self.assertTrue(all(row["代理编号"] for row in history))  # 按当时代理留档
        self.assertTrue(any("超时边界" in row["原因"] for row in history))
        logs = [row for row in store.rows(LOG_MODULE) if "read timeout" in str(row.get("错误信息", ""))]
        self.assertEqual(len(logs), 1)
        self.assertEqual(logs[0]["失败条数"], 1)
        self.assertEqual(logs[0]["status"], "完全失败")

    def test_snapshot_reports_quota_queue_and_history(self) -> None:
        pool.acquire("r1", now=at(0))
        snap = pool.snapshot(now=at(0))
        self.assertEqual(snap["active_requests"], 1)
        proxy_view = {item["代理编号"]: item for item in snap["proxies"]}
        self.assertIn("剩余额度", proxy_view["PROX-0001"])
        self.assertIn("失败次数", proxy_view["PROX-0001"])
        self.assertIn("冷却截止时间", proxy_view["PROX-0001"])


class ProxyPoolApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from fastapi.testclient import TestClient

        from app.main import app

        cls.client = TestClient(app)

    def setUp(self) -> None:
        reset_state()

    def test_pool_workbench_and_allocation_flow(self) -> None:
        acquired = self.client.post("/api/proxy/pool/acquire", json={"request_key": "api-1", "task_ref": "TASK-API"})
        self.assertEqual(acquired.status_code, 200)
        self.assertTrue(acquired.json()["ok"])

        listing = self.client.get("/api/proxy").json()
        enriched = listing["items"][0]
        self.assertIn("在途占用", enriched)
        self.assertIn("剩余额度", enriched)

        bench = self.client.get("/api/proxy/pool").json()
        self.assertEqual(bench["active_requests"], 1)
        self.assertIn("proxies", bench)

        refreshed = self.client.post("/api/proxy/pool/refresh", json={}).json()
        self.assertIn("queue", refreshed)

        bad_outcome = self.client.post(
            "/api/proxy/pool/release", json={"request_key": "api-1", "outcome": "exploded"}
        )
        self.assertEqual(bad_outcome.status_code, 400)

        released = self.client.post(
            "/api/proxy/pool/release", json={"request_key": "api-1", "outcome": "timeout", "error_message": "慢"}
        )
        self.assertEqual(released.status_code, 200)
        self.assertTrue(released.json()["ok"])

        history = self.client.get("/api/proxy/allocation-history").json()
        self.assertGreaterEqual(history["total"], 2)
        self.assertTrue(any(row["请求标识"] == "api-1" for row in history["items"]))

    def test_null_request_key_rejected_via_api(self) -> None:
        response = self.client.post("/api/proxy/pool/acquire", json={"request_key": ""})
        self.assertEqual(response.status_code, 200)
        self.assertFalse(response.json()["ok"])


if __name__ == "__main__":
    unittest.main()

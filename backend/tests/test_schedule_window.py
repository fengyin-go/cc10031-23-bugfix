"""调度窗口到期判定、时区分支与任务队列入队口径的回归测试。"""
from __future__ import annotations

import sys
import threading
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

from app.services import schedule_window
from app.services.scheduler import SchedulerService
from app.services.task_queue import TaskQueueService, _PROCESSING
from app.store import store

SHANGHAI = ZoneInfo("Asia/Shanghai")


def _plan(plan_id: int, *, cron: str, window: str | None = None,
          status: str = "已激活", timezone_name: str | None = None,
          priority: str = "中") -> dict:
    plan = {
        "id": plan_id,
        "status": status,
        "pending": True,
        "abnormal": False,
        "计划名称": f"计划{plan_id}",
        "绑定流程": f"流程{plan_id}",
        "cron表达式": cron,
        "执行窗口": window,
        "并发数": "1",
        "优先级": priority,
        "通知渠道": "邮件",
    }
    if timezone_name:
        plan["时区"] = timezone_name
    return plan


@pytest.fixture()
def clean_scheduler():
    """隔离调度/队列数据与进程级处理台账。"""
    scheduler_rows = store.rows("scheduler")
    queue_rows = store.rows("task_queue")
    scheduler_backup = [dict(row) for row in scheduler_rows]
    queue_backup = [dict(row) for row in queue_rows]
    scheduler_rows.clear()
    queue_rows.clear()
    _PROCESSING.reset()
    try:
        yield scheduler_rows, queue_rows
    finally:
        scheduler_rows.clear()
        scheduler_rows.extend(scheduler_backup)
        queue_rows.clear()
        queue_rows.extend(queue_backup)
        _PROCESSING.reset()


def _reset_watermarks() -> None:
    _PROCESSING.reset()


# ---------------------------------------------------------------------------
# 窗口判定
# ---------------------------------------------------------------------------

class TestWindowEvaluation:
    def test_before_window_is_not_due(self) -> None:
        plan = _plan(1, cron="0 9 * * *", window="09:00-11:00")
        decision = schedule_window.evaluate_plan(
            plan, at="2026-11-18T08:59:00+08:00")
        assert decision["due"] is False
        assert decision["reason"] == "未到执行窗口"
        assert decision["credential"] is None
        assert decision["next_occurrence"] == "2026-11-18T09:00:00+08:00"

    def test_inside_window_is_due(self) -> None:
        plan = _plan(1, cron="0 9 * * *", window="09:00-11:00")
        decision = schedule_window.evaluate_plan(
            plan, at="2026-11-18T09:30:00+08:00")
        assert decision["due"] is True
        assert decision["occurrence"] == "2026-11-18T09:00:00+08:00"
        assert decision["credential"] == "PLAN-1-20261118T010000Z"

    def test_after_window_is_not_due(self) -> None:
        plan = _plan(1, cron="0 9 * * *", window="09:00-11:00")
        decision = schedule_window.evaluate_plan(
            plan, at="2026-11-18T11:30:00+08:00")
        assert decision["due"] is False
        assert decision["reason"] == "已过执行窗口"

    def test_duration_window(self) -> None:
        plan = _plan(1, cron="0 * * * *", window="45m")
        assert schedule_window.evaluate_plan(
            plan, at="2026-11-18T09:30:00+08:00")["due"] is True
        assert schedule_window.evaluate_plan(
            plan, at="2026-11-18T09:50:00+08:00")["due"] is False

    def test_default_window_is_short_after_cron(self) -> None:
        plan = _plan(1, cron="0 * * * *")
        assert schedule_window.evaluate_plan(
            plan, at="2026-11-18T09:00:30+08:00")["due"] is True
        assert schedule_window.evaluate_plan(
            plan, at="2026-11-18T09:10:00+08:00")["due"] is False

    def test_invalid_cron_is_not_due(self) -> None:
        plan = _plan(1, cron="不是 cron", window="09:00-11:00")
        decision = schedule_window.evaluate_plan(
            plan, at="2026-11-18T09:30:00+08:00")
        assert decision["due"] is False
        assert "cron" in decision["reason"]

    def test_credential_is_stable_per_window(self) -> None:
        plan = _plan(1, cron="0 9 * * *", window="09:00-11:00")
        first = schedule_window.evaluate_plan(
            plan, at="2026-11-18T09:05:00+08:00")
        retry = schedule_window.evaluate_plan(
            plan, at="2026-11-18T09:40:00+08:00")
        assert first["credential"] == retry["credential"]


# ---------------------------------------------------------------------------
# 时区处理分支
# ---------------------------------------------------------------------------

class TestTimezoneBranches:
    def test_scheduler_mode_normalizes_every_plan(self) -> None:
        plan = _plan(1, cron="0 9 * * *", window="09:00-11:00",
                     timezone_name="Europe/London")
        # 17:05 上海 = 09:05 伦敦，但统一换算到调度时区后已过 09:00-11:00。
        decision = schedule_window.evaluate_plan(
            plan, at="2026-11-18T17:05:00+08:00", mode="scheduler")
        assert decision["mode"] == "scheduler"
        assert decision["timezone"] == "Asia/Shanghai"
        assert decision["due"] is False

    def test_plan_mode_uses_frozen_timezone(self) -> None:
        plan = _plan(1, cron="0 9 * * *", window="09:00-11:00",
                     timezone_name="Europe/London")
        decision = schedule_window.evaluate_plan(
            plan, at="2026-11-18T17:05:00+08:00", mode="plan")
        assert decision["mode"] == "plan"
        assert decision["timezone"] == "Europe/London"
        assert decision["due"] is True
        assert decision["occurrence"] == "2026-11-18T09:00:00+00:00"

    def test_both_branches_only_return_real_due_plans(self) -> None:
        plans = [
            _plan(1, cron="0 9 * * *", window="09:00-11:00",
                  timezone_name="Europe/London"),
            _plan(2, cron="0 9 * * *", window="09:00-11:00",
                  timezone_name="America/New_York"),
        ]
        at = "2026-11-18T17:05:00+08:00"
        scheduler_due = {d["plan_id"] for d in schedule_window.due_plans(
            plans, at=at, mode="scheduler")}
        plan_due = {d["plan_id"] for d in schedule_window.due_plans(
            plans, at=at, mode="plan")}
        assert scheduler_due == set()
        assert plan_due == {1}

    def test_plan_without_timezone_falls_back_to_scheduler_tz(self) -> None:
        decision = schedule_window.evaluate_plan(
            _plan(1, cron="0 9 * * *"), at="2026-11-18T09:05:00+08:00",
            mode="plan")
        assert decision["timezone"] == "Asia/Shanghai"
        assert decision["plan_timezone_frozen"] is False


# ---------------------------------------------------------------------------
# 调度发布
# ---------------------------------------------------------------------------

class TestScheduleDispatch:
    def test_only_due_plans_are_published(self, clean_scheduler) -> None:
        rows, _ = clean_scheduler
        rows.extend([
            _plan(1, cron="0 9 * * *", window="09:00-11:00"),
            _plan(2, cron="0 23 * * *", window="23:00-23:30", priority="高"),
            _plan(3, cron="0 9 * * *", window="09:00-11:00", status="已暂停"),
        ])
        result = SchedulerService().schedule_crawl_jobs(
            at="2026-11-18T09:05:00+08:00")
        assert result["triggered"] == 1
        assert [item["plan_id"] for item in result["plans"]] == [1]
        assert {item["plan_id"] for item in result["waiting"]} == {2}

        published = [
            message for message in store.channel.snapshot(
                topic="rpa.crawl_scheduled")
            if message["payload"].get("plan_id") == 1
            and message["payload"].get("occurrence", "").startswith(
                "2026-11-18T09:00")
        ]
        assert len(published) == 1

    def test_retry_same_window_is_not_published_twice(
            self, clean_scheduler) -> None:
        rows, _ = clean_scheduler
        rows.append(_plan(1, cron="0 9 * * *", window="09:00-11:00"))
        service = SchedulerService()
        before = len(store.channel.snapshot(topic="rpa.crawl_scheduled"))
        first = service.schedule_crawl_jobs(at="2026-11-18T09:05:00+08:00")
        retry = service.schedule_crawl_jobs(at="2026-11-18T09:20:00+08:00")
        assert first["triggered"] == 1
        assert retry["triggered"] == 0
        assert len(retry["skipped"]) == 1
        after = len(store.channel.snapshot(topic="rpa.crawl_scheduled"))
        assert after - before == 1

    def test_next_window_triggers_again(self, clean_scheduler) -> None:
        rows, _ = clean_scheduler
        rows.append(_plan(1, cron="0 * * * *", window="50m"))
        service = SchedulerService()
        service.schedule_crawl_jobs(at="2026-11-18T09:05:00+08:00")
        second = service.schedule_crawl_jobs(at="2026-11-18T10:05:00+08:00")
        assert second["triggered"] == 1

    def test_stale_window_after_watermark_is_not_republished(
            self, clean_scheduler) -> None:
        rows, _ = clean_scheduler
        rows.append(_plan(1, cron="0 * * * *", window="50m"))
        service = SchedulerService()
        service.schedule_crawl_jobs(at="2026-11-18T10:05:00+08:00")
        # 水位已经推进到 10:00 窗口，回补 09:00 窗口不再发布。
        backfill = service.schedule_crawl_jobs(at="2026-11-18T09:30:00+08:00")
        assert backfill["triggered"] == 0
        assert backfill["skipped"][0]["state"] == "旧窗口已跳过"


# ---------------------------------------------------------------------------
# 任务队列入队
# ---------------------------------------------------------------------------

class TestTaskQueueIngest:
    def test_only_valid_credentials_create_tasks(self, clean_scheduler) -> None:
        rows, queue_rows = clean_scheduler
        rows.append(_plan(1, cron="0 9 * * *", window="09:00-11:00"))
        SchedulerService().schedule_crawl_jobs(at="2026-11-18T09:05:00+08:00")

        result = TaskQueueService().consume_scheduled_crawl_jobs(
            at="2026-11-18T09:05:00+08:00")
        assert result["generated"] == 1
        assert len(queue_rows) == 1
        assert queue_rows[0]["status"] == "排队中"
        assert queue_rows[0]["触发凭证"] == "PLAN-1-20261118T010000Z"

    def test_not_due_credential_is_rejected(self, clean_scheduler) -> None:
        rows, queue_rows = clean_scheduler
        rows.append(_plan(1, cron="0 9 * * *", window="09:00-11:00"))
        store.channel.publish(
            topic="rpa.crawl_scheduled",
            source="scheduler",
            payload={
                "plan_id": 1,
                "plan_name": "计划1",
                "credential": "PLAN-1-20261118T010000Z",
                "token": "PLAN-1-20261118T010000Z",
                "occurrence": "2026-11-18T09:00:00+08:00",
                "timezone": "Asia/Shanghai",
                "mode": "scheduler",
            },
        )
        # 08:00 还没到 09:00 的窗口。
        result = TaskQueueService().consume_scheduled_crawl_jobs(
            at="2026-11-18T08:10:00+08:00")
        assert result["generated"] == 0
        assert result["rejected"] == 1
        assert queue_rows == []
        assert result["result_list"][0]["outcome"] == "窗口未通过"
        assert "未到执行窗口" in result["result_list"][0]["reason"]

    def test_forged_credential_is_rejected(self, clean_scheduler) -> None:
        rows, queue_rows = clean_scheduler
        rows.append(_plan(1, cron="0 9 * * *", window="09:00-11:00"))
        store.channel.publish(
            topic="rpa.crawl_scheduled",
            source="scheduler",
            payload={
                "plan_id": 1,
                "credential": "PLAN-1-20990101T000000Z",
                "occurrence": "2026-11-18T09:00:00+08:00",
            },
        )
        result = TaskQueueService().consume_scheduled_crawl_jobs(
            at="2026-11-18T09:05:00+08:00")
        assert result["generated"] == 0
        assert queue_rows == []

    def test_duplicate_message_only_enqueues_once(self, clean_scheduler) -> None:
        rows, queue_rows = clean_scheduler
        rows.append(_plan(1, cron="0 9 * * *", window="09:00-11:00"))
        payload = {
            "plan_id": 1,
            "plan_name": "计划1",
            "priority": "高",
            "credential": "PLAN-1-20261118T010000Z",
            "token": "PLAN-1-20261118T010000Z",
            "occurrence": "2026-11-18T09:00:00+08:00",
            "timezone": "Asia/Shanghai",
            "mode": "scheduler",
        }
        for _ in range(2):
            store.channel.publish(
                topic="rpa.crawl_scheduled", source="scheduler",
                payload=dict(payload))
        result = TaskQueueService().consume_scheduled_crawl_jobs(
            at="2026-11-18T09:05:00+08:00")
        assert result["generated"] == 1
        assert result["duplicated"] == 1
        assert len(queue_rows) == 1

    def test_concurrent_submissions_take_effect_once(
            self, clean_scheduler) -> None:
        rows, queue_rows = clean_scheduler
        rows.append(_plan(1, cron="0 9 * * *", window="09:00-11:00"))
        payload = {
            "plan_id": 1,
            "plan_name": "计划1",
            "credential": "PLAN-1-20261118T010000Z",
            "occurrence": "2026-11-18T09:00:00+08:00",
            "timezone": "Asia/Shanghai",
            "mode": "scheduler",
        }
        for _ in range(4):
            store.channel.publish(
                topic="rpa.crawl_scheduled", source="scheduler",
                payload=dict(payload))

        errors: list[Exception] = []

        def worker() -> None:
            try:
                TaskQueueService().consume_scheduled_crawl_jobs(
                    at="2026-11-18T09:05:00+08:00")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert not errors
        assert len(queue_rows) == 1

    def test_processing_list_and_results_are_updated_together(
            self, clean_scheduler) -> None:
        rows, _ = clean_scheduler
        rows.append(_plan(1, cron="0 9 * * *", window="09:00-11:00"))
        SchedulerService().schedule_crawl_jobs(at="2026-11-18T09:05:00+08:00")
        service = TaskQueueService()
        service.consume_scheduled_crawl_jobs(at="2026-11-18T09:05:00+08:00")
        snapshot = service.processing_snapshot()
        assert snapshot["processing_count"] == snapshot["result_count"] == 1
        assert snapshot["processing_list"][0]["task_id"] == \
            snapshot["result_list"][0]["task_id"]

    def test_watermark_terminal_state_follows_task_status(
            self, clean_scheduler) -> None:
        rows, _ = clean_scheduler
        plan = _plan(1, cron="0 23 * * *", window="23:00-23:30")
        rows.append(plan)
        SchedulerService().schedule_crawl_jobs(at="2026-11-18T23:05:00+08:00")
        service = TaskQueueService()
        enqueued = service.consume_scheduled_crawl_jobs(
            at="2026-11-18T23:05:00+08:00")
        assert enqueued["generated"] == 1
        task_id = enqueued["task_ids"][0]
        assert plan["队列水位终态"] == "已受理"

        service.update_queue_status(task_id, "已完成", {"rows": 10})
        assert plan["队列水位终态"] == "已完成"
        assert _PROCESSING.watermarks[1]["state"] == "已完成"

    def test_cancel_action_syncs_watermark_terminal_state(
            self, clean_scheduler) -> None:
        rows, _ = clean_scheduler
        plan = _plan(23, cron="0 23 * * *", window="23:00-23:30")
        rows.append(plan)
        SchedulerService().schedule_crawl_jobs(at="2026-11-18T23:05:00+08:00")
        service = TaskQueueService()
        enqueued = service.consume_scheduled_crawl_jobs(
            at="2026-11-18T23:05:00+08:00")
        task_id = enqueued["task_ids"][0]

        entry, message = service.run_action(task_id, "取消任务")
        assert entry is not None
        assert "取消任务" in message
        assert entry["status"] == "已取消"
        assert plan["队列水位终态"] == "已取消"
        assert _PROCESSING.watermarks[23]["state"] == "已取消"
        snapshot = service.processing_snapshot()
        assert snapshot["result_list"][0]["result_state"] == "已取消"

        # 重复入队仍然只生效一次，重试不增加排队任务。
        retry = service.consume_scheduled_crawl_jobs(
            at="2026-11-18T09:05:00+08:00")
        assert retry["generated"] == 0

    def test_step_flow_still_uses_shared_path(self, clean_scheduler) -> None:
        _, queue_rows = clean_scheduler
        store.channel.publish(
            topic="rpa.steps_submitted",
            source="step_editor",
            payload={
                "bot_id": 7,
                "steps": [
                    {"step_id": 101, "name": "打开页面", "type": "navigate"},
                    {"step_id": 102, "name": "提取表格", "type": "extract"},
                ],
            },
        )
        service = TaskQueueService()
        first = service.consume_steps_to_queue()
        second = service.consume_steps_to_queue()
        assert first["generated"] == 2
        assert second["generated"] == 0
        assert len(queue_rows) == 2

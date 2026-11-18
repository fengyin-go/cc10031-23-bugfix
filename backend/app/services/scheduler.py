"""调度计划领域服务 — 管理定时采集计划，输出 domain_snapshot。

数据工程核心：数据清洗与转换链路处理 — 跨模块数据产物

到期口径：
    ``schedule_crawl_jobs`` 不再遍历“已激活”直接发布，而是走
    :func:`app.services.schedule_window.evaluate_plan` 的共用判定：
    当前调度时点必须越过 cron 触发点，且仍落在计划执行窗口内。
    每个到期窗口生成唯一触发凭证，已发布过的窗口重试时不再二次发布。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from app.config import settings
from app.services import schedule_window
from app.services.base import BaseModuleService
from app.store import store

OPTIONAL_FIELDS = ("时区", "计划时区")

# 计划优先级是自由文本，任务队列只认高/中/低。
_TASK_PRIORITIES = {"高", "中", "低"}


class SchedulerService(BaseModuleService):
    MODULE = "scheduler"
    ENTITY = "采集计划"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["计划名称", "绑定流程", "cron表达式", "执行窗口", "并发数", "优先级", "通知渠道"]
    REQUIRED_FIELDS = ["计划名称", "绑定流程", "cron表达式"]
    STATUS_ORDER = ["草稿", "已激活", "已暂停", "已归档"]
    ACTION_RULES = {"激活计划": "草稿", "暂停计划": "已激活", "归档计划": "已暂停"}
    NEGATIVE_ACTIONS = []

    _dispatch_lock = threading.Lock()

    # ---- 登记扩展：保留计划时区（不属于列表字段，但判定窗口必须用到） ----

    def create_entry(self, values: dict[str, Any]):
        entry, missing = super().create_entry(values)
        if entry is not None:
            for field in OPTIONAL_FIELDS:
                value = values.get(field)
                if value is not None and str(value).strip():
                    entry[field] = str(value).strip()
        return entry, missing

    # ---- 到期判定与发布 ----

    def preview_due_plans(
        self,
        *,
        at: datetime | str | None = None,
        timezone_name: str | None = None,
        mode: str | None = None,
        plan: str | None = None,
        plan_timezone: str | None = None,
        window: str | None = None,
        include_inactive: bool = False,
    ) -> dict[str, Any]:
        """在工作台查看“本次调度时点”各计划的到期结果。

        只做只读判定，不改计划状态、不发消息、不生成任务。
        未到期计划原样返回等待状态和下一次触发时点。
        """
        scheduler_tz_name = timezone_name or settings.scheduler_timezone
        normalized_mode = schedule_window.normalize_mode(
            mode, settings.schedule_window_mode)
        rows = store.rows(self.MODULE)
        if not include_inactive:
            rows = schedule_window.active_plans(rows)

        decisions = schedule_window.evaluate_plans(
            rows,
            at=at,
            scheduler_timezone=scheduler_tz_name,
            mode=normalized_mode,
        )
        # 已经发布过的窗口在只读视图里标成“本窗口已触发”，
        # 避免工作台把到期计划误读成还要再排一次队。
        for decision in decisions:
            row = store.find(self.MODULE, int(decision["plan_id"] or 0))
            if (
                decision["due"]
                and row is not None
                and row.get("已触发凭证") == decision["credential"]
            ):
                decision["state"] = "本窗口已触发"
                decision["reason"] = (
                    f"窗口 {decision['occurrence']} 已发布，重试不再重复排队")
                decision["already_triggered"] = True
            if row is not None:
                decision["queue_watermark"] = row.get("队列水位")
                decision["queue_watermark_state"] = row.get("队列水位终态")
        decisions = self._filter_decisions(
            decisions, plan=plan, plan_timezone=plan_timezone, window=window)

        due = [item for item in decisions if item["due"]]
        waiting = [item for item in decisions if not item["due"]]
        return {
            "at": schedule_window.parse_at(
                at, schedule_window.resolve_timezone(scheduler_tz_name)).isoformat(),
            "timezone": schedule_window.timezone_name(
                schedule_window.resolve_timezone(scheduler_tz_name)),
            "mode": normalized_mode,
            "total": len(decisions),
            "due_count": len(due),
            "waiting_count": len(waiting),
            "plans": decisions,
            "due_plans": due,
            "waiting_plans": waiting,
        }

    def schedule_crawl_jobs(
        self,
        *,
        at: datetime | str | None = None,
        timezone_name: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        """把当前调度时点真正到期的计划发布为采集任务凭证。

        判定条件（调度器时点与计划窗口共同决定）：
        1. 计划状态为“已激活”；
        2. 当前时点已越过 cron 触发点；
        3. 当前时点仍在执行窗口内（未配置窗口时按短补触发窗口）。

        同一计划同一 cron 窗口只发布一次：窗口重试命中已发布凭证时
        记入 ``skipped``，不会重复进入任务队列。

        Returns:
            {"triggered": int, "plans": [...], "skipped": [...],
             "waiting": [...], "at": ..., "timezone": ..., "mode": ...}
        """
        scheduler_tz_name = timezone_name or settings.scheduler_timezone
        normalized_mode = schedule_window.normalize_mode(
            mode, settings.schedule_window_mode)
        scheduler_tz = schedule_window.resolve_timezone(scheduler_tz_name)
        moment = schedule_window.parse_at(at, scheduler_tz)
        at_text = moment.astimezone(scheduler_tz).isoformat()

        rows = store.rows(self.MODULE)
        active = schedule_window.active_plans(rows)
        decisions = schedule_window.evaluate_plans(
            active,
            at=moment,
            scheduler_timezone=scheduler_tz_name,
            mode=normalized_mode,
        )

        triggered: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        waiting: list[dict[str, Any]] = []
        invalid: list[dict[str, Any]] = []

        for decision in decisions:
            if not decision["due"]:
                waiting.append(decision)
                continue
            plan = store.find(self.MODULE, int(decision["plan_id"] or 0))
            if plan is None:
                invalid.append(decision)
                continue

            with self._dispatch_lock:
                last_token = plan.get("已触发凭证")
                if last_token == decision["credential"]:
                    decision["state"] = "本窗口已触发"
                    decision["reason"] = (
                        f"窗口 {decision['occurrence']} 已发布，重试不再重复排队")
                    decision["already_triggered"] = True
                    skipped.append(decision)
                    continue
                last_occurrence = plan.get("最近触发窗口")
                if last_occurrence:
                    try:
                        if (
                            schedule_window.parse_at(
                                decision["occurrence"], scheduler_tz)
                            <= schedule_window.parse_at(
                                last_occurrence, scheduler_tz)
                        ):
                            # 水位已经推进到更晚的窗口，旧窗口补触发不再发布。
                            decision["state"] = "旧窗口已跳过"
                            decision["reason"] = (
                                f"计划排队水位已推进到 {last_occurrence}，"
                                "旧窗口不重复发布")
                            skipped.append(decision)
                            continue
                    except schedule_window.ScheduleWindowError:
                        pass

                msg = store.channel.publish(
                    topic="rpa.crawl_scheduled",
                    source=self.MODULE,
                    payload={
                        "plan_id": plan.get("id"),
                        "plan_name": plan.get("计划名称"),
                        "bot_ref": plan.get("绑定流程"),
                        "cron": plan.get("cron表达式"),
                        "window": plan.get("执行窗口"),
                        "priority": plan.get("优先级"),
                        "timezone": decision["timezone"],
                        "scheduler_timezone": decision["scheduler_timezone"],
                        "plan_timezone": decision["plan_timezone"],
                        "mode": decision["mode"],
                        "occurrence": decision["occurrence"],
                        "window_start": decision["window_start"],
                        "window_end": decision["window_end"],
                        "credential": decision["credential"],
                        "token": decision["credential"],
                        "triggered_at": at_text,
                    },
                )
                plan["已触发凭证"] = decision["credential"]
                plan["最近触发时间"] = at_text
                plan["最近触发窗口"] = decision["occurrence"]
                plan["调度排队水位"] = at_text

                triggered.append({
                    "plan_id": plan.get("id"),
                    "plan_name": plan.get("计划名称"),
                    "credential": decision["credential"],
                    "occurrence": decision["occurrence"],
                    "window_start": decision["window_start"],
                    "window_end": decision["window_end"],
                    "msg_id": msg.msg_id,
                    "task_priority": self._task_priority(plan.get("优先级")),
                })

        return {
            "triggered": len(triggered),
            "plans": triggered,
            "skipped": skipped,
            "waiting": waiting,
            "invalid": invalid,
            "active_count": len(active),
            "at": at_text,
            "timezone": schedule_window.timezone_name(scheduler_tz),
            "mode": normalized_mode,
            "watermark": at_text,
        }

    @staticmethod
    def _task_priority(value: Any) -> str:
        text = str(value or "").strip()
        return text if text in _TASK_PRIORITIES else "中"

    @staticmethod
    def _filter_decisions(
        decisions: list[dict[str, Any]],
        *,
        plan: str | None = None,
        plan_timezone: str | None = None,
        window: str | None = None,
    ) -> list[dict[str, Any]]:
        filtered = decisions
        if plan:
            needle = plan.strip().casefold()
            filtered = [
                item for item in filtered
                if needle in str(item.get("plan_id", "")).casefold()
                or needle in str(item.get("plan_name", "")).casefold()
            ]
        if plan_timezone:
            needle = plan_timezone.strip().casefold()
            filtered = [
                item for item in filtered
                if needle in str(item.get("timezone", "")).casefold()
                or needle in str(item.get("plan_timezone", "")).casefold()
            ]
        if window:
            needle = window.strip().casefold()
            filtered = [
                item for item in filtered
                if needle in str(item.get("window", "")).casefold()
                or needle in str(item.get("window_kind", "")).casefold()
                or needle in str(item.get("state", "")).casefold()
            ]
        return filtered

    # ---- 计划时区维护 ----

    def set_plan_timezone(self, plan_id: int, timezone_name: str) -> dict[str, Any] | None:
        """为计划冻结时区。统一时区分支不依赖它，逐条分支按它判断。"""
        entry = store.find(self.MODULE, plan_id)
        if entry is None:
            return None
        resolved = schedule_window.resolve_timezone(timezone_name)
        entry["时区"] = schedule_window.timezone_name(resolved)
        return {"plan_id": plan_id, "timezone": entry["时区"]}

    def list_active_plans(self) -> list[dict[str, Any]]:
        """列出已激活的调度计划。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"), "name": r.get("计划名称"),
                "cron": r.get("cron表达式"), "bot": r.get("绑定流程"),
                "window": r.get("执行窗口"), "concurrency": r.get("并发数"),
                "priority": r.get("优先级"), "timezone": r.get("时区"),
                "last_credential": r.get("已触发凭证"),
            }
            for r in rows if r.get("status") == "已激活"
        ]

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        rows = store.rows(self.MODULE)
        return {
            **base,
            "active_plans": sum(1 for r in rows if r.get("status") == "已激活"),
            "paused_plans": sum(1 for r in rows if r.get("status") == "已暂停"),
            "archived_plans": sum(1 for r in rows if r.get("status") == "已归档"),
            "scheduler_timezone": settings.scheduler_timezone,
            "window_mode": settings.schedule_window_mode,
            "dispatched_windows": sum(1 for r in rows if r.get("已触发凭证")),
        }

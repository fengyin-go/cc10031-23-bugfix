"""任务队列领域服务 — 消费编排结果与调度凭证生成执行任务。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物

数据流:
    channel("rpa.steps_submitted") ← step_editor
    channel("rpa.crawl_scheduled") ← scheduler
    → task_queue 消费 → 生成排队任务 + execution_log 记录

调度到队列的处理口径（与其他入口共用一条处理路径）:
1. 凭证必须能对应到一条已激活的采集计划；
2. 用 ``schedule_window.evaluate_plan`` 复用调度器同一套判定，
   只有“当前调度时点 + 计划 cron 窗口”判定通过、且凭证与本次窗口
   一致时才创建任务；
3. 同一计划同一窗口（触发凭证）只入队一次，重复投递 / 并发提交
   命中既有任务或计划队列水位时按“已存在”处理，不新增排队任务；
4. 处理列表与结果清单在同一次处理内一起更新，状态沿用
   ``STATUS_ORDER[0] = 排队中`` 的既有规则。
"""
from __future__ import annotations

import threading
from datetime import datetime, timezone
from typing import Any

from app.config import settings
from app.services import schedule_window
from app.services.base import BaseModuleService
from app.store import store

TASK_STATUS_QUEUED = "排队中"
TASK_PRIORITIES = ("高", "中", "低")


class _ProcessingState:
    """进程级处理台账。

    服务实例由路由按请求创建，处理台账不能跟着实例走，
    否则调度工作台读不到上一次处理结果，队列水位也会被重置。
    """

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.processing_items: list[dict[str, Any]] = []
        self.results: list[dict[str, Any]] = []
        self.watermarks: dict[int, dict[str, Any]] = {}

    def reset(self) -> None:
        with self.lock:
            self.processing_items = []
            self.results = []
            self.watermarks = {}


_PROCESSING = _ProcessingState()


class _IngestCollector:
    """单次处理过程的结果明细：处理列表与结果清单同步记录。"""

    def __init__(self) -> None:
        self.processing_list: list[dict[str, Any]] = []
        self.result_list: list[dict[str, Any]] = []

    def accepted(self, item: dict[str, Any]) -> None:
        self.processing_list.append(dict(item))
        self.result_list.append({
            **item,
            "outcome": "已入队",
            "result_state": "入队成功",
            "reason": "窗口判定通过，生成排队任务",
        })

    def rejected(self, item: dict[str, Any], outcome: str, reason: str) -> None:
        self.result_list.append({
            **item,
            "outcome": outcome,
            "result_state": outcome,
            "reason": reason,
        })

    def duplicate(self, existing: dict[str, Any] | None,
                  item: dict[str, Any], reason: str) -> None:
        task_id = item.get("task_id")
        task_number = item.get("task_number")
        if existing is not None:
            task_id = existing.get("id")
            task_number = existing.get("任务编号")
        entry = {
            "task_id": task_id,
            "task_number": task_number,
            "flow_name": item.get("flow_name"),
            "priority": item.get("priority"),
            "status": existing.get("status") if existing else None,
            "trigger_key": item.get("trigger_key"),
            **{k: v for k, v in item.items()
               if k not in {"task_id", "task_number"}},
        }
        self.processing_list.append(dict(entry))
        self.result_list.append({
            **entry,
            "outcome": "重复提交",
            "result_state": "已存在",
            "reason": reason,
        })

    def payload(self) -> dict[str, Any]:
        accepted = sum(
            1 for item in self.result_list if item.get("outcome") == "已入队")
        duplicated = sum(
            1 for item in self.result_list if item.get("outcome") == "重复提交")
        return {
            "generated": accepted,
            "duplicated": duplicated,
            "rejected": len(self.result_list) - accepted - duplicated,
            "processed": len(self.result_list),
            "task_ids": [
                item["task_id"] for item in self.result_list
                if item.get("outcome") == "已入队"
            ],
            "processing_list": [dict(item) for item in self.processing_list],
            "result_list": [dict(item) for item in self.result_list],
        }


class TaskQueueService(BaseModuleService):
    MODULE = "task_queue"
    ENTITY = "排队任务"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["任务编号", "流程名称", "提交时间", "优先级", "预计执行", "排队时长", "当前状态"]
    REQUIRED_FIELDS = ["任务编号", "流程名称", "提交时间"]
    STATUS_ORDER = ["排队中", "执行中", "已完成", "已取消", "执行失败"]
    ACTION_RULES = {"取消任务": "已取消", "提升优先级": "排队中"}
    NEGATIVE_ACTIONS = []

    # 所有入队路径共用一把锁：并发重复提交只允许一个请求真正落任务。
    _ingest_lock = threading.Lock()

    # ---- 共用处理路径 ----

    def _next_task_number(self) -> str:
        rows = store.rows(self.MODULE)
        next_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        return f"TASK-{next_id:04d}"

    def _existing_task(self, trigger_key: str) -> dict[str, Any] | None:
        for row in store.rows(self.MODULE):
            if row.get("触发凭证") == trigger_key:
                return row
        return None

    def _create_task_entry(
        self,
        *,
        flow_name: str,
        priority: str,
        expected: str,
        submitted_at: str,
        extra: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, str]:
        """按既有状态规则登记一条排队任务（调用方需持有入队锁）。"""
        values = {
            "任务编号": self._next_task_number(),
            "流程名称": flow_name,
            "提交时间": submitted_at,
            "优先级": priority,
            "预计执行": expected,
            "排队时长": "0s",
            "当前状态": TASK_STATUS_QUEUED,
        }
        entry, missing = self.create_entry(values)
        if entry is None:
            return None, f"缺少必填字段：{'、'.join(missing)}"
        entry.update(extra)
        return entry, ""

    def _enqueue(
        self,
        collector: _IngestCollector,
        *,
        trigger_key: str,
        flow_name: str,
        priority: str,
        expected: str,
        submitted_at: str,
        extra: dict[str, Any],
        context: dict[str, Any],
    ) -> str:
        """唯一入队实现：锁内查重 + 建任务 + 同步处理列表与结果清单。

        Returns:
            "已入队" / "重复提交" / "入队失败"
        """
        item = {
            "task_id": None,
            "task_number": None,
            "flow_name": flow_name,
            "priority": priority,
            "trigger_key": trigger_key,
            **context,
        }
        with self._ingest_lock:
            existing = self._existing_task(trigger_key)
            if existing is not None:
                collector.duplicate(
                    existing, item,
                    "同一窗口任务已存在，重复提交只生效一次")
                return "重复提交"

            entry, error = self._create_task_entry(
                flow_name=flow_name,
                priority=priority,
                expected=expected,
                submitted_at=submitted_at,
                extra={**extra, "触发凭证": trigger_key},
            )
            if entry is None:
                collector.rejected(item, "入队失败", error)
                return "入队失败"

            collector.accepted({
                **item,
                "task_id": entry["id"],
                "task_number": entry.get("任务编号"),
                "status": entry.get("status"),
            })
            return "已入队"

    def _record_rejection(self, collector: _IngestCollector, *,
                          trigger_key: str | None, outcome: str,
                          reason: str, context: dict[str, Any]) -> None:
        """拒绝项只进结果清单，不生成排队任务。"""
        collector.rejected({
            "task_id": None,
            "task_number": None,
            "trigger_key": trigger_key,
            **context,
        }, outcome, reason)

    def reset_processing(self) -> None:
        _PROCESSING.reset()

    def _commit(self, collector: _IngestCollector) -> None:
        with _PROCESSING.lock:
            _PROCESSING.processing_items.extend(collector.processing_list)
            _PROCESSING.results.extend(collector.result_list)

    def processing_snapshot(self) -> dict[str, Any]:
        with _PROCESSING.lock:
            return {
                "processing_count": len(_PROCESSING.processing_items),
                "result_count": len(_PROCESSING.results),
                "processing_list": [dict(item) for item in _PROCESSING.processing_items],
                "result_list": [dict(item) for item in _PROCESSING.results],
                "watermarks": {
                    str(k): {**v,
                             "occurrence_dt": v["occurrence_dt"].isoformat()
                             if hasattr(v.get("occurrence_dt"), "isoformat")
                             else v.get("occurrence_dt")}
                    for k, v in _PROCESSING.watermarks.items()
                },
            }

    # ---- 状态流转：沿用既有动作规则，同时同步处理清单与排队水位 ----

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """任务动作统一走 ``update_queue_status``，避免列表和工作台状态错位。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"

        if action == "提升优先级":
            # 提升优先级不改状态，只抬高优先级并重排队。
            entry["优先级"] = "高"
            entry["status"] = target
            entry["pending"] = True
            entry["abnormal"] = False
            self._sync_processing_entry(entry)
            return entry, f"{self.ENTITY}已{action}"

        updated = self.update_queue_status(entry_id, target)
        if updated is None:
            return None, f"{self.ENTITY} {entry_id} 状态无法流转到{target}"
        return entry, f"{self.ENTITY}已{action}"

    def _sync_processing_entry(self, entry: dict[str, Any]) -> None:
        """把任务最新状态同步进处理列表和结果清单。"""
        credential = entry.get("触发凭证")
        if not credential:
            return
        with _PROCESSING.lock:
            for item in _PROCESSING.processing_items:
                if item.get("task_id") == entry.get("id"):
                    item["status"] = entry.get("status")
            for item in _PROCESSING.results:
                if item.get("task_id") == entry.get("id"):
                    item["status"] = entry.get("status")
                    item["result_state"] = entry.get("status")
                    item["reason"] = f"任务状态更新为{entry.get('status')}"

    # ---- 跨模块业务方法 ----

    def consume_steps_to_queue(self) -> dict[str, Any]:
        """消费 step_editor 提交的编排结果，生成任务队列条目。

        与调度入口共用 ``_enqueue``：每个 step 以 step_id 作为触发凭证，
        重复消费不会重复建任务。

        Returns:
            {"generated": int, "plans_consumed": int, "task_ids": [...]}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.steps_submitted",
        )
        collector = _IngestCollector()
        if not msgs:
            return {
                **collector.payload(),
                "plans_consumed": 0,
            }

        consumed = 0
        for msg in msgs:
            plan = msg.payload
            bot_id = plan.get("bot_id", 0)
            steps = plan.get("steps", [])
            if not steps:
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue
            consumed += 1

            for i, step in enumerate(steps):
                seq = i + 1
                step_id = step.get("step_id") or f"{msg.msg_id}-{seq}"
                self._enqueue(
                    collector,
                    trigger_key=f"step:{step_id}",
                    flow_name=f"Bot-{bot_id}",
                    priority="高" if seq <= 3 else "中",
                    expected=step.get("name", f"Step-{seq}"),
                    submitted_at=datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"),
                    extra={
                        "step_id": step.get("step_id"),
                        "bot_id": bot_id,
                        "step_type": step.get("type", ""),
                    },
                    context={
                        "source": "rpa.steps_submitted",
                        "step_id": step.get("step_id"),
                        "bot_id": bot_id,
                    },
                )

            store.channel.confirm(msg.msg_id, self.MODULE)

        self._commit(collector)
        return {
            **collector.payload(),
            "plans_consumed": consumed,
        }

    def consume_scheduled_crawl_jobs(
        self,
        *,
        at: datetime | str | None = None,
        timezone_name: str | None = None,
        mode: str | None = None,
    ) -> dict[str, Any]:
        """消费 scheduler 发布的调度凭证，只为窗口判定通过的凭证建任务。

        每个凭证都会用调度器同一套 ``evaluate_plan`` 重新判定：
        当前时点必须与凭证所属 cron 窗口一致，且仍在该计划的执行窗口内。
        未到窗口、已过窗口、凭证不匹配的投递只记入结果清单，不进入队列。
        """
        scheduler_tz_name = timezone_name or settings.scheduler_timezone
        scheduler_tz = schedule_window.resolve_timezone(scheduler_tz_name)
        moment = schedule_window.parse_at(at, scheduler_tz)

        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.crawl_scheduled",
        )
        collector = _IngestCollector()
        for msg in sorted(msgs, key=lambda item: item.msg_id):
            self._ingest_scheduled_credential(
                collector,
                msg.payload,
                at=moment,
                scheduler_tz_name=scheduler_tz_name,
                mode=mode,
            )
            store.channel.confirm(msg.msg_id, self.MODULE)

        self._commit(collector)
        return {
            **collector.payload(),
            "consumed": len(msgs),
            "timezone": schedule_window.timezone_name(scheduler_tz),
            "at": moment.astimezone(scheduler_tz).isoformat(),
        }

    def _ingest_scheduled_credential(
        self,
        collector: _IngestCollector,
        payload: dict[str, Any],
        *,
        at: datetime,
        scheduler_tz_name: str,
        mode: str | None,
    ) -> str:
        plan_id_raw = payload.get("plan_id")
        try:
            plan_id = int(plan_id_raw)
        except (TypeError, ValueError):
            self._record_rejection(
                collector,
                trigger_key=payload.get("credential") or payload.get("token"),
                outcome="计划无效",
                reason=f"调度凭证缺少有效计划标识：{plan_id_raw!r}",
                context={"source": "rpa.crawl_scheduled"},
            )
            return "计划无效"

        plan = store.find("scheduler", plan_id)
        credential = payload.get("credential") or payload.get("token")
        context = {
            "source": "rpa.crawl_scheduled",
            "plan_id": plan_id,
            "plan_name": payload.get("plan_name"),
            "credential": credential,
            "occurrence": payload.get("occurrence"),
            "dispatch_timezone": payload.get("timezone"),
            "dispatch_mode": payload.get("mode"),
        }

        if plan is None:
            self._record_rejection(
                collector, trigger_key=credential,
                outcome="计划不存在",
                reason=f"采集计划 {plan_id} 不存在或已归档",
                context=context,
            )
            return "计划不存在"

        if not credential:
            self._record_rejection(
                collector, trigger_key=None,
                outcome="缺少触发凭证",
                reason="调度凭证缺少 credential，无法判定窗口",
                context=context,
            )
            return "缺少触发凭证"

        valid, reason = schedule_window.validate_credential(
            plan,
            payload.get("occurrence"),
            credential,
            scheduler_timezone=scheduler_tz_name,
            mode=mode,
        )
        if not valid:
            self._record_rejection(
                collector, trigger_key=credential,
                outcome="凭证无效", reason=reason, context=context,
            )
            return "凭证无效"

        decision = schedule_window.evaluate_plan(
            plan,
            at=at,
            scheduler_timezone=scheduler_tz_name,
            mode=payload.get("mode") or mode,
        )
        context.update({
            "timezone": decision["timezone"],
            "mode": decision["mode"],
            "window_start": decision["window_start"],
            "window_end": decision["window_end"],
            "evaluated_at": decision["evaluated_at"],
        })

        if not decision["due"]:
            self._record_rejection(
                collector, trigger_key=credential,
                outcome="窗口未通过",
                reason=decision["reason"] or "执行窗口判定未通过",
                context=context,
            )
            return "窗口未通过"

        if decision["credential"] != credential:
            self._record_rejection(
                collector, trigger_key=credential,
                outcome="窗口不匹配",
                reason=(
                    f"凭证窗口 {payload.get('occurrence')} 与当前到期窗口 "
                    f"{decision['occurrence']} 不一致，未到期计划不能进入队列"
                ),
                context=context,
            )
            return "窗口不匹配"

        watermark = _PROCESSING.watermarks.get(plan_id)
        occurrence = schedule_window.parse_at(decision["occurrence"], at.tzinfo)
        if watermark and occurrence <= watermark["occurrence_dt"]:
            existing = self._existing_task(credential)
            collector.duplicate(
                existing,
                {
                    "task_id": None,
                    "task_number": None,
                    "flow_name": plan.get("绑定流程"),
                    "priority": payload.get("priority"),
                    "trigger_key": credential,
                    **context,
                },
                f"计划队列水位已推进到 {watermark['occurrence']}，旧窗口不重复排队",
            )
            return "重复提交"

        priority = payload.get("priority")
        if priority not in TASK_PRIORITIES:
            priority = "中"

        outcome = self._enqueue(
            collector,
            trigger_key=credential,
            flow_name=str(plan.get("绑定流程") or payload.get("plan_name") or ""),
            priority=priority,
            expected=str(decision["occurrence"]),
            submitted_at=str(decision["occurrence"]),
            extra={
                "所属计划": plan.get("计划名称"),
                "计划ID": plan.get("id"),
                "触发窗口": decision["occurrence"],
                "窗口结束": decision["window_end"],
                "执行时区": decision["timezone"],
                "调度模式": decision["mode"],
            },
            context=context,
        )
        if outcome != "已入队":
            return outcome

        entry = self._existing_task(credential)
        task_id = entry.get("id") if entry else None
        _PROCESSING.watermarks[plan_id] = {
            "occurrence": decision["occurrence"],
            "occurrence_dt": occurrence,
            "credential": credential,
            "task_id": task_id,
            "task_number": entry.get("任务编号") if entry else None,
            "timezone": decision["timezone"],
            "mode": decision["mode"],
            "state": "已受理",
            "updated_at": at.isoformat(),
        }
        plan["队列水位"] = decision["occurrence"]
        plan["队列水位凭证"] = credential
        plan["队列水位终态"] = "已受理"
        plan["队列水位更新时间"] = at.isoformat()
        return "已入队"

    def update_queue_status(self, task_id: int,
                             status: str, result: dict[str, Any] | None = None) -> dict[str, Any] | None:
        """更新任务状态并在执行完成时写 execution_log。

        Args:
            task_id: 任务 ID
            status: 目标状态（"执行中", "已完成", "执行失败"）
            result: 执行结果数据（可选）

        Returns:
            更新后的任务信息或 None
        """
        entry = store.find(self.MODULE, task_id)
        if entry is None:
            return None

        valid_statuses = {"执行中", "已完成", "已取消", "执行失败"}
        if status not in valid_statuses:
            return None

        entry["status"] = status
        entry["pending"] = status not in ("已完成", "已取消")
        entry["abnormal"] = status == "执行失败"

        if result:
            entry["result"] = result

        # 队列水位同步到任务终态：调度侧可据此看到处理终态。
        plan_id = entry.get("计划ID")
        if plan_id is not None:
            watermark = _PROCESSING.watermarks.get(int(plan_id))
            if watermark and watermark.get("credential") == entry.get("触发凭证"):
                watermark["state"] = status
                watermark["updated_at"] = datetime.now(timezone.utc).isoformat()
                plan = store.find("scheduler", int(plan_id))
                if plan is not None:
                    plan["队列水位终态"] = status
                    plan["队列水位更新时间"] = watermark["updated_at"]

        self._sync_processing_entry(entry)

        # 完成后写入执行日志通道
        if status in ("已完成", "执行失败"):
            exec_payload = {
                "task_id": task_id,
                "task_number": entry.get("任务编号"),
                "final_status": status,
                "processed_by": self.MODULE,
                "result": result or {},
            }
            store.channel.publish(
                topic="rpa.task_completed",
                source=self.MODULE,
                payload=exec_payload,
            )

        return {
            "task_id": task_id,
            "status": status,
            "task_number": entry.get("任务编号"),
        }

    def schedule_next_batch(self, max_tasks: int = 5) -> dict[str, Any]:
        """从排队中取下一批任务标记为执行中。

        模拟调度器的取任务逻辑 — 按优先级 + 提交时间排序。

        Returns:
            {"scheduled": int, "task_ids": [...]}
        """
        rows = store.rows(self.MODULE)
        queued = [
            r for r in rows
            if r.get("status") == "排队中" and not r.get("abnormal")
        ]
        # 按优先级排序（高 > 中 > 低），同优先级按提交时间先后。
        priority_order = {"高": 0, "中": 1, "低": 2}
        queued.sort(key=lambda r: (
            priority_order.get(r.get("优先级", "低"), 99),
            str(r.get("提交时间", "")),
            int(r.get("id", 0)),
        ))

        scheduled = []
        for task in queued[:max_tasks]:
            task["status"] = "执行中"
            task["pending"] = True
            task["开始执行时间"] = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S")
            scheduled.append(task["id"])
            # 模拟执行记录
            store.channel.publish(
                topic="rpa.task_started",
                source=self.MODULE,
                payload={
                    "task_id": task["id"],
                    "task_number": task.get("任务编号"),
                    "start_time": task["开始执行时间"],
                },
            )

        return {"scheduled": len(scheduled), "task_ids": scheduled}

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        # 通道消费统计
        consumed = store.channel.snapshot(topic="rpa.steps_submitted")
        completed = store.channel.snapshot(topic="rpa.task_completed")
        scheduled = store.channel.snapshot(topic="rpa.crawl_scheduled")
        with _PROCESSING.lock:
            processing_count = len(_PROCESSING.processing_items)
            result_count = len(_PROCESSING.results)
            watermark_count = len(_PROCESSING.watermarks)
        return {
            **base,
            "steps_plans_consumed": len(consumed),
            "tasks_completed": len(completed),
            "crawl_credentials_received": len(scheduled),
            "queued_tasks": sum(
                1 for row in store.rows(self.MODULE)
                if row.get("status") == TASK_STATUS_QUEUED),
            "processing_count": processing_count,
            "result_count": result_count,
            "schedule_watermarks": watermark_count,
        }

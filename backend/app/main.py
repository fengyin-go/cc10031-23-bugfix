"""RPA流程自动化中心 后端服务入口。

启动：uvicorn app.main:app --host 127.0.0.1 --port 8000
健康检查：GET /api/health
"""
from __future__ import annotations

from fastapi import Body, FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.routers import ROUTERS
from app.store import store
from app.services import runtime_session, callback_hook
from app.services.scheduler import SchedulerService
from app.services.task_queue import TaskQueueService

app = FastAPI(title="RPA流程自动化中心", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/api/health")
def health() -> dict[str, object]:
    """健康检查：确认服务已经监听、示例数据已经就绪。"""
    return {"ok": True, "app": settings.app_name, "modules": len(store.module_names())}


@app.get("/api/overview")
def overview() -> dict[str, object]:
    """运营概览：把各业务模块的待处理量汇总成看板卡片。"""
    return store.overview()


# ---- 跨模块数据流 API ----

@app.get("/api/nodestats")
def nodestats(module: str | None = Query(default=None)) -> dict[str, object]:
    """节点统计：某一模块或全平台的领域快照。

    用于跨模块数据消费方查看各领域数据质量状态。
    """
    from app.services import SERVICES, service_for

    if module:
        svc = service_for(module)
        if svc is None:
            return {"error": f"未知模块: {module}"}
        return svc.domain_snapshot()

    # 全平台聚合快照
    snapshots = {}
    for key, svc in SERVICES.items():
        snapshots[key] = svc.domain_snapshot()

    # 附加非模块服务
    snapshots["runtime_session"] = runtime_session.domain_snapshot()
    snapshots["callback_hook"] = callback_hook.domain_snapshot()

    # 存储基础设施概览
    infra = {
        "channel": {
            "total_messages": store.channel.total_messages,
            "unconfirmed": store.channel.uncomfirmed_count(),
            "topics": list(set(
                m["topic"] for m in store.channel.snapshot()
            )),
        },
        "dirty_vault": store.dirty_vault.snapshot(),
        "collect_records": store.collect_records.snapshot(),
        "recovery": store.recovery.snapshot(),
    }

    return {"modules": snapshots, "infrastructure": infra}


@app.get("/api/channel/{topic}")
def channel_snapshot(topic: str) -> dict[str, object]:
    """获取指定 topic 的通道消息快照。"""
    msgs = store.channel.snapshot(topic=topic)
    latest = store.channel.latest(topic)
    return {"topic": topic, "count": len(msgs), "messages": msgs, "latest": latest}


@app.get("/api/dirty-vault")
def dirty_vault(source: str | None = Query(default=None)) -> dict[str, object]:
    """脏数据隔离区查询。"""
    if source:
        records = store.dirty_vault.list_by_source(source)
    else:
        return store.dirty_vault.snapshot()
    return {
        "source": source,
        "count": len(records),
        "records": [
            {"id": r.record_id, "batch": r.batch_ref, "reason": r.reason,
             "raw": r.raw_data, "detected_at": r.detected_at}
            for r in records
        ],
    }


@app.get("/api/collect-records")
def collect_records(batch_ref: str | None = Query(default=None),
                     source: str | None = Query(default=None)) -> dict[str, object]:
    """采集记录查询。"""
    if batch_ref:
        records = store.collect_records.list_by_batch(batch_ref)
    elif source:
        records = store.collect_records.list_by_source(source)
    else:
        return store.collect_records.snapshot()
    return {
        "count": len(records),
        "records": [
            {"id": e.entry_id, "batch": e.batch_ref, "fields": e.fields,
             "validated": e.validated, "score": e.quality_score}
            for e in records
        ],
    }


@app.get("/api/recovery")
def recovery_stats() -> dict[str, object]:
    """失败恢复状态查询。"""
    return store.recovery.snapshot()


@app.get("/api/runtime-sessions")
def runtime_sessions() -> dict[str, object]:
    """运行时会话列表。"""
    sessions = runtime_session.list_sessions()
    return {"count": len(sessions), "sessions": sessions}


# ---- 跨模块数据流编排入口 ----

@app.post("/api/pipeline/rpa-lifecycle")
def pipeline_rpa_lifecycle() -> dict[str, object]:
    """执行 RPA 流程全生命周期数据流。

    模拟完整的端到端数据流：bot_definition → step_editor → task_queue → page_model → data_extract → result_verify → callback_hook
    """
    from app.services import (
        BotDefinitionService, StepEditorService, TaskQueueService,
        PageModelService, DataExtractService, ResultVerifyService,
    )

    results: dict[str, Any] = {}
    errors: list[str] = []

    # 1. 发布流程
    bot_svc = BotDefinitionService()
    active_bots = bot_svc.query_active_bots()
    publish_results = []
    for bot in active_bots:
        r = bot_svc.publish_bot(bot["id"])
        if r:
            publish_results.append(r)
    results["bot_published"] = len(publish_results)

    # 2. 编排步骤
    step_svc = StepEditorService()
    enrich = step_svc.enrich_steps_from_channel()
    results["steps_enriched"] = enrich["enriched"]

    # 3. 提交到队列
    task_svc = TaskQueueService()
    queue_result = task_svc.consume_steps_to_queue()
    results["tasks_queued"] = queue_result["generated"]

    # 4. 发布源端配置
    page_svc = PageModelService()
    active_models = page_svc.list_active_models()
    config_published = 0
    for model in active_models:
        r = page_svc.publish_source_config(model["model_id"])
        if r:
            config_published += 1
    # 如果没有active模型，尝试发布已验证模型
    if config_published == 0:
        models = store.rows("page_model")
        for m in models:
            if m.get("status") in ("草稿", "已验证"):
                r = page_svc.publish_source_config(m["id"])
                if r:
                    config_published += 1
                    break
    results["source_configs_published"] = config_published

    # 5. 执行数据采集
    extract_svc = DataExtractService()
    extract_result = extract_svc.consume_source_and_extract()
    results["data_extracted"] = extract_result

    # 6. 校验采集结果
    verify_svc = ResultVerifyService()
    verify_result = verify_svc.verify_collected_data()
    results["validation"] = verify_result

    # 7. 触发回调
    cb_result = callback_hook.consume_validation_and_callback()
    results["callbacks"] = cb_result

    # 8. 标记任务完成
    scheduled = task_svc.schedule_next_batch(max_tasks=3)
    results["tasks_scheduled"] = scheduled["scheduled"]

    return {"ok": True, "results": results, "errors": errors if errors else None}


@app.post("/api/pipeline/crawl-schedule")
def pipeline_crawl_schedule(
    at: str | None = Query(default=None, description="调度时点，缺省取当前时间"),
    timezone_name: str | None = Query(
        default=None, alias="timezone", description="调度时区，缺省取配置"),
    mode: str | None = Query(
        default=None, description="时区处理分支：scheduler 统一换算 / plan 按计划冻结时区"),
    consume: bool = Query(default=True, description="是否同时让任务队列消费到期凭证"),
) -> dict[str, object]:
    """触发调度采集流程：只发布当前窗口真正到期的计划，并由任务队列落任务。

    同一窗口重复调用不会重复排队；未到窗口的计划只出现在 waiting 中。
    """
    sched_svc = SchedulerService()
    result = sched_svc.schedule_crawl_jobs(
        at=at, timezone_name=timezone_name, mode=mode)

    queue_result: dict[str, object] | None = None
    if consume and result["triggered"] > 0:
        queue_svc = TaskQueueService()
        queue_result = queue_svc.consume_scheduled_crawl_jobs(
            at=at, timezone_name=timezone_name, mode=mode)

    return {"ok": True, "scheduled": result, "queue": queue_result}


@app.get("/api/scheduler-windows")
def scheduler_windows(
    at: str | None = Query(default=None, description="调度时点，缺省取当前时间"),
    timezone_name: str | None = Query(default=None, alias="timezone"),
    mode: str | None = Query(default=None, description="scheduler / plan"),
    plan: str | None = Query(default=None, description="按计划 ID 或名称过滤"),
    plan_timezone: str | None = Query(default=None, description="按计划时区过滤"),
    window: str | None = Query(default=None, description="按窗口或到期状态过滤"),
    include_inactive: bool = Query(default=False),
) -> dict[str, object]:
    """调度工作台：按计划、时区、窗口查看本次到期结果。

    只读判定，不推进水位、不生成任务；未到期计划保持等待状态。
    """
    sched_svc = SchedulerService()
    return sched_svc.preview_due_plans(
        at=at,
        timezone_name=timezone_name,
        mode=mode,
        plan=plan,
        plan_timezone=plan_timezone,
        window=window,
        include_inactive=include_inactive,
    )


@app.get("/api/task-queue/scheduled-processing")
def scheduled_processing() -> dict[str, object]:
    """任务队列工作台：处理项、结果清单与计划排队水位。"""
    return TaskQueueService().processing_snapshot()


@app.post("/api/task-queue/consume-scheduled")
def consume_scheduled(
    at: str | None = Query(default=None),
    timezone_name: str | None = Query(default=None, alias="timezone"),
    mode: str | None = Query(default=None),
) -> dict[str, object]:
    """任务队列单独消费调度凭证（用于重试场景）。"""
    queue_svc = TaskQueueService()
    result = queue_svc.consume_scheduled_crawl_jobs(
        at=at, timezone_name=timezone_name, mode=mode)
    return {"ok": True, "result": result}


@app.post("/api/scheduler/plan-timezone")
def set_plan_timezone(payload: dict = Body(...)) -> dict[str, object]:
    """为计划冻结时区，供“按计划冻结时区逐条判断”分支使用。"""
    plan_id = payload.get("plan_id")
    timezone_name = payload.get("timezone")
    if plan_id is None or not timezone_name:
        return {"ok": False, "message": "需要 plan_id 与 timezone"}
    result = SchedulerService().set_plan_timezone(int(plan_id), str(timezone_name))
    if result is None:
        return {"ok": False, "message": f"采集计划 {plan_id} 不存在"}
    return {"ok": True, "message": "计划时区已冻结", "entry": result}


@app.post("/api/pipeline/element-execution")
def pipeline_element_execution() -> dict[str, object]:
    """触发元素定位与自动化执行流程。"""
    result = runtime_session.consume_strategies_and_execute()
    return {"ok": True, "result": result}


# 资源路由含 /api/{module}，必须放在 health / overview 之后注册，
# 否则固定路径会被动态路径抢先匹配。
for module in ROUTERS:
    app.include_router(module.router)
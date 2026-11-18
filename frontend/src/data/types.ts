/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}

/** 调度工作台：单个计划在本次调度时点的到期判定结果 */
export type SchedulerPlanDecision = {
  plan_id: number | null
  plan_name: string | null
  bot_ref: string | null
  cron: string
  window: string | null
  priority: string | null
  timezone: string
  evaluation_timezone: string
  scheduler_timezone: string
  plan_timezone: string
  plan_timezone_frozen: boolean
  mode: 'scheduler' | 'plan'
  evaluated_at: string
  due: boolean
  state: string
  reason: string
  occurrence: string | null
  window_start: string | null
  window_end: string | null
  window_kind: string
  credential: string | null
  next_occurrence: string | null
  already_triggered?: boolean
  queue_watermark?: string | null
  queue_watermark_state?: string | null
}

export type SchedulerWindowResult = {
  at: string
  timezone: string
  mode: 'scheduler' | 'plan'
  total: number
  due_count: number
  waiting_count: number
  plans: SchedulerPlanDecision[]
  due_plans: SchedulerPlanDecision[]
  waiting_plans: SchedulerPlanDecision[]
}

export type ScheduledProcessingItem = {
  task_id: number | null
  task_number: string | null
  flow_name?: string
  priority?: string
  status?: string
  trigger_key?: string
  outcome?: string
  result_state?: string
  reason?: string
  plan_id?: number
  plan_name?: string
  occurrence?: string | null
  timezone?: string
  [key: string]: unknown
}

export type ScheduledProcessingSnapshot = {
  processing_count: number
  result_count: number
  processing_list: ScheduledProcessingItem[]
  result_list: ScheduledProcessingItem[]
  watermarks: Record<string, Record<string, unknown>>
}

export type CrawlScheduleResult = {
  ok: boolean
  scheduled: {
    triggered: number
    plans: ScheduledProcessingItem[]
    skipped: SchedulerPlanDecision[]
    waiting: SchedulerPlanDecision[]
    active_count: number
    at: string
    timezone: string
    mode: 'scheduler' | 'plan'
    watermark: string
  }
  queue: {
    generated: number
    duplicated: number
    rejected: number
    processed: number
    consumed: number
    at: string
    timezone: string
    processing_list: ScheduledProcessingItem[]
    result_list: ScheduledProcessingItem[]
  } | null
}

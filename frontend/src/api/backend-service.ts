import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  CrawlScheduleResult,
  EntryRow,
  OverviewResult,
  PageResult,
  ScheduledProcessingSnapshot,
  SchedulerWindowResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

/** 跨模块数据流：获取全平台领域快照 */
export async function loadNodeStats(module?: string): Promise<Record<string, unknown>> {
  const query = module ? `?module=${module}` : ''
  const response = await request(`/api/nodestats${query}`)
  if (!response.ok) throw new Error('节点统计读取失败')
  return await response.json()
}

/** 跨模块数据流：获取指定 topic 的通道消息 */
export async function loadChannelSnapshot(topic: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/channel/${topic}`)
  if (!response.ok) throw new Error('通道快照读取失败')
  return await response.json()
}

/** 跨模块数据流：查询脏数据隔离区 */
export async function loadDirtyVault(source?: string): Promise<Record<string, unknown>> {
  const query = source ? `?source=${source}` : ''
  const response = await request(`/api/dirty-vault${query}`)
  if (!response.ok) throw new Error('脏数据查询失败')
  return await response.json()
}

/** 跨模块数据流：查询采集记录 */
export async function loadCollectRecords(batchRef?: string): Promise<Record<string, unknown>> {
  const query = batchRef ? `?batch_ref=${batchRef}` : ''
  const response = await request(`/api/collect-records${query}`)
  if (!response.ok) throw new Error('采集记录查询失败')
  return await response.json()
}

/** 跨模块数据流：执行RPA全生命周期管道 */
export async function triggerPipeline(lifecycle: string): Promise<Record<string, unknown>> {
  const response = await request(`/api/pipeline/${lifecycle}`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('管道执行失败')
  return await response.json()
}

/** 跨模块数据流：获取失败恢复状态 */
export async function loadRecoveryStats(): Promise<Record<string, unknown>> {
  const response = await request('/api/recovery')
  if (!response.ok) throw new Error('恢复状态查询失败')
  return await response.json()
}

/** 调度工作台：按计划、时区、窗口查看本次到期结果 */
export async function loadSchedulerWindows(
  params: Record<string, string> = {},
): Promise<SchedulerWindowResult> {
  const query = new URLSearchParams(params).toString()
  const response = await request(`/api/scheduler-windows${query ? `?${query}` : ''}`)
  if (!response.ok) throw new Error('调度窗口读取失败')
  return await response.json()
}

/** 调度工作台：触发一次到期计划发布，并让任务队列消费凭证 */
export async function runCrawlSchedule(
  params: Record<string, string> = {},
): Promise<CrawlScheduleResult> {
  const query = new URLSearchParams(params).toString()
  const response = await request(
    `/api/pipeline/crawl-schedule${query ? `?${query}` : ''}`,
    { method: 'POST' },
  )
  if (!response.ok) throw new Error('调度触发失败')
  return await response.json()
}

/** 任务队列工作台：处理项、结果清单与计划排队水位 */
export async function loadScheduledProcessing(): Promise<ScheduledProcessingSnapshot> {
  const response = await request('/api/task-queue/scheduled-processing')
  if (!response.ok) throw new Error('处理清单读取失败')
  return await response.json()
}

/** 组合其他模块的待办同步：这里只读，不改其他模块状态 */
export async function refreshWorkbench(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  return listEntries(key, filters)
}

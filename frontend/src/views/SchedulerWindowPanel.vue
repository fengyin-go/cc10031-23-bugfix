<template>
  <section class="panel" data-testid="scheduler-window-panel">
    <header class="panel-head">
      <div>
        <h3>调度窗口到期结果</h3>
        <p class="panel-desc">
          调度时点与计划窗口共同决定到期；未到窗口的计划保持等待，不进入任务队列。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="run-crawl-schedule" type="button" :disabled="running" @click="dispatch">
          {{ running ? '触发中…' : '触发本次到期计划' }}
        </button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="reload">
      <label class="filter-item">
        <span>调度时点</span>
        <input v-model="filters.at" data-testid="window-at" placeholder="留空取当前时间" />
      </label>
      <label class="filter-item">
        <span>调度时区</span>
        <input v-model="filters.timezone" data-testid="window-timezone" placeholder="Asia/Shanghai" />
      </label>
      <label class="filter-item">
        <span>时区处理分支</span>
        <select v-model="filters.mode" data-testid="window-mode">
          <option value="">默认（调度时区统一换算）</option>
          <option value="scheduler">统一换算到调度时区</option>
          <option value="plan">按计划冻结时区逐条判断</option>
        </select>
      </label>
      <label class="filter-item">
        <span>计划</span>
        <input v-model="filters.plan" data-testid="window-plan" placeholder="计划名称或 ID" />
      </label>
      <label class="filter-item">
        <span>计划时区</span>
        <input v-model="filters.plan_timezone" data-testid="window-plan-timezone" placeholder="如 America/New_York" />
      </label>
      <label class="filter-item">
        <span>窗口 / 状态</span>
        <input v-model="filters.window" data-testid="window-window" placeholder="如 未到期、daily" />
      </label>
      <button class="btn" data-testid="window-query" type="submit">查看到期结果</button>
    </form>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">调度时点</span>
        <strong class="stat-value small">{{ result?.at ?? '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">调度时区 / 分支</span>
        <strong class="stat-value small">{{ result ? `${result.timezone} / ${result.mode}` : '—' }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">本次到期</span>
        <strong class="stat-value" data-testid="due-count">{{ result?.due_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">等待中</span>
        <strong class="stat-value" data-testid="waiting-count">{{ result?.waiting_count ?? 0 }}</strong>
      </article>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>计划</th>
          <th>cron</th>
          <th>执行窗口</th>
          <th>判定时区</th>
          <th>cron 窗口</th>
          <th>窗口范围</th>
          <th>本次结果</th>
          <th>说明</th>
          <th>触发凭证</th>
          <th>排队水位终态</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="plan in result?.plans ?? []"
          :key="String(plan.plan_id)"
          :data-testid="`window-row-${plan.plan_id}`"
          :data-state="plan.state"
        >
          <td>{{ plan.plan_name ?? '—' }}</td>
          <td>{{ plan.cron || '—' }}</td>
          <td>{{ plan.window ?? '默认窗口' }}</td>
          <td>{{ plan.timezone }}</td>
          <td>{{ plan.occurrence ?? '—' }}</td>
          <td>{{ plan.window_start ?? '—' }} ~ {{ plan.window_end ?? '—' }}</td>
          <td>
            <span :class="['state-pill', plan.due ? 'state-due' : 'state-waiting']">
              {{ plan.due ? '到期' : '未到期' }}
            </span>
          </td>
          <td>{{ plan.reason }}</td>
          <td class="mono">{{ plan.credential ?? '—' }}</td>
          <td>{{ plan.queue_watermark_state ?? '—' }}</td>
        </tr>
        <tr v-if="!result || !result.plans.length">
          <td colspan="10" class="empty-state">
            当前筛选条件下没有可判定的已激活计划
          </td>
        </tr>
      </tbody>
    </table>

    <p v-if="lastDispatch" class="dispatch-summary" data-testid="dispatch-summary">
      本次触发 {{ lastDispatch.scheduled.triggered }} 个到期计划；
      任务队列受理 {{ lastDispatch.queue?.generated ?? 0 }} 个，
      重复提交 {{ lastDispatch.queue?.duplicated ?? 0 }} 个，
      未进队列 {{ lastDispatch.queue?.rejected ?? 0 }} 个。
    </p>
    <p v-if="errorMessage" class="error-text" data-testid="window-error">{{ errorMessage }}</p>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { loadSchedulerWindows, runCrawlSchedule } from '@/api/backend-service'
import type { CrawlScheduleResult, SchedulerWindowResult } from '@/data/types'

const result = ref<SchedulerWindowResult | null>(null)
const lastDispatch = ref<CrawlScheduleResult | null>(null)
const running = ref(false)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({
  at: '',
  timezone: '',
  mode: '',
  plan: '',
  plan_timezone: '',
  window: '',
})

function activeFilters(): Record<string, string> {
  return Object.fromEntries(
    Object.entries(filters.value).filter(([, value]) => String(value).trim() !== ''),
  )
}

async function reload() {
  errorMessage.value = ''
  try {
    result.value = await loadSchedulerWindows(activeFilters())
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '调度窗口读取失败'
  }
}

async function dispatch() {
  running.value = true
  errorMessage.value = ''
  try {
    lastDispatch.value = await runCrawlSchedule(activeFilters())
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '调度触发失败'
  } finally {
    running.value = false
  }
}

onMounted(reload)
</script>

<template>
  <section class="panel" data-testid="scheduled-processing-panel">
    <header class="panel-head">
      <div>
        <h3>调度处理清单与排队水位</h3>
        <p class="panel-desc">
          只有窗口判定通过的触发凭证才创建排队任务；同一窗口重复提交只生效一次。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-processing" type="button" @click="reload">
          刷新处理清单
        </button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">处理项</span>
        <strong class="stat-value" data-testid="processing-count">{{ snapshot?.processing_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">结果清单</span>
        <strong class="stat-value" data-testid="result-count">{{ snapshot?.result_count ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">计划排队水位</span>
        <strong class="stat-value" data-testid="watermark-count">{{ watermarkRows.length }}</strong>
      </article>
    </div>

    <table class="data-table">
      <thead>
        <tr>
          <th>任务编号</th>
          <th>流程</th>
          <th>计划</th>
          <th>触发窗口</th>
          <th>时区 / 分支</th>
          <th>处理结果</th>
          <th>说明</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="(item, index) in snapshot?.result_list ?? []"
          :key="`${item.trigger_key}-${index}`"
          :data-testid="`result-row-${index}`"
          :data-outcome="String(item.outcome ?? '')"
        >
          <td class="mono">{{ item.task_number ?? '—' }}</td>
          <td>{{ item.flow_name ?? '—' }}</td>
          <td>{{ item.plan_name ?? '—' }}</td>
          <td>{{ item.occurrence ?? '—' }}</td>
          <td>{{ item.timezone ?? '—' }} / {{ item.mode ?? '—' }}</td>
          <td>
            <span :class="['state-pill', item.outcome === '已入队' ? 'state-due' : 'state-waiting']">
              {{ item.outcome ?? item.result_state ?? '—' }}
            </span>
          </td>
          <td>{{ item.reason ?? '—' }}</td>
        </tr>
        <tr v-if="!snapshot || !snapshot.result_list.length">
          <td colspan="7" class="empty-state">
            还没有调度处理记录，先到调度计划页触发本次到期计划
          </td>
        </tr>
      </tbody>
    </table>

    <h4 class="panel-subtitle">计划排队水位</h4>
    <table class="data-table">
      <thead>
        <tr>
          <th>计划 ID</th>
          <th>已排队窗口</th>
          <th>触发凭证</th>
          <th>任务编号</th>
          <th>时区 / 分支</th>
          <th>处理终态</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in watermarkRows" :key="row.planId" :data-testid="`watermark-${row.planId}`">
          <td>{{ row.planId }}</td>
          <td>{{ row.occurrence }}</td>
          <td class="mono">{{ row.credential }}</td>
          <td class="mono">{{ row.taskNumber ?? '—' }}</td>
          <td>{{ row.timezone }} / {{ row.mode }}</td>
          <td>{{ row.state }}</td>
        </tr>
        <tr v-if="!watermarkRows.length">
          <td colspan="6" class="empty-state">暂无排队水位</td>
        </tr>
      </tbody>
    </table>

    <p v-if="errorMessage" class="error-text" data-testid="processing-error">{{ errorMessage }}</p>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { loadScheduledProcessing } from '@/api/backend-service'
import type { ScheduledProcessingSnapshot } from '@/data/types'

const snapshot = ref<ScheduledProcessingSnapshot | null>(null)
const errorMessage = ref('')

const watermarkRows = computed(() =>
  Object.entries(snapshot.value?.watermarks ?? {}).map(([planId, value]) => ({
    planId,
    occurrence: String(value.occurrence ?? '—'),
    credential: String(value.credential ?? '—'),
    taskNumber: value.task_number ?? null,
    timezone: String(value.timezone ?? '—'),
    mode: String(value.mode ?? '—'),
    state: String(value.state ?? '—'),
  })),
)

async function reload() {
  errorMessage.value = ''
  try {
    snapshot.value = await loadScheduledProcessing()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '处理清单读取失败'
  }
}

onMounted(reload)
</script>

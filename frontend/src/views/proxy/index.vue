<template>
  <section class="page" data-module="proxy">
    <header class="page-head">
      <div>
        <h2>代理与请求控制管理</h2>
        <p class="page-desc">维护代理池的地址、并发控制与可用性检测；代理池按并发额度、失败次数、冷却截止时间选择与恢复。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" type="button" @click="openCreate">登记代理配置</button>
        <button class="btn" type="button" @click="exportRows">导出代理与请求控制清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <!-- 代理池工作台：额度、队列与分配历史随刷新实时更新 -->
    <section class="pool-board">
      <div class="board-head">
        <h3>代理池工作台</h3>
        <button class="btn primary" type="button" :disabled="refreshing" @click="refreshPool">
          {{ refreshing ? '刷新中…' : '刷新任务额度' }}
        </button>
      </div>
      <p v-if="poolMessage" class="board-message">{{ poolMessage }}</p>

      <h4 class="board-sub">代理额度（排序口径：剩余额度 → 失败次数 → 冷却截止时间）</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>代理编号</th>
            <th>配置状态</th>
            <th>是否可用</th>
            <th>并发上限</th>
            <th>在途占用</th>
            <th>剩余额度</th>
            <th>失败次数</th>
            <th>冷却截止时间</th>
            <th>池状态</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="item in pool.proxies" :key="String(item.id)">
            <td>{{ item['代理编号'] ?? '—' }}</td>
            <td>{{ item.status ?? '—' }}</td>
            <td>{{ item['是否可用'] ? '是' : '否' }}</td>
            <td>{{ item['并发上限'] ?? '—' }}</td>
            <td>{{ item['在途占用'] }}</td>
            <td>
              <strong :class="{ 'quota-warn': item['剩余额度'] === 0 }">{{ item['剩余额度'] }}</strong>
            </td>
            <td :class="{ 'fail-warn': item['失败次数'] > 0 }">{{ item['失败次数'] }}</td>
            <td>{{ formatTime(item['冷却截止时间']) }}</td>
            <td>
              <span class="state-tag" :data-state="item['池状态']">{{ item['池状态'] }}</span>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="pool-actions">
        <label class="filter-item">
          <span>请求标识（相同标识重复申请即恢复，幂等不多占额度）</span>
          <input v-model="requestKey" placeholder="例如 REQ-20261112-001" />
        </label>
        <label class="filter-item">
          <span>关联任务</span>
          <input v-model="taskRef" placeholder="例如 TASK-0008" />
        </label>
        <button class="btn primary" type="button" @click="acquire">申请额度 / 恢复排队</button>
        <button class="btn" type="button" @click="interrupt">任务中断（保留排队位置）</button>
        <label class="filter-item">
          <span>释放结果</span>
          <select v-model="outcome">
            <option value="success">成功</option>
            <option value="error">异常</option>
            <option value="timeout">超时</option>
            <option value="retry_exhausted">重试耗尽</option>
            <option value="null">空值</option>
          </select>
        </label>
        <label class="filter-item">
          <span>已重试次数（重试耗尽时留档）</span>
          <input v-model.number="retryCount" type="number" min="0" step="1" style="width: 90px" />
        </label>
        <button class="btn" type="button" @click="release">归还额度（失败不占额度）</button>
      </div>

      <h4 class="board-sub">等待队列（FIFO，中断请求保留位置且不允许后序请求插队）</h4>
      <table class="data-table">
        <thead>
          <tr><th>位置</th><th>请求标识</th><th>关联任务</th><th>是否中断</th><th>排队时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="item in pool.queue" :key="item.request_key">
            <td>{{ item.position }}</td>
            <td>{{ item.request_key }}</td>
            <td>{{ item['关联任务'] ?? '—' }}</td>
            <td>
              <span :class="item.interrupted ? 'fail-warn' : ''">{{ item.interrupted ? '中断中（保留位置）' : '等待中' }}</span>
            </td>
            <td>{{ formatTime(item.queued_at) }}</td>
          </tr>
          <tr v-if="!pool.queue.length">
            <td colspan="5" class="empty-state">当前没有排队请求</td>
          </tr>
        </tbody>
      </table>

      <h4 class="board-sub">历史分配记录（按当时代理与原因留档，并同步采集日志）</h4>
      <table class="data-table">
        <thead>
          <tr><th>分配编号</th><th>请求标识</th><th>关联任务</th><th>代理编号</th><th>动作</th><th>结果</th><th>原因</th><th>发生时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="row in pool.history" :key="String(row.id)">
            <td>{{ row['分配编号'] }}</td>
            <td>{{ row['请求标识'] ?? '—' }}</td>
            <td>{{ row['关联任务'] ?? '—' }}</td>
            <td>{{ row['代理编号'] ?? '—' }}</td>
            <td>{{ row['动作'] }}</td>
            <td>{{ row['结果'] || '—' }}</td>
            <td class="reason-cell">{{ row['原因'] }}</td>
            <td>{{ row['发生时间'] }}</td>
          </tr>
          <tr v-if="!pool.history.length">
            <td colspan="8" class="empty-state">暂无分配记录</td>
          </tr>
        </tbody>
      </table>
    </section>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>在途占用</th>
          <th>剩余额度</th>
          <th>失败次数</th>
          <th>池状态</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td>{{ row['在途占用'] ?? 0 }}</td>
          <td>{{ row['剩余额度'] ?? 0 }}</td>
          <td>{{ row['失败次数'] ?? 0 }}</td>
          <td>{{ row['池状态'] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 5" class="empty-state">暂无代理与请求控制数据，可先登记代理配置</td>
        </tr>
      </tbody>
    </table>

    <footer class="page-foot">
      <span>共 {{ total }} 条代理与请求控制记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | boolean | null>

interface ProxyQuota {
  id: number
  代理编号: string
  status: string
  是否可用: boolean
  并发上限: number | null
  在途占用: number
  剩余额度: number
  失败次数: number
  冷却截止时间: number | null
  冷却中: boolean
  探测进行中: boolean
  池状态: string
}

interface QueueItem {
  position: number
  request_key: string
  关联任务: string | null
  interrupted: boolean
  queued_at: number | null
}

interface AllocationRow {
  id: number
  分配编号: string
  请求标识: string | null
  关联任务: string | null
  代理编号: string | null
  动作: string
  结果: string
  原因: string
  发生时间: string
}

interface PoolSnapshot {
  at: number
  active_requests: number
  queued_requests: number
  proxies: ProxyQuota[]
  queue: QueueItem[]
  history: AllocationRow[]
}

const ENDPOINT = '/api/proxy'
const columns = ['代理编号', '代理地址', '代理端口', '协议类型', '认证凭据', '并发上限', '冷却时间', '是否可用']
const actions = ['提交校验', '启用配置', '停用配置']
const stats = ref([
  { label: '代理配置总数', value: 0 },
  { label: '在途请求', value: 0 },
  { label: '排队请求', value: 0 },
])

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)

const emptyPool = (): PoolSnapshot => ({ at: 0, active_requests: 0, queued_requests: 0, proxies: [], queue: [], history: [] })
const pool = ref<PoolSnapshot>(emptyPool())
const refreshing = ref(false)
const poolMessage = ref('')
const requestKey = ref('')
const taskRef = ref('')
const outcome = ref('success')
const retryCount = ref(0)

function formatTime(value: unknown): string {
  if (value === null || value === undefined || value === '') return '—'
  const epoch = Number(value)
  if (!Number.isFinite(epoch)) return String(value)
  return new Date(epoch * 1000).toLocaleString('zh-CN', { hour12: false })
}

function resetFilters() {
  filters.value = {}
  void reload()
}

function exportRows() {
  window.open(`${ENDPOINT}/export`, '_blank')
}

function openCreate() {
  errorMessage.value = '代理配置登记入口尚未接入审批流'
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ values: { action } }),
    })
    if (!response.ok) {
      throw new Error('代理与请求控制动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理与请求控制操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('代理配置列表读取失败')
    }
    const payload = await response.json()
    rows.value = (payload.items ?? []) as Row[]
    total.value = payload.total ?? rows.value.length
    stats.value[0].value = total.value
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理与请求控制列表读取失败'
  }
}

async function refreshPool() {
  refreshing.value = true
  poolMessage.value = ''
  try {
    // 先触发恢复 tick（冷却到期的单次探测机会、FIFO 补位），再读取工作台快照。
    await request(`${ENDPOINT}/pool/refresh`, { method: 'POST', body: JSON.stringify({ values: {} }) })
    const response = await request(`${ENDPOINT}/pool`)
    if (!response.ok) {
      throw new Error('代理池工作台读取失败')
    }
    pool.value = (await response.json()) as PoolSnapshot
    stats.value[1].value = pool.value.active_requests
    stats.value[2].value = pool.value.queued_requests
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理池刷新失败'
  } finally {
    refreshing.value = false
  }
}

async function callPool(path: string, body: Record<string, unknown>) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}${path}`, {
      method: 'POST',
      body: JSON.stringify(body),
    })
    const payload = await response.json().catch(() => ({}))
    if (!response.ok || payload.ok === false) {
      throw new Error(payload.detail || payload.message || '代理池操作未生效')
    }
    poolMessage.value = payload.message || '操作已生效'
    await refreshPool()
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '代理池操作失败'
  }
}

function acquire() {
  if (!requestKey.value.trim()) {
    errorMessage.value = '请求标识不能为空：空值请求无法登记排队或分配额度'
    return
  }
  void callPool('/pool/acquire', { request_key: requestKey.value.trim(), task_ref: taskRef.value.trim() || null })
}

function interrupt() {
  if (!requestKey.value.trim()) {
    errorMessage.value = '请先填写需要中断的请求标识'
    return
  }
  void callPool('/pool/interrupt', { request_key: requestKey.value.trim() })
}

function release() {
  if (!requestKey.value.trim()) {
    errorMessage.value = '请先填写需要归还额度的请求标识'
    return
  }
  void callPool('/pool/release', {
    request_key: requestKey.value.trim(),
    outcome: outcome.value,
    retry_count: Number.isFinite(retryCount.value) ? Math.max(retryCount.value, 0) : 0,
    error_message: outcome.value === 'success' ? null : `前端模拟：${outcome.value}`,
  })
}

onMounted(async () => {
  await reload()
  await refreshPool()
})
</script>

<style scoped>
.pool-board {
  background: #fff;
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 12px 14px;
  margin-bottom: 16px;
}
.board-head {
  display: flex;
  justify-content: space-between;
  align-items: center;
}
.board-head h3 {
  margin: 0;
  font-size: 15px;
}
.board-sub {
  margin: 14px 0 6px;
  font-size: 13px;
  color: var(--muted);
}
.board-message {
  margin: 8px 0 0;
  font-size: 12px;
  color: var(--brand);
}
.pool-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  align-items: flex-end;
  margin: 12px 0;
  padding: 10px;
  background: #f8fafc;
  border: 1px dashed var(--border);
  border-radius: 6px;
}
.quota-warn {
  color: #b42318;
}
.fail-warn {
  color: #b42318;
  font-weight: 600;
}
.state-tag {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 999px;
  font-size: 12px;
  background: #eef2ff;
  color: #3730a3;
}
.state-tag[data-state='冷却中'],
.state-tag[data-state='额度已满'],
.state-tag[data-state='配置异常'] {
  background: #fef2f2;
  color: #b42318;
}
.state-tag[data-state='待探测'],
.state-tag[data-state='探测中'] {
  background: #fffbeb;
  color: #b45309;
}
.state-tag[data-state='未启用'] {
  background: #f1f5f9;
  color: var(--muted);
}
.reason-cell {
  max-width: 320px;
  font-size: 12px;
  color: var(--muted);
}
</style>

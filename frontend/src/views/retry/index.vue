<template>
  <section class="page" data-module="retry">
    <header class="page-head">
      <div>
        <h2>失败重试管理</h2>
        <p class="page-desc">配置采集失败的重试次数、退避策略和幂等控制。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记重试策略</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出失败重试清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无失败重试数据，可先登记重试策略</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记重试策略</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条失败重试记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>

    <!-- ============================================================== -->
    <!-- 重试执行数据集工作台（v2 扩展，不改动上方既有结构与 testid） -->
    <!-- ============================================================== -->
    <section class="rex-workbench" data-testid="rex-workbench">
      <header class="page-head">
        <div>
          <h3>重试执行数据集</h3>
          <p class="page-desc">
            失败批次按幂等标识生成唯一重试记录，退避、次数与状态按任务批次统一管理；
            数据集与重试任务分配清单同事务提交。
          </p>
        </div>
        <div class="page-actions">
          <button class="btn primary" data-testid="rex-open-failure" type="button" @click="openFailureModal">
            接入失败事件
          </button>
          <button class="btn" data-testid="rex-refresh" type="button" @click="reloadWorkbench">刷新工作台</button>
          <button class="btn" data-testid="rex-version" type="button" @click="openVersionModal">
            数据集版本：{{ datasetVersion }}
          </button>
        </div>
      </header>

      <div class="stat-row">
        <article class="stat-card" data-testid="rex-stat-batches">
          <span class="stat-label">失败批次</span>
          <strong class="stat-value">{{ failureBatches.length }}</strong>
        </article>
        <article class="stat-card" data-testid="rex-stat-executions">
          <span class="stat-label">重试执行记录（幂等唯一）</span>
          <strong class="stat-value">{{ executions.length }}</strong>
        </article>
        <article class="stat-card" data-testid="rex-stat-assignments">
          <span class="stat-label">分配清单待处理</span>
          <strong class="stat-value">{{ assignments.length }}</strong>
        </article>
        <article class="stat-card" data-testid="rex-stat-success">
          <span class="stat-label">已成功批次</span>
          <strong class="stat-value">{{ successCount }}</strong>
        </article>
        <article class="stat-card" data-testid="rex-stat-exhausted">
          <span class="stat-label">已达上限批次</span>
          <strong class="stat-value">{{ exhaustedCount }}</strong>
        </article>
      </div>

      <div class="rex-tabs" role="tablist">
        <button
          v-for="tab in tabs"
          :key="tab.key"
          class="btn"
          type="button"
          role="tab"
          :data-testid="`rex-tab-${tab.key}`"
          :class="{ primary: activeTab === tab.key }"
          @click="activeTab = tab.key"
        >
          {{ tab.label }}
        </button>
      </div>

      <span v-if="rexMessage" class="error-text" data-testid="rex-message">{{ rexMessage }}</span>

      <!-- 失败批次核对 -->
      <table v-if="activeTab === 'failures'" class="data-table" data-testid="rex-failure-table">
        <thead>
          <tr>
            <th>失败批次编号</th><th>采集任务编号</th><th>幂等标识</th>
            <th>失败条目数</th><th>失败事件次数</th><th>退避类型</th>
            <th>退避系数</th><th>首次失败时间</th><th>状态记录</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="batch in failureBatches" :key="batch['幂等标识']" :data-testid="`rex-failure-row-${batch['失败批次编号']}`">
            <td>{{ batch['失败批次编号'] }}</td>
            <td>{{ batch['采集任务编号'] }}</td>
            <td>{{ batch['幂等标识'] }}</td>
            <td>{{ batch['失败条目'].length }}</td>
            <td>{{ batch['失败事件次数'] }}</td>
            <td>{{ batch['退避类型'] }}</td>
            <td>{{ batch['退避系数'] }}</td>
            <td>{{ formatTime(batch['首次失败时间']) }}</td>
            <td>{{ batch['状态记录'] }}</td>
          </tr>
          <tr v-if="!failureBatches.length">
            <td colspan="9" class="empty-state">暂无失败批次，可先接入失败事件</td>
          </tr>
        </tbody>
      </table>

      <!-- 重试执行数据集 -->
      <table v-if="activeTab === 'executions'" class="data-table" data-testid="rex-execution-table">
        <thead>
          <tr>
            <th>执行数据集编号</th><th>失败批次编号</th><th>采集任务编号</th><th>幂等标识</th>
            <th>已执行次数/最大重试</th><th>退避时间</th><th>状态记录</th>
            <th>触发来源</th><th>成功回执</th><th>待补排</th><th>数据集版本</th><th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in executions"
            :key="item['幂等标识']"
            :data-testid="`rex-execution-row-${item['失败批次编号']}`"
            :data-status="item['状态记录']"
          >
            <td>{{ item['执行数据集编号'] }}</td>
            <td>{{ item['失败批次编号'] }}</td>
            <td>{{ item['采集任务编号'] }}</td>
            <td>{{ item['幂等标识'] }}</td>
            <td data-testid="rex-attempts">{{ item['已执行次数'] }} / {{ item['最大重试'] }}</td>
            <td>{{ formatTime(item['退避时间']) }}</td>
            <td>{{ item['状态记录'] }}</td>
            <td>{{ item['触发来源'] || '—' }}<template v-if="item['并发触发记录'].length > 1">
              （并发：{{ item['并发触发记录'].join('、') }}）
            </template></td>
            <td data-testid="rex-receipt-count">{{ item['成功回执'].length }}</td>
            <td>{{ item['待补排条目'].length }}</td>
            <td>{{ item['数据集版本'] }}</td>
            <td class="row-actions">
              <button class="link" type="button" data-testid="rex-manual" @click="openRetryModal(item, '人工重试')">
                人工重试
              </button>
              <button class="link" type="button" data-testid="rex-scheduled" @click="openRetryModal(item, '定时重试')">
                定时重试
              </button>
              <button class="link" type="button" data-testid="rex-receipt" @click="openReceiptModal(item)">
                提交整组回执
              </button>
              <button class="link" type="button" data-testid="rex-detail" @click="openDetailModal(item)">
                核对明细
              </button>
            </td>
          </tr>
          <tr v-if="!executions.length">
            <td colspan="12" class="empty-state">暂无重试执行数据集</td>
          </tr>
        </tbody>
      </table>

      <!-- 重试任务分配清单（爬虫引擎跨业务面消费） -->
      <table v-if="activeTab === 'assignments'" class="data-table" data-testid="rex-assignment-table">
        <thead>
          <tr>
            <th>分配清单编号</th><th>爬虫任务编号</th><th>失败批次编号</th><th>执行数据集编号</th>
            <th>已执行次数/最大重试</th><th>退避时间</th><th>分配状态</th><th>待处理条目</th><th>数据集版本</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="assign in assignments" :key="assign['幂等标识']" :data-testid="`rex-assignment-row-${assign['失败批次编号']}`">
            <td>{{ assign['分配清单编号'] }}</td>
            <td>{{ assign['爬虫任务编号'] }}</td>
            <td>{{ assign['失败批次编号'] }}</td>
            <td>{{ assign['执行数据集编号'] }}</td>
            <td>{{ assign['已执行次数'] }} / {{ assign['最大重试'] }}</td>
            <td>{{ formatTime(assign['退避时间']) }}</td>
            <td>{{ assign['分配状态'] }}</td>
            <td>{{ assign['待处理条目'].join('、') }}</td>
            <td>{{ assign['数据集版本'] }}</td>
          </tr>
          <tr v-if="!assignments.length">
            <td colspan="9" class="empty-state">分配清单为空：所有批次均已成功或达到上限</td>
          </tr>
        </tbody>
      </table>

      <!-- 接入失败事件 -->
      <div v-if="showFailureModal" class="modal-backdrop" data-testid="rex-failure-modal">
        <form class="modal" @submit.prevent="submitFailureEvent">
          <h3>接入失败采集事件（幂等）</h3>
          <p class="page-desc">同一幂等标识重复提交只累加事件计数，不会创建第二份执行记录。</p>
          <label class="create-field"><span>采集任务编号</span>
            <input v-model="failureForm.task" data-testid="rex-failure-task" required />
          </label>
          <label class="create-field"><span>失败批次编号</span>
            <input v-model="failureForm.batch" data-testid="rex-failure-batch" required />
          </label>
          <label class="create-field"><span>幂等标识</span>
            <input v-model="failureForm.idem" data-testid="rex-failure-idem" required />
          </label>
          <label class="create-field"><span>最大重试</span>
            <input v-model.number="failureForm.maxRetry" type="number" min="1" data-testid="rex-failure-max" required />
          </label>
          <label class="create-field"><span>退避类型</span>
            <select v-model="failureForm.backoffType" data-testid="rex-failure-backoff-type">
              <option>固定退避</option>
              <option>指数退避</option>
            </select>
          </label>
          <label class="create-field"><span>退避系数（秒）</span>
            <input v-model.number="failureForm.backoffFactor" type="number" min="1" data-testid="rex-failure-backoff-factor" required />
          </label>
          <label class="create-field"><span>失败条目编号（逗号分隔，批内每条独立）</span>
            <input v-model="failureForm.itemCodes" data-testid="rex-failure-items" placeholder="ITEM-1,ITEM-2" required />
          </label>
          <div class="page-actions">
            <button class="btn" type="button" @click="showFailureModal = false">取消</button>
            <button class="btn primary" type="submit" data-testid="rex-failure-submit">确认接入</button>
          </div>
        </form>
      </div>

      <!-- 人工/定时重试提交 -->
      <div v-if="showRetryModal" class="modal-backdrop" data-testid="rex-retry-modal">
        <form class="modal" @submit.prevent="submitRetryRequest">
          <h3>{{ retryForm.source }}提交</h3>
          <p class="page-desc">
            失败批次 {{ retryForm.batch }} · 当前执行次数 {{ retryForm.currentAttempts }} / {{ retryForm.maxRetry }}。
            人工与定时并发提交时下游只保留一份带最终执行次数的记录。
          </p>
          <label class="create-field"><span>请求幂等号（同一请求第二次提交只记一次）</span>
            <input v-model="retryForm.requestId" data-testid="rex-retry-request-id" required />
          </label>
          <label class="create-field"><span>期望版本号（可选，乐观并发控制）</span>
            <input v-model="retryForm.expectVersion" data-testid="rex-retry-expect-version" placeholder="留空则不校验" />
          </label>
          <div class="page-actions">
            <button class="btn" type="button" @click="showRetryModal = false">取消</button>
            <button class="btn primary" type="submit" data-testid="rex-retry-submit">确认提交整组</button>
          </div>
        </form>
      </div>

      <!-- 整组执行回执 -->
      <div v-if="showReceiptModal" class="modal-backdrop" data-testid="rex-receipt-modal">
        <form class="modal" @submit.prevent="submitReceipts">
          <h3>整组执行回执 · {{ receiptForm.batch }}</h3>
          <p class="page-desc">
            批量入口直接提交整组，批内每条数据独立返回结果；部分失败只保留成功回执并补排失败项，
            不会整批覆盖既有执行结果。
          </p>
          <label class="create-field"><span>请求幂等号</span>
            <input v-model="receiptForm.requestId" data-testid="rex-receipt-request-id" required />
          </label>
          <table class="data-table">
            <thead><tr><th>条目编号</th><th>是否成功</th><th>回执内容</th></tr></thead>
            <tbody>
              <tr v-for="line in receiptForm.lines" :key="line.code" :data-testid="`rex-receipt-line-${line.code}`">
                <td>{{ line.code }}</td>
                <td>
                  <select v-model="line.ok" data-testid="rex-receipt-ok">
                    <option :value="true">成功</option>
                    <option :value="false">失败</option>
                  </select>
                </td>
                <td><input v-model="line.note" data-testid="rex-receipt-note" :placeholder="line.ok ? '成功回执' : '失败原因'" /></td>
              </tr>
            </tbody>
          </table>
          <div class="page-actions">
            <button class="btn" type="button" @click="showReceiptModal = false">取消</button>
            <button class="btn primary" type="submit" data-testid="rex-receipt-submit">提交整组回执</button>
          </div>
        </form>
      </div>

      <!-- 明细核对：失败批次、重试次数、退避时间、执行回执、人工/定时是否只执行一次 -->
      <div v-if="showDetailModal" class="modal-backdrop" data-testid="rex-detail-modal">
        <div class="modal">
          <h3>执行数据集核对 · {{ detailRecord?.['失败批次编号'] }}</h3>
          <dl v-if="detailRecord" class="rex-detail">
            <dt>执行数据集编号</dt><dd>{{ detailRecord['执行数据集编号'] }}</dd>
            <dt>幂等标识</dt><dd>{{ detailRecord['幂等标识'] }}</dd>
            <dt>重试次数</dt><dd data-testid="rex-detail-attempts">
              {{ detailRecord['已执行次数'] }} / {{ detailRecord['最大重试'] }}
            </dd>
            <dt>退避时间</dt><dd>{{ formatTime(detailRecord['退避时间']) }}</dd>
            <dt>状态记录</dt><dd>{{ detailRecord['状态记录'] }}</dd>
            <dt>触发来源</dt><dd>{{ detailRecord['触发来源'] || '—' }}</dd>
            <dt>人工/定时并发留痕</dt><dd data-testid="rex-detail-sources">
              {{ detailRecord['并发触发记录'].join('、') || '—' }}
            </dd>
            <dt>已提交请求幂等号</dt><dd>{{ detailRecord['已提交请求'].join('、') || '—' }}</dd>
            <dt>乐观版本号</dt><dd>{{ detailRecord['乐观版本号'] }}</dd>
            <dt>数据集版本</dt><dd>{{ detailRecord['数据集版本'] }}</dd>
          </dl>
          <h4>批内条目独立结果</h4>
          <table class="data-table" data-testid="rex-detail-items">
            <thead><tr><th>条目编号</th><th>状态</th><th>尝试次数</th><th>最后回执</th><th>最后更新</th></tr></thead>
            <tbody>
              <tr v-for="(state, code) in detailRecord?.['条目结果']" :key="code">
                <td>{{ code }}</td>
                <td>{{ state['状态'] }}</td>
                <td>{{ state['尝试次数'] }}</td>
                <td>{{ state['最后回执'] || '—' }}</td>
                <td>{{ formatTime(state['最后更新']) }}</td>
              </tr>
            </tbody>
          </table>
          <h4>成功回执（只追加不覆盖）</h4>
          <table class="data-table" data-testid="rex-detail-receipts">
            <thead><tr><th>回执编号</th><th>条目编号</th><th>执行轮次</th><th>回执内容</th><th>时间</th></tr></thead>
            <tbody>
              <tr v-for="receipt in detailRecord?.['成功回执']" :key="receipt['回执编号']">
                <td>{{ receipt['回执编号'] }}</td>
                <td>{{ receipt['条目编号'] }}</td>
                <td>{{ receipt['执行轮次'] }}</td>
                <td>{{ receipt['回执内容'] }}</td>
                <td>{{ formatTime(receipt['时间']) }}</td>
              </tr>
              <tr v-if="!detailRecord?.['成功回执'].length"><td colspan="5" class="empty-state">暂无成功回执</td></tr>
            </tbody>
          </table>
          <div class="page-actions">
            <button class="btn primary" type="button" data-testid="rex-detail-close" @click="showDetailModal = false">关闭</button>
          </div>
        </div>
      </div>

      <!-- 数据集版本切换 -->
      <div v-if="showVersionModal" class="modal-backdrop" data-testid="rex-version-modal">
        <form class="modal" @submit.prevent="submitVersionSwitch">
          <h3>切换重试执行数据集版本</h3>
          <p class="page-desc">切换后既有输入输出字段保持不变，仅新记录使用新版本戳。</p>
          <label class="create-field"><span>目标版本</span>
            <select v-model="versionForm.version" data-testid="rex-version-select">
              <option v-for="v in supportedVersions" :key="v" :value="v">{{ v }}</option>
            </select>
          </label>
          <p class="page-desc">稳定字段契约：{{ fieldContract.join('、') }}</p>
          <div class="page-actions">
            <button class="btn" type="button" @click="showVersionModal = false">取消</button>
            <button class="btn primary" type="submit" data-testid="rex-version-submit">确认切换</button>
          </div>
        </form>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

type ItemState = {
  条目编号: string
  状态: string
  尝试次数: number
  最后回执: string
  最后更新: string
}

type Receipt = {
  回执编号: string
  条目编号: string
  执行轮次: number
  状态: string
  回执内容: string
  时间: string
}

type ExecutionRecord = {
  执行数据集编号: string
  失败批次编号: string
  采集任务编号: string
  幂等标识: string
  最大重试: number
  退避类型: string
  退避系数: number
  退避时间: string
  已执行次数: number
  状态记录: string
  触发来源: string
  并发触发记录: string[]
  数据集版本: string
  条目结果: Record<string, ItemState>
  成功回执: Receipt[]
  待补排条目: string[]
  已提交请求: string[]
  乐观版本号: number
  创建时间: string
  最后更新: string
}

type FailureBatch = {
  失败批次编号: string
  采集任务编号: string
  幂等标识: string
  失败条目: { 条目编号: string; 原始URL: string; 失败原因: string }[]
  最大重试: number
  退避类型: string
  退避系数: number
  首次失败时间: string
  最后失败时间: string
  失败事件次数: number
  状态记录: string
}

type Assignment = {
  分配清单编号: string
  爬虫任务编号: string
  失败批次编号: string
  幂等标识: string
  执行数据集编号: string
  数据集版本: string
  已执行次数: number
  最大重试: number
  退避时间: string
  分配状态: string
  待处理条目: string[]
  最后更新: string
}

const ENDPOINT = '/api/retry'
const columns = ["策略编号", "适用任务", "最大重试", "退避类型", "退避系数", "幂等标识", "状态记录", "最后更新"]
const actions = ["提交校验", "启用配置", "停用配置"]
const statuses = ["草稿", "待校验", "已启用", "已停用"]
const metricNames = ["重试策略总数", "待校验重试策略", "已启用重试策略"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'retry.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('失败重试动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '失败重试操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('重试策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '失败重试列表读取失败'
  }
}

// ---------------------------------------------------------------
// 重试执行数据集工作台
// ---------------------------------------------------------------

const tabs = [
  { key: 'failures', label: '失败批次' },
  { key: 'executions', label: '重试执行数据集' },
  { key: 'assignments', label: '重试任务分配清单' },
] as const

type TabKey = (typeof tabs)[number]['key']

const activeTab = ref<TabKey>('executions')
const failureBatches = ref<FailureBatch[]>([])
const executions = ref<ExecutionRecord[]>([])
const assignments = ref<Assignment[]>([])
const datasetVersion = ref('v1')
const supportedVersions = ref<string[]>(['v1', 'v2'])
const fieldContract = ref<string[]>([])
const rexMessage = ref('')

const successCount = computed(() => executions.value.filter((item) => item['状态记录'] === '成功').length)
const exhaustedCount = computed(() => executions.value.filter((item) => item['状态记录'] === '已达上限').length)

async function callApi(path: string, init?: RequestInit): Promise<{ ok: boolean; status: number; body: any }> {
  const response = await request(path, init)
  const body = await response.json().catch(() => ({}))
  return { ok: response.ok, status: response.status, body }
}

function failText(status: number, body: { detail?: unknown }): string {
  if (typeof body.detail === 'string') return body.detail
  return `请求失败（HTTP ${status}）`
}

function formatTime(value: string): string {
  if (!value) return '—'
  return value.replace('T', ' ').replace('+00:00', ' UTC')
}

async function reloadWorkbench() {
  rexMessage.value = ''
  try {
    const [failuresRes, execRes, assignRes, versionRes] = await Promise.all([
      callApi(`${ENDPOINT}/failure-batches`),
      callApi(`${ENDPOINT}/executions`),
      callApi('/api/crawler/retry-assignments'),
      callApi(`${ENDPOINT}/dataset-version`),
    ])
    if (!failuresRes.ok || !execRes.ok || !assignRes.ok || !versionRes.ok) {
      throw new Error('重试执行数据集读取失败')
    }
    failureBatches.value = failuresRes.body.items ?? []
    executions.value = execRes.body.items ?? []
    assignments.value = assignRes.body.items ?? []
    datasetVersion.value = versionRes.body.active_version ?? 'v1'
    supportedVersions.value = versionRes.body.supported_versions ?? ['v1', 'v2']
    fieldContract.value = versionRes.body.field_contract ?? []
  } catch (error) {
    rexMessage.value = error instanceof Error ? error.message : '工作台加载失败'
  }
}

// 接入失败事件
const showFailureModal = ref(false)
const failureForm = ref({
  task: '', batch: '', idem: '', maxRetry: 3,
  backoffType: '指数退避', backoffFactor: 10, itemCodes: '',
})

function openFailureModal() {
  failureForm.value = {
    task: '', batch: '', idem: '', maxRetry: 3,
    backoffType: '指数退避', backoffFactor: 10, itemCodes: '',
  }
  rexMessage.value = ''
  showFailureModal.value = true
}

async function submitFailureEvent() {
  rexMessage.value = ''
  const codes = failureForm.value.itemCodes
    .split(/[,，]/).map((code) => code.trim()).filter(Boolean)
  if (!codes.length) {
    rexMessage.value = '失败条目至少一条'
    return
  }
  const payload = {
    采集任务编号: failureForm.value.task,
    失败批次编号: failureForm.value.batch,
    幂等标识: failureForm.value.idem,
    最大重试: failureForm.value.maxRetry,
    退避类型: failureForm.value.backoffType,
    退避系数: failureForm.value.backoffFactor,
    失败条目: codes.map((code) => ({ 条目编号: code })),
  }
  const result = await callApi(`${ENDPOINT}/failure-events`, {
    method: 'POST', body: JSON.stringify(payload),
  })
  if (!result.ok) {
    rexMessage.value = failText(result.status, result.body)
    return
  }
  showFailureModal.value = false
  rexMessage.value = result.body.deduplicated
    ? '重复失败事件已幂等处理，未创建第二份执行记录'
    : '失败事件已接入，执行数据集与分配清单同事务写入'
  await reloadWorkbench()
}

// 人工/定时重试提交
const showRetryModal = ref(false)
const retryForm = ref({
  batch: '', idem: '', source: '人工重试', requestId: '',
  expectVersion: '', currentAttempts: 0, maxRetry: 0,
})

function openRetryModal(item: ExecutionRecord, source: string) {
  retryForm.value = {
    batch: item['失败批次编号'],
    idem: item['幂等标识'],
    source,
    requestId: `REQ-${source === '人工重试' ? 'MAN' : 'SCH'}-${Date.now()}`,
    expectVersion: '',
    currentAttempts: item['已执行次数'],
    maxRetry: item['最大重试'],
  }
  rexMessage.value = ''
  showRetryModal.value = true
}

async function submitRetryRequest() {
  rexMessage.value = ''
  const payload: Record<string, unknown> = {
    失败批次编号: retryForm.value.batch,
    触发来源: retryForm.value.source,
    请求幂等号: retryForm.value.requestId,
  }
  if (retryForm.value.expectVersion.trim()) {
    payload['期望版本号'] = Number(retryForm.value.expectVersion)
  }
  const result = await callApi(`${ENDPOINT}/retry-requests`, {
    method: 'POST', body: JSON.stringify(payload),
  })
  if (!result.ok) {
    rexMessage.value = failText(result.status, result.body)
    return
  }
  showRetryModal.value = false
  rexMessage.value = result.body.message || '重试请求已提交'
  await reloadWorkbench()
}

// 整组回执
const showReceiptModal = ref(false)
const receiptForm = ref<{ batch: string; idem: string; requestId: string; lines: { code: string; ok: boolean; note: string }[] }>({
  batch: '', idem: '', requestId: '', lines: [],
})

function openReceiptModal(item: ExecutionRecord) {
  if (!item['待补排条目'].length) {
    rexMessage.value = '该批次已无待补排条目，无需再提交回执'
    return
  }
  receiptForm.value = {
    batch: item['失败批次编号'],
    idem: item['幂等标识'],
    requestId: `RCPT-${Date.now()}`,
    lines: item['待补排条目'].map((code) => ({ code, ok: true, note: '' })),
  }
  rexMessage.value = ''
  showReceiptModal.value = true
}

async function submitReceipts() {
  rexMessage.value = ''
  const payload = {
    失败批次编号: receiptForm.value.batch,
    请求幂等号: receiptForm.value.requestId,
    条目结果: receiptForm.value.lines.map((line) => ({
      条目编号: line.code,
      是否成功: line.ok,
      回执: line.note,
    })),
  }
  const result = await callApi(`${ENDPOINT}/execution-receipts`, {
    method: 'POST', body: JSON.stringify(payload),
  })
  if (!result.ok) {
    rexMessage.value = failText(result.status, result.body)
    return
  }
  showReceiptModal.value = false
  rexMessage.value = result.body.deduplicated
    ? '该回执请求已提交过，按既有成功回执返回，未重复执行'
    : (result.body.message || '整组回执已提交')
  await reloadWorkbench()
}

// 明细核对
const showDetailModal = ref(false)
const detailRecord = ref<ExecutionRecord | null>(null)

function openDetailModal(item: ExecutionRecord) {
  detailRecord.value = item
  showDetailModal.value = true
}

// 版本切换
const showVersionModal = ref(false)
const versionForm = ref({ version: 'v2' })

function openVersionModal() {
  versionForm.value = { version: datasetVersion.value === 'v1' ? 'v2' : 'v1' }
  rexMessage.value = ''
  showVersionModal.value = true
}

async function submitVersionSwitch() {
  rexMessage.value = ''
  const result = await callApi(`${ENDPOINT}/dataset-version/switch`, {
    method: 'POST', body: JSON.stringify({ version: versionForm.value.version }),
  })
  if (!result.ok) {
    rexMessage.value = failText(result.status, result.body)
    return
  }
  showVersionModal.value = false
  rexMessage.value = result.body.message || '数据集版本已切换，既有输入输出字段保持不变'
  await reloadWorkbench()
}

onMounted(() => {
  void reload()
  void reloadWorkbench()
})
</script>

<style scoped>
.rex-workbench {
  margin-top: 28px;
  padding-top: 16px;
  border-top: 2px solid var(--border);
}

.rex-tabs {
  display: flex;
  gap: 8px;
  margin: 12px 0;
}

.rex-detail {
  display: grid;
  grid-template-columns: 160px 1fr;
  gap: 6px 12px;
  margin: 8px 0 16px;
  font-size: 13px;
}

.rex-detail dt {
  color: var(--muted);
}

.rex-detail dd {
  margin: 0;
}

.rex-workbench h4 {
  margin: 16px 0 8px;
}

.rex-workbench select {
  padding: 6px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 13px;
}
</style>

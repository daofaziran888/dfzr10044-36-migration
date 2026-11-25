<template>
  <section class="page" data-module="quarantine_zone">
    <header class="page-head">
      <div>
        <h2>隔离批次详情</h2>
        <p class="page-desc">
          湖表 {{ lakeTable }} · 旧批 {{ batchId }}：逐行查看原处置、迁移确认与回炉凭证。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="migrate-batch" type="button" @click="runMigrate">
          迁移该旧批
        </button>
        <button class="btn" data-testid="reconcile" type="button" @click="runReconcile">
          两路核对
        </button>
        <button class="btn" data-testid="restore-legacy" type="button" @click="runRestore">
          还原旧入口
        </button>
        <RouterLink class="btn ghost" to="/quarantine_zone">返回隔离区</RouterLink>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">处置清单行</span>
        <strong class="stat-value">{{ lines.length }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待处置</span>
        <strong class="stat-value">{{ countByStatus('待处置') + countByStatus('处置中') }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">待核</span>
        <strong class="stat-value">{{ countByStatus('待核') }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">已回炉</span>
        <strong class="stat-value">{{ countByStatus('已回炉') }}</strong>
      </article>
    </div>

    <p v-if="migrationNote" class="status-legend" data-testid="migration-note">{{ migrationNote }}</p>

    <h3>处置清单（迁移后逐行）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>行号</th>
          <th>当时规则</th>
          <th>隔离原值</th>
          <th>当前状态</th>
          <th>回炉凭证</th>
          <th>确认入口</th>
          <th>迁移确认</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="line in lines" :key="line.disposition_id" :data-testid="`disp-${line.line_no}`">
          <td>{{ line.line_no }}</td>
          <td>{{ line.rule_id }} · {{ line.rule_name }}</td>
          <td><code>{{ JSON.stringify(line.original_row) }}</code></td>
          <td>{{ line.status }}</td>
          <td>{{ line.retry_voucher ?? '—' }}</td>
          <td>{{ line.confirmed_via ?? '—' }}</td>
          <td>{{ line.migration_id ?? '—' }}</td>
          <td class="row-actions">
            <button
              class="link"
              type="button"
              :data-testid="`claim-${line.line_no}`"
              @click="claim(line)"
            >
              领取
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`retry-${line.line_no}`"
              @click="retry(line)"
            >
              回炉重试
            </button>
          </td>
        </tr>
        <tr v-if="!lines.length">
          <td colspan="8" class="empty-state">暂无处置清单行，可先点击「迁移该旧批」</td>
        </tr>
      </tbody>
    </table>

    <h3>原隔离清单（旧入口视图）</h3>
    <table class="data-table">
      <thead>
        <tr>
          <th>行号</th>
          <th>违规规则</th>
          <th>旧记录原文</th>
          <th>旧确认</th>
          <th>处置编号</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in legacyRows" :key="String(row._quarantine_line)" :data-testid="`legacy-${row._quarantine_line}`">
          <td>{{ row._quarantine_line }}</td>
          <td>{{ row._quarantine_rule }}</td>
          <td><code>{{ JSON.stringify(stripMeta(row)) }}</code></td>
          <td>{{ row._retry_voucher ?? '—' }}</td>
          <td>{{ row._disposition_id ?? '—' }}</td>
        </tr>
        <tr v-if="!legacyRows.length">
          <td colspan="5" class="empty-state">旧清单为空（可能已被旧入口弹出，可点「还原旧入口」重建）</td>
        </tr>
      </tbody>
    </table>

    <section v-if="receipt" class="stat-card" data-testid="receipt-panel">
      <h3>入湖凭证（已关联同一份旧记录）</h3>
      <p>凭证号：{{ receipt.voucher }}<span v-if="receipt.merged">（归并既有确认）</span></p>
      <p>关联处置行：{{ receipt.disposition_id }}</p>
      <p>旧批 / 行号：{{ receipt.batch_id }} · 第 {{ receipt.line_no }} 行</p>
      <p>落湖快照：{{ receipt.snapshot_id }} · 重入批次：{{ receipt.rebatch_id }}</p>
      <p>关联旧记录：<code>{{ JSON.stringify(receipt.legacy_ref) }}</code></p>
    </section>

    <section v-if="reconcileReport" class="stat-card" data-testid="reconcile-panel">
      <h3>两路核对（旧入口 × 新清单）</h3>
      <p>{{ reconcileReport.consistent ? '两路看到的处置结果一致' : '存在不一致，见下' }}</p>
      <ul v-if="reconcileReport.inconsistencies.length">
        <li v-for="item in reconcileReport.inconsistencies" :key="item">{{ item }}</li>
      </ul>
    </section>

    <footer class="page-foot">
      <span>共 {{ lines.length }} 行处置清单记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  claimDisposition,
  listDispositions,
  loadLegacyView,
  migrateBatch,
  reconcileBatch,
  restoreLegacyEntry,
  retryDisposition,
  type DispositionLine,
  type IngestReceipt,
  type ReconcileReport,
} from '@/api/quarantine-api'

const props = defineProps<{ lakeTable: string; batchId: string }>()

const lines = ref<DispositionLine[]>([])
const legacyRows = ref<Record<string, unknown>[]>([])
const receipt = ref<(IngestReceipt & { merged?: boolean }) | null>(null)
const reconcileReport = ref<ReconcileReport | null>(null)
const migrationNote = ref('')
const errorMessage = ref('')

function countByStatus(status: string): number {
  return lines.value.filter((line) => line.status === status).length
}

function stripMeta(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).filter(([key]) => !key.startsWith('_')),
  )
}

async function reload() {
  errorMessage.value = ''
  try {
    lines.value = await listDispositions(props.lakeTable, props.batchId)
    legacyRows.value = await loadLegacyView(props.lakeTable, props.batchId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '批次详情读取失败'
  }
}

async function runMigrate() {
  errorMessage.value = ''
  try {
    const summary = await migrateBatch(props.lakeTable, props.batchId)
    migrationNote.value = `迁移 ${summary.migration_id}：${summary.status}，共 ${summary.total_lines} 行（${JSON.stringify(summary.by_status)}）`
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '迁移失败'
  }
}

async function runReconcile() {
  errorMessage.value = ''
  try {
    reconcileReport.value = await reconcileBatch(props.lakeTable, props.batchId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '两路核对失败'
  }
}

async function runRestore() {
  errorMessage.value = ''
  try {
    legacyRows.value = await restoreLegacyEntry(props.lakeTable, props.batchId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '还原旧入口失败'
  }
}

async function claim(line: DispositionLine) {
  errorMessage.value = ''
  try {
    const result = await claimDisposition(line.disposition_id)
    if (!result.ok) throw new Error(result.message)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '领取失败'
  }
}

async function retry(line: DispositionLine) {
  errorMessage.value = ''
  try {
    const result = await retryDisposition(line.disposition_id)
    if (!result.ok || !result.receipt) throw new Error(result.message)
    receipt.value = { ...result.receipt, merged: result.merged ?? result.receipt.merged ?? false }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回炉重试失败'
  }
}

onMounted(reload)
</script>

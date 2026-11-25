/** 隔离区迁移与回炉接口：处置清单、迁移流水线、两路核对、旧入口视图、入湖凭证。 */
import { request } from './client'

export type DispositionLine = {
  disposition_id: string
  lake_table: string
  batch_id: string
  line_no: number
  rule_id: string
  rule_name: string
  original_row: Record<string, unknown>
  legacy_row: Record<string, unknown>
  quarantine_time: number | null
  status: string
  retry_voucher: string | null
  confirmed_via: string | null
  migration_id: string | null
  migrated_at: number | null
  claimed_by: string | null
  history: { event: string; [key: string]: unknown }[]
  updated_at: number
}

export type IngestReceipt = {
  voucher: string
  lake_table: string
  batch_id: string
  line_no: number
  disposition_id: string | null
  rebatch_id: string
  snapshot_id: string
  file_id: string
  via: string
  issued_at: number
  legacy_ref: Record<string, unknown> | null
  merged: boolean
}

export type ReconcileReport = {
  consistent: boolean
  inconsistencies: string[]
  merged_confirmations: Record<string, unknown>[]
  ledger_view: DispositionLine[]
  legacy_view: Record<string, unknown>[]
}

export type MigrationSummary = {
  migration_id: string
  lake_table: string
  batch_id: string
  status: string
  total_lines: number
  by_status: Record<string, number>
  note?: string
}

async function parseJson<T>(response: Response): Promise<T> {
  return (await response.json()) as T
}

export async function listDispositions(
  lakeTable: string,
  batchId: string,
): Promise<DispositionLine[]> {
  const query = new URLSearchParams({ lake_table: lakeTable, batch_id: batchId })
  const payload = await parseJson<{ items: DispositionLine[] }>(
    await request(`/api/quarantine/dispositions?${query}`),
  )
  return payload.items
}

export async function migrateBatch(lakeTable: string, batchId: string): Promise<MigrationSummary> {
  const response = await request('/api/quarantine/migrate', {
    method: 'POST',
    body: JSON.stringify({ lake_table: lakeTable, batch_id: batchId }),
  })
  return await parseJson<MigrationSummary>(response)
}

export async function claimDisposition(dispositionId: string): Promise<{ ok: boolean; message: string }> {
  const response = await request(`/api/quarantine/dispositions/${encodeURIComponent(dispositionId)}/claim`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
  return await parseJson(response)
}

export async function retryDisposition(
  dispositionId: string,
): Promise<{ ok: boolean; message: string; receipt?: IngestReceipt; merged?: boolean }> {
  const response = await request(`/api/quarantine/dispositions/${encodeURIComponent(dispositionId)}/retry`, {
    method: 'POST',
  })
  return await parseJson(response)
}

export async function loadLegacyView(
  lakeTable: string,
  batchId: string,
): Promise<Record<string, unknown>[]> {
  const query = new URLSearchParams({ lake_table: lakeTable, batch_id: batchId })
  const payload = await parseJson<{ items: Record<string, unknown>[] }>(
    await request(`/api/quarantine/legacy?${query}`),
  )
  return payload.items
}

export async function restoreLegacyEntry(
  lakeTable: string,
  batchId: string,
): Promise<Record<string, unknown>[]> {
  const response = await request('/api/quarantine/legacy/restore', {
    method: 'POST',
    body: JSON.stringify({ lake_table: lakeTable, batch_id: batchId }),
  })
  const payload = await parseJson<{ items: Record<string, unknown>[] }>(response)
  return payload.items
}

export async function reconcileBatch(lakeTable: string, batchId: string): Promise<ReconcileReport> {
  const query = new URLSearchParams({ lake_table: lakeTable, batch_id: batchId })
  return await parseJson<ReconcileReport>(await request(`/api/quarantine/reconcile?${query}`))
}

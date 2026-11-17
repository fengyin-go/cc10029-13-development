import { expect, test } from '@playwright/test'

/**
 * 失败重试 v2：幂等重试批次与重试执行数据集工作台验收。
 *
 * 覆盖口径：
 * - 失败事件接入幂等，重复事件不创建第二份执行记录
 * - 人工/定时重试并发只保留一份带最终执行次数的记录
 * - 整组执行、批内逐条回执，部分失败只补排不覆盖
 * - 分配清单只含尚未成功且未达上限的批次
 * - 同一请求第二次提交只记一次
 * - 数据集版本切换保持既有字段
 * - 操作者可核对失败批次、重试次数、退避时间、执行回执及人工/定时是否只执行一次
 */

const ENDPOINT = '/api/retry'

async function resetDatasets(request) {
  // 内存库没有清理接口，通过接入全新幂等批次保证用例数据独立。
  const stamp = Date.now()
  const payload = {
    采集任务编号: `E2E-JOB-${stamp}`,
    失败批次编号: `E2E-FAIL-${stamp}`,
    幂等标识: `E2E-IDEM-${stamp}`,
    最大重试: 2,
    退避类型: '指数退避',
    退避系数: 5,
    失败条目: [{ 条目编号: 'ITEM-A' }, { 条目编号: 'ITEM-B' }],
  }
  const resp = await request.post(`${ENDPOINT}/failure-events`, { data: payload })
  expect(resp.ok()).toBeTruthy()
  return payload
}

test.describe('失败重试执行数据集工作台', () => {
  test.beforeEach(async ({ page }) => {
    await page.goto('/retry')
    await page.getByTestId('rex-workbench').waitFor()
  })

  test('工作台渲染三类数据集与统计卡片，既有重试策略 CRUD 区域保留', async ({ page }) => {
    // 既有 CRUD 结构未被破坏
    await expect(page.getByTestId('open-create')).toBeVisible()
    await expect(page.locator('tbody tr[data-testid^="row-"]')).toHaveCount(3)

    // v2 工作台
    await expect(page.getByTestId('rex-stat-batches')).toBeVisible()
    await expect(page.getByTestId('rex-stat-executions')).toBeVisible()
    await expect(page.getByTestId('rex-stat-assignments')).toBeVisible()
    await page.getByTestId('rex-tab-failures').click()
    await expect(page.getByTestId('rex-failure-table')).toBeVisible()
    await page.getByTestId('rex-tab-executions').click()
    await expect(page.getByTestId('rex-execution-table')).toBeVisible()
    await page.getByTestId('rex-tab-assignments').click()
    await expect(page.getByTestId('rex-assignment-table')).toBeVisible()
  })

  test('失败事件重复接入只产生一份执行记录', async ({ page, request }) => {
    const payload = await resetDatasets(request)
    await page.getByTestId('rex-refresh').click()

    await page.getByTestId('rex-open-failure').click()
    await expect(page.getByTestId('rex-failure-modal')).toBeVisible()
    await page.getByTestId('rex-failure-task').fill(payload.采集任务编号)
    await page.getByTestId('rex-failure-batch').fill(payload.失败批次编号)
    await page.getByTestId('rex-failure-idem').fill(payload.幂等标识)
    await page.getByTestId('rex-failure-items').fill('ITEM-A,ITEM-B')
    await page.getByTestId('rex-failure-submit').click()
    await expect(page.getByTestId('rex-failure-modal')).toHaveCount(0)
    await expect(page.getByTestId('rex-message')).toContainText('幂等')

    // 后端口径：同一幂等标识执行数据集始终只有一份
    const executions = await request.get(`${ENDPOINT}/executions`).then((r) => r.json())
    const matched = executions.items.filter((it) => it.幂等标识 === payload.幂等标识)
    expect(matched).toHaveLength(1)
    expect(matched[0].状态记录).toBe('待重试')
    expect(matched[0].已执行次数).toBe(0)
  })

  test('人工与定时并发只执行一次，整组回执部分失败只补排不覆盖', async ({ page, request }) => {
    const payload = await resetDatasets(request)
    const batch = payload.失败批次编号
    await page.getByTestId('rex-refresh').click()
    await page.getByTestId('rex-tab-executions').click()
    const row = page.getByTestId(`rex-execution-row-${batch}`)
    await row.waitFor()

    // 人工重试开轮
    await row.getByTestId('rex-manual').click()
    await expect(page.getByTestId('rex-retry-modal')).toBeVisible()
    const manualRequestId = `E2E-MAN-${Date.now()}`
    await page.getByTestId('rex-retry-request-id').fill(manualRequestId)
    await page.getByTestId('rex-retry-submit').click()
    await expect(page.getByTestId('rex-retry-modal')).toHaveCount(0)

    // 定时重试并发提交：冲突合并，次数不增加
    await row.getByTestId('rex-scheduled').click()
    await page.getByTestId('rex-retry-request-id').fill(`E2E-SCH-${Date.now()}`)
    await page.getByTestId('rex-retry-submit').click()

    let record = await request.get(`${ENDPOINT}/executions/${batch}`).then((r) => r.json())
    expect(record.已执行次数).toBe(1)
    expect(record.并发触发记录).toContain('人工重试')
    expect(record.并发触发记录).toContain('定时重试')

    // 同一人工请求第二次提交只记一次
    const dup = await request.post(`${ENDPOINT}/retry-requests`, {
      data: { 失败批次编号: batch, 触发来源: '人工重试', 请求幂等号: manualRequestId },
    }).then((r) => r.json())
    expect(dup.deduplicated).toBe(true)
    record = await request.get(`${ENDPOINT}/executions/${batch}`).then((r) => r.json())
    expect(record.已执行次数).toBe(1)

    // 整组回执：ITEM-A 成功、ITEM-B 失败
    await row.getByTestId('rex-receipt').click()
    await expect(page.getByTestId('rex-receipt-modal')).toBeVisible()
    const lines = page.locator('[data-testid^="rex-receipt-line-"]')
    await expect(lines).toHaveCount(2)
    await page.getByTestId('rex-receipt-line-ITEM-B').getByTestId('rex-receipt-ok').selectOption('false')
    await page.getByTestId('rex-receipt-submit').click()
    await expect(page.getByTestId('rex-receipt-modal')).toHaveCount(0)

    record = await request.get(`${ENDPOINT}/executions/${batch}`).then((r) => r.json())
    expect(record.状态记录).toBe('部分成功待补排')
    expect(record.成功回执).toHaveLength(1)
    expect(record.成功回执[0].条目编号).toBe('ITEM-A')
    expect(record.待补排条目).toEqual(['ITEM-B'])
    expect(record.条目结果['ITEM-A'].状态).toBe('成功')
    expect(record.条目结果['ITEM-B'].状态).toBe('失败')

    // 第二轮补排 ITEM-B 成功，历史回执不被覆盖
    await request.post(`${ENDPOINT}/retry-requests`, {
      data: { 失败批次编号: batch, 触发来源: '定时重试', 请求幂等号: `E2E-SCH2-${Date.now()}` },
    })
    const finalBody = await request.post(`${ENDPOINT}/execution-receipts`, {
      data: {
        失败批次编号: batch,
        请求幂等号: `E2E-RCPT2-${Date.now()}`,
        条目结果: [{ 条目编号: 'ITEM-B', 是否成功: true, 回执: 'ok-b' }],
      },
    }).then((r) => r.json())
    expect(finalBody.execution.状态记录).toBe('成功')
    expect(finalBody.execution.成功回执).toHaveLength(2)
    expect(finalBody.execution.已执行次数).toBe(2)

    // 明细核对弹窗可核对次数、退避、回执与并发留痕
    await page.getByTestId('rex-refresh').click()
    await row.getByTestId('rex-detail').click()
    await expect(page.getByTestId('rex-detail-modal')).toBeVisible()
    await expect(page.getByTestId('rex-detail-attempts')).toContainText('2 / 2')
    await expect(page.getByTestId('rex-detail-sources')).toContainText('人工重试')
    await expect(page.getByTestId('rex-detail-sources')).toContainText('定时重试')
    await expect(page.getByTestId('rex-detail-receipts').locator('tbody tr')).toHaveCount(2)
  })

  test('分配清单只含未成功且未达上限批次，成功后立即消失', async ({ page, request }) => {
    const payload = await resetDatasets(request)
    const batch = payload.失败批次编号
    await page.getByTestId('rex-refresh').click()
    await page.getByTestId('rex-tab-assignments').click()
    await expect(page.getByTestId(`rex-assignment-row-${batch}`)).toBeVisible()

    // 两轮内全部成功
    for (const round of [1, 2]) {
      const remaining = round === 1 ? ['ITEM-A', 'ITEM-B'] : ['ITEM-B']
      await request.post(`${ENDPOINT}/retry-requests`, {
        data: { 失败批次编号: batch, 触发来源: '人工重试', 请求幂等号: `ASSIGN-O-${round}-${Date.now()}` },
      })
      const resp = await request.post(`${ENDPOINT}/execution-receipts`, {
        data: {
          失败批次编号: batch,
          请求幂等号: `ASSIGN-R-${round}-${Date.now()}`,
          条目结果: remaining.map((code) => ({
            条目编号: code,
            是否成功: round === 2 || code === 'ITEM-A',
          })),
        },
      })
      expect(resp.ok()).toBeTruthy()
    }
    await page.getByTestId('rex-refresh').click()
    await expect(page.getByTestId(`rex-assignment-row-${batch}`)).toHaveCount(0)

    // 爬虫引擎跨业务面口同样过滤
    const crawlerView = await request.get('/api/crawler/retry-assignments').then((r) => r.json())
    expect(crawlerView.items.find((it) => it.失败批次编号 === batch)).toBeUndefined()
  })

  test('数据集版本切换保持既有输入输出字段', async ({ page, request }) => {
    const payload = await resetDatasets(request)
    const before = await request.get(`${ENDPOINT}/executions/${payload.失败批次编号}`).then((r) => r.json())
    const beforeKeys = Object.keys(before).sort()

    await page.getByTestId('rex-version').click()
    await expect(page.getByTestId('rex-version-modal')).toBeVisible()
    await page.getByTestId('rex-version-select').selectOption('v2')
    await page.getByTestId('rex-version-submit').click()
    await expect(page.getByTestId('rex-version-modal')).toHaveCount(0)

    const meta = await request.get(`${ENDPOINT}/dataset-version`).then((r) => r.json())
    expect(meta.active_version).toBe('v2')
    const after = await request.get(`${ENDPOINT}/executions/${payload.失败批次编号}`).then((r) => r.json())
    expect(Object.keys(after).sort()).toEqual(beforeKeys)
    expect(after).toEqual(before)

    // 切回 v1 避免影响其他用例
    await request.post(`${ENDPOINT}/dataset-version/switch`, { data: { version: 'v1' } })
  })
})

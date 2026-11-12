<template>
  <div>
    <div class="toolbar">
      <select v-model="filters.status" @change="load" style="width:160px">
        <option value="">全部状态</option>
        <option value="PENDING">待审批</option>
        <option value="APPROVED">已通过未合并</option>
        <option value="REJECTED">已驳回</option>
        <option value="CONSOLIDATED">已合并出具</option>
      </select>
      <button class="primary" @click="openSubmit">提交更正申请</button>
      <button @click="load">刷新</button>
      <div class="spacer"></div>
      <span class="muted">未审批不入账 · 归档账本与关闭结论只读 · 同周期合并出具一份说明</span>
    </div>

    <div class="grid cols-2">
      <div class="card">
        <div class="card-title">关闭周期更正申请</div>
        <div class="table-wrap">
          <table class="data">
            <thead>
              <tr>
                <th>编号</th><th>类型</th><th>周期</th><th>渠道</th>
                <th>状态</th><th>提交人</th><th>审批人</th><th></th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="c in rows" :key="c.id">
                <td class="mono">{{ c.correction_ref }}</td>
                <td><span class="badge" :class="c.correction_type">{{ typeLabel(c.correction_type) }}</span></td>
                <td class="mono">{{ c.report_month }}</td>
                <td>{{ c.channel }}</td>
                <td><span class="badge" :class="statusClass(c.status)">{{ statusLabel(c.status) }}</span></td>
                <td>{{ c.submitted_by }}</td>
                <td>{{ c.approver }}</td>
                <td><button class="sm" @click="openDetail(c)">详情</button></td>
              </tr>
            </tbody>
          </table>
          <div v-if="!rows.length" class="empty">暂无更正申请</div>
        </div>
      </div>

      <div class="card">
        <div class="card-title">合并更正说明（同一关闭周期只出具一份）</div>
        <div class="table-wrap">
          <table class="data">
            <thead><tr><th>说明编号</th><th>周期/渠道</th><th>更正笔数</th><th>发布人</th><th></th></tr></thead>
            <tbody>
              <tr v-for="s in statements" :key="s.id">
                <td class="mono">{{ s.statement_ref }}</td>
                <td>{{ s.report_month }} / {{ s.channel }}</td>
                <td>{{ s.correction_count }}</td>
                <td>{{ s.published_by }}</td>
                <td><button class="sm" @click="openStatement(s)">查看</button></td>
              </tr>
            </tbody>
          </table>
          <div v-if="!statements.length" class="empty">尚无合并更正说明</div>
        </div>
        <div class="pill-row section-gap">
          <select v-model="publishReportId" style="width:240px">
            <option :value="null">选择关闭周期报表…</option>
            <option v-for="r in closedReports" :key="r.id" :value="r.id">
              {{ r.report_month }} / {{ r.channel }} / {{ r.merchant_id }} (v{{ r.current_version }})
            </option>
          </select>
          <input v-model="publishOperator" style="width:150px" placeholder="发布人工号" />
          <button class="primary" :disabled="!publishReportId" @click="publish">合并出具更正说明</button>
          <button :disabled="!publishReportId" @click="consolidateOne">一键收口该周期</button>
          <button @click="consolidateAll">收口全部周期</button>
        </div>
      </div>
    </div>

    <!-- 提交弹窗 -->
    <div v-if="showSubmit" class="modal-mask" @click.self="showSubmit = false">
      <div class="modal">
        <h3>提交关闭周期更正申请</h3>
        <p class="soft" style="font-size:12.5px;margin:0 0 14px">
          提交后生成待审批差额与新版本报表预览，<b>不会直接修改已归档原始账本与关闭结论</b>；
          审批通过才过账，驳回则永不进入账本。
        </p>
        <label class="field"><span>更正类型</span>
          <select v-model="form.correction_type">
            <option value="SUPPLEMENT">补单（关闭后迟到的业务事实）</option>
            <option value="PARSING_FIX">解析修复（已入账事实的金额更正）</option>
          </select>
        </label>
        <div class="grid cols-2" style="gap:10px">
          <label class="field"><span>渠道</span><input v-model="form.channel" /></label>
          <label class="field"><span>商户</span><input v-model="form.merchant_id" /></label>
        </div>
        <label class="field"><span>归属关闭周期（YYYY-MM）</span>
          <input v-model="form.report_month" placeholder="2026-10" />
        </label>
        <label class="field"><span>更正原因</span>
          <textarea rows="2" v-model="form.reason"></textarea>
        </label>
        <label class="field"><span>影响范围</span>
          <textarea rows="2" v-model="form.impact_scope" placeholder="受影响订单/渠道/金额/报表"></textarea>
        </label>
        <div class="grid cols-2" style="gap:10px">
          <label class="field"><span>提交人</span><input v-model="form.operator" /></label>
          <label class="field"><span>指定审批人（不得与提交人相同）</span><input v-model="form.approver" /></label>
        </div>

        <div v-if="form.correction_type === 'SUPPLEMENT'">
          <label class="field"><span>补单原始事件 payload（JSON，审批前不入账）</span>
            <textarea rows="8" v-model="form.supplementJson" class="mono"></textarea>
          </label>
        </div>
        <div v-else>
          <label class="field"><span>被修复业务事件 ID</span>
            <input v-model="form.fixBusinessEventId" type="number" />
          </label>
          <div class="grid cols-2" style="gap:10px">
            <label class="field"><span>正确本金</span>
              <input v-model="form.correctedAmount" type="number" step="0.01" />
            </label>
            <label class="field"><span>正确手续费（可空）</span>
              <input v-model="form.correctedFee" type="number" step="0.01" />
            </label>
          </div>
          <label class="field"><span>修复说明</span><input v-model="form.fixNote" /></label>
        </div>

        <div class="modal-actions">
          <button @click="showSubmit = false">取消</button>
          <button class="primary" @click="doSubmit">提交申请</button>
        </div>
      </div>
    </div>

    <!-- 详情弹窗 -->
    <div v-if="current" class="modal-mask" @click.self="current = null">
      <div class="modal" style="width:720px">
        <h3>
          {{ current.correction_ref }}
          <span class="badge" :class="statusClass(current.status)" style="margin-left:8px">{{ statusLabel(current.status) }}</span>
        </h3>
        <p class="soft" style="margin:0 0 4px">{{ current.reason }}</p>
        <p class="muted" style="margin:0 0 12px;font-size:12.5px">
          影响范围：{{ current.impact_scope }} · 周期 {{ current.report_month }} / {{ current.channel }} ·
          提交 {{ current.submitted_by }} → 审批 {{ current.approver }}
        </p>
        <p class="muted" style="font-size:12.5px;margin:0 0 12px"
           v-if="!current.posted_to_ledger">
          当前状态未进入账本；账本仅会在审批通过后追加差额分录。
        </p>

        <div v-if="current.status === 'PENDING'" class="pill-row" style="margin-bottom:14px">
          <input v-model="decision.operator" style="width:150px" placeholder="审批人工号" />
          <input v-model="decision.comment" style="width:260px" placeholder="审批意见" />
          <button class="sm primary" @click="doApprove">通过</button>
          <button class="sm danger" @click="doReject">驳回</button>
        </div>

        <div class="card-title">差额记录（提交时生成，未审批不入账）</div>
        <div class="table-wrap">
          <table class="data">
            <thead>
              <tr><th>#</th><th>订单</th><th>渠道参考</th><th>行类型</th><th>科目</th>
                <th>方向</th><th class="num">结算币</th><th class="num">记账币</th>
                <th class="num">净影响(CNY)</th></tr>
            </thead>
            <tbody>
              <tr v-for="d in deltas" :key="d.id">
                <td class="mono muted">{{ d.line_no }}</td>
                <td class="mono">{{ d.order_ref || '—' }}</td>
                <td class="mono">{{ d.channel_ref || '—' }}</td>
                <td>{{ d.line_type }}</td>
                <td class="mono">{{ d.account }}</td>
                <td>{{ d.direction }}</td>
                <td class="num">{{ d.settle_amount }} {{ d.settle_currency }}</td>
                <td class="num">{{ d.book_amount }} {{ d.book_currency }}</td>
                <td class="num" :class="Number(d.impact_book_amount) > 0 ? 'pos' : Number(d.impact_book_amount) < 0 ? 'neg' : 'muted'">
                  {{ Number(d.impact_book_amount) ? d.impact_book_amount : '—' }}
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div class="section-gap" v-if="current.impact_detail && current.impact_detail.affected_orders">
          <div class="card-title">审批通过后的影响说明</div>
          <p class="soft" style="font-size:12.5px">
            受影响订单 {{ current.impact_detail.affected_orders.map(o => o.order_ref).filter(Boolean).join('、') || '—' }} ·
            渠道 {{ current.channel }} ·
            金额 {{ current.impact_detail.total_book }} CNY ·
            报表版本
            {{ (current.impact_detail.affected_reports || []).map(r => r.new_version).join('、') }}
          </p>
          <p class="muted" style="font-size:12px">
            账本守恒复核：{{ current.conservation_check && current.conservation_check.balanced ? '通过' : '未校验/不通过' }}
            · 差额业务事件 #{{ current.delta_business_event_id || '—' }}
            · 合并说明 #{{ current.statement_id || '待合并' }}
          </p>
        </div>

        <div class="section-gap">
          <div class="card-title">新版本报表预览（提交时口径，未落正式版本）</div>
          <pre class="json" style="max-height:220px">{{ JSON.stringify(current.preview_report || {}, null, 2) }}</pre>
        </div>

        <div class="section-gap">
          <div class="card-title">审批轨迹（只追加）</div>
          <ul class="timeline">
            <li v-for="a in actions" :key="a.id">
              <div><b>{{ actionLabel(a.action_type) }}</b> · {{ a.operator }}</div>
              <div v-if="a.comment" class="soft" style="font-size:12.5px">{{ a.comment }}</div>
              <div class="t-meta">{{ fmtTime(a.created_at) }}</div>
            </li>
            <li v-if="!actions.length" class="muted">暂无审批记录</li>
          </ul>
        </div>
        <div class="modal-actions"><button @click="current = null">关闭窗口</button></div>
      </div>
    </div>

    <!-- 说明弹窗 -->
    <div v-if="statement" class="modal-mask" @click.self="statement = null">
      <div class="modal" style="width:720px">
        <h3>{{ statement.statement_ref }}</h3>
        <p class="soft" style="font-size:12.5px;margin:0 0 12px">
          {{ statement.report_month }} / {{ statement.channel }} / {{ statement.merchant_id }} ·
          合并 {{ statement.correction_count }} 笔已通过更正 · 发布人 {{ statement.published_by }}
        </p>
        <pre class="json" style="max-height:60vh">{{ JSON.stringify(statement.snapshot, null, 2) }}</pre>
        <div class="modal-actions"><button @click="statement = null">关闭窗口</button></div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { computed, onMounted, reactive, ref } from 'vue'
import api from '../api'

const rows = ref([])
const statements = ref([])
const reports = ref([])
const current = ref(null)
const deltas = ref([])
const actions = ref([])
const showSubmit = ref(false)
const statement = ref(null)
const publishReportId = ref(null)
const publishOperator = ref('finance.lead')
const filters = reactive({ status: 'PENDING' })
const decision = reactive({ operator: 'finance.lead', comment: '' })
const form = reactive({
  correction_type: 'SUPPLEMENT', channel: 'STRIPE', merchant_id: 'M1',
  report_month: '2026-10', reason: '', impact_scope: '',
  operator: 'finance.chen', approver: 'finance.lead',
  supplementJson: JSON.stringify({
    event_type: 'REFUND_SUCCEEDED', entity_type: 'REFUND',
    entity_ref: 'R-008-LATE', order_ref: 'ORD-008', channel_ref: 'CH-P008',
    channel: 'STRIPE', merchant_id: 'M1',
    business_key: 'REFUND_SUCCEEDED:R-008-LATE:v1',
    event_time: '2026-10-28T10:00:00+00:00',
    amount: '10', fee_amount: '0.3',
    txn_currency: 'USD', settle_currency: 'USD', book_currency: 'CNY',
    decimal_places: 2,
  }, null, 2),
  fixBusinessEventId: '', correctedAmount: '', correctedFee: '', fixNote: '解析修复',
})

const closedReports = computed(() => reports.value.filter((r) => r.status === 'CLOSED'))
const typeLabel = (t) => ({ SUPPLEMENT: '补单', PARSING_FIX: '解析修复' }[t] || t)
const statusLabel = (s) => ({
  PENDING: '待审批', APPROVED: '已通过未合并', REJECTED: '已驳回', CONSOLIDATED: '已合并出具',
}[s] || s)
const statusClass = (s) => ({
  PENDING: 'OPEN', APPROVED: 'RESOLVED', REJECTED: 'UNKNOWN', CONSOLIDATED: 'CLOSED',
}[s] || s)
const actionLabel = (a) => ({
  SUBMIT: '提交', APPROVE: '审批通过', REJECT: '审批驳回', CONSOLIDATE: '合并出具',
}[a] || a)
const fmtTime = (t) => (t ? new Date(t).toLocaleString('zh-CN') : '')

function toast(msg, ok) {
  const el = document.createElement('div')
  el.className = `toast ${ok ? 'ok' : 'err'}`
  el.textContent = msg
  document.body.appendChild(el)
  setTimeout(() => el.remove(), 3600)
}

async function load() {
  rows.value = await api.corrections({ status: filters.status || undefined })
  statements.value = await api.statements()
  reports.value = await api.reports()
}
function openSubmit() { showSubmit.value = true }
function openStatement(s) { statement.value = null; api.statement(s.id).then((d) => { statement.value = d }) }
async function openDetail(c) {
  current.value = c
  const d = await api.correction(c.id)
  deltas.value = d.deltas
  actions.value = d.actions
  decision.operator = current.value.approver
  decision.comment = ''
}
async function doSubmit() {
  let payload
  try {
    if (form.correction_type === 'SUPPLEMENT') {
      payload = { supplement: JSON.parse(form.supplementJson) }
    } else {
      payload = {
        parsing_fix: {
          business_event_id: Number(form.fixBusinessEventId),
          corrected_amount: form.correctedAmount,
          corrected_fee: form.correctedFee === '' ? null : form.correctedFee,
          note: form.fixNote,
        },
      }
    }
  } catch { return toast('payload JSON 格式错误', false) }
  try {
    await api.submitCorrection({
      correction_type: form.correction_type, channel: form.channel,
      merchant_id: form.merchant_id, report_month: form.report_month,
      reason: form.reason, impact_scope: form.impact_scope,
      approver: form.approver, operator: form.operator, ...payload,
    })
    showSubmit.value = false
    toast('更正申请已提交，等待审批（差额未入账）', true)
    await load()
  } catch (e) { toast(e.message, false) }
}
async function doApprove() {
  try {
    const r = await api.approveCorrection(current.value.id, decision.operator, decision.comment)
    toast(`审批通过：差额已过账、报表新版本 v${r.report_version.version}、守恒复核通过`, true)
    current.value = null
    await load()
  } catch (e) { toast(e.message, false) }
}
async function doReject() {
  try {
    await api.rejectCorrection(current.value.id, decision.operator, decision.comment)
    toast('已驳回：差额永不进入账本', true)
    current.value = null
    await load()
  } catch (e) { toast(e.message, false) }
}
async function publish() {
  try {
    const s = await api.publishStatement(publishReportId.value, publishOperator.value)
    toast(`已合并出具 ${s.statement_ref}（${s.correction_count} 笔更正）`, true)
    await load()
  } catch (e) { toast(e.message, false) }
}
async function consolidateOne() {
  try {
    const r = await api.consolidateStatements(publishOperator.value, publishReportId.value)
    toast(r.count ? `已收口 ${r.count} 个周期` : '没有待合并的已通过更正', true)
    await load()
  } catch (e) { toast(e.message, false) }
}
async function consolidateAll() {
  try {
    const r = await api.consolidateStatements(publishOperator.value)
    toast(r.count ? `已收口 ${r.count} 个周期` : '没有待合并的已通过更正', true)
    await load()
  } catch (e) { toast(e.message, false) }
}
onMounted(load)
</script>

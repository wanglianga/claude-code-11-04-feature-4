# 数据层设计（四层 + 只追加）

## 分层总览

```
来源系统                L1 原始事件层        L2 标准业务账本层                 L3 结算匹配层              L4 月度汇总层
CDC/Webhook/轮询/人工 → raw_events       → business_events（归一化）       settlement_batches       monthly_reports
                         （证据原样留存）     ledger_entries（双分录/三币种）  settlement_lines         monthly_report_versions
                                            fx_rates（历史汇率版本）         bank_transactions        closed_period_corrections(+deltas/actions)
                                            order/payment_states（投影）      settlement_matches       correction_statements
                                                                             discrepancies(+actions)
```

### L1 原始事件层 `raw_events`
- 唯一键 `(source, idempotency_key)`：CDC 用 LSN/消息 ID，webhook 用 event_id，轮询用游标+渠道参考号。
- 重放/重复推送在 L1 即被丢弃，原始报文 JSONB 原样留存。
- 仅 `status / processed_at / reject_reason` 生命周期列允许更新，证据列由触发器锁定。

### L2 标准业务账本层
- `business_events`：稳定业务键 `business_key` 全局唯一，多来源同一事实只入账一次；`event_version` + `supersedes_event_id` 表达修订链。
- `ledger_entries`：每个业务事件一组**借贷平衡**双分录；每行同时保留
  - 交易币 `txn_amount/txn_currency`、结算币 `settle_*`、记账币 `book_*`；
  - 两级汇率 `txn_to_settle_rate / settle_to_book_rate`、`rate_source`、`rate_effective_at`；
  - 精度 `decimal_places` 与 `raw_amount` 原始金额；
  - `effective_month` 业务归属期（更正账/补单可能晚于归属期过账）。
- `fx_rates`：只追加，更正 = 新版本（`is_correction`），历史事件永远按其发生时点汇率计量。
- `order_states / payment_states`：可更新的**投影**，按 `event_time` 重放；迟到旧事件不允许把状态打回。

### L3 结算匹配层
- 匹配两级：`ORDER_TO_SETTLEMENT`（订单事件↔结算行）、`SETTLEMENT_TO_BANK`（结算单↔银行流水，支持一笔流水合并多单、部分分配）。
- 差异五类：`AMOUNT / FEE / FX / TIMING / UNKNOWN`，均带 `dedup_key` 支持幂等重跑。
- 引擎只写 `PROPOSED` 匹配与 `OPEN` 差异；`UNKNOWN` 必须人工调账后才能关闭。
- `discrepancy_actions` 只追加，完整保留认领/附件/建议/过账/关闭轨迹。

### L4 月度汇总层
- `monthly_report_versions`：快照 JSONB 不可变；新版本产生时旧版本仅 `version_status` 置 `SUPERSEDED`（触发器放行该列），快照行本身禁改禁删。
- 快照内置 `conservation_check` 借贷守恒校验与指纹，内容无变化时复用当前版本。

### L4 关闭周期更正审批
- `closed_period_corrections`：关闭周期补单/解析修复的**申请**。提交时写入更正原因、影响范围、指定审批人与
  `preview_report`（新版本报表预览）。审批状态机 `PENDING -> APPROVED/REJECTED`、`APPROVED -> CONSOLIDATED`，
  `REJECTED/CONSOLIDATED` 为终态；申请证据列（原因/影响范围/提交人/预览）不可改，终态行整体不可改。
- `correction_deltas`：只追加的**差额记录**。每行是一笔标准账本借贷行（`direction` 单列、金额为正），
  另带带方向的 `impact_settle_amount/impact_book_amount`（净应收影响，只记在主影响行）供合并说明汇总。
  提交即生成，审批通过前**不产生任何 `ledger_entries`**。
- `correction_actions`：只追加的审批轨迹（`SUBMIT / APPROVE / REJECT / CONSOLIDATE`）。
- `correction_statements`：只追加的**合并更正说明**。同一 `report_id`（同一关闭周期）的全部
  `APPROVED` 未结更正一次性合并为一份快照，列出受影响订单号、渠道、金额与报表版本；
  发布后相关更正置 `CONSOLIDATED`，不会逐笔零散发布。
- 审批通过只追加一条 `CORRECTION_ADJUSTMENT` 业务事件与借贷平衡分录（`effective_month` 归属关闭周期），
  随后复核全账本与周期口径守恒，再追加正式报表版本；原始账本行、归档快照、关闭结论均不被修改。

## 只追加触发器（PostgreSQL）

`app/init_db.py` 为以下表安装 `BEFORE UPDATE OR DELETE` 与 `BEFORE TRUNCATE` 触发器：
`raw_events, business_events, fx_rates, ledger_entries, discrepancy_actions, monthly_report_versions,
correction_deltas, correction_actions, correction_statements`；
`closed_period_corrections` 使用同一函数但特判审批状态机（见上）。

- 任何 UPDATE/DELETE/TRUNCATE 直接抛异常（`raw_events` 生命周期列、报表版本状态列、更正审批状态机例外，证据列仍不可改）。
- 被外键引用的表，TRUNCATE 还会先被外键约束拦截。
- 测试重置使用 `DROP SCHEMA public CASCADE`（DDL 级，不受行触发器限制）。

## 关闭结论保护（额外触发器）

`recon_protect_closed_conclusion` 施加于 `settlement_batches`、`monthly_reports`：
- 批次/报表一旦 `CLOSED`：不可改回 `OPEN`；`closed_at` 不可改写；批次的单号/版本/渠道/商户/周期/币种/
  总额/手续费/净额/关闭人，报表的渠道/商户/币种/月份均不可改；`current_version` 只增不减。
- 因此「关闭后出现补单或解析修复」只能通过审批流在 L4 追加差额分录与新版本，不存在直接改归档账本或关闭结论的路径。
- 另有摄入守卫（`services/ledger.py::_guard_closed_period`）：已关闭周期的捕获/退款/拒付等事实类事件
  若走常规 `/api/events/ingest` 直接补录会被拒绝，必须提交 `/api/corrections`。

## 金额与汇率规则
- 统一 `Numeric(20,6)` 存储，展示按 `decimal_places`（默认 2）`ROUND_HALF_UP`。
- 分录平衡以记账币校验；结算币/交易币由记账币按事件时点汇率反推，三种币种下各自守恒。
- 汇率更正不回写历史分录，而是按受影响事件追加 `FX_GAIN_LOSS` 更正分录并产出新报表版本。

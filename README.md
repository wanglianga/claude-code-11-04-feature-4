# 订单到清结算差异处理中心

跨境电商财务的订单 → 支付 → 退款/拒付 → 渠道结算 → 银行入账全链路差异处置系统。
FastAPI + PostgreSQL（四层数据架构）+ Vue 3，解决重复记账、乱序状态破坏、手续费/汇率后补、
合并拆分入账、关闭周期被静默改写等问题。

## 一、架构与关键约束

| 层 | 表 | 职责 |
|---|---|---|
| L1 原始事件层 | `raw_events` | CDC/webhook/轮询/人工消息原样留存，`(source, idempotency_key)` 去重 |
| L2 标准业务账本层 | `business_events` `ledger_entries` `fx_rates` `order_states` `payment_states` | 归一化业务事件、双分录三币种账本、历史汇率版本、按业务时间重放的状态投影 |
| L3 结算匹配层 | `settlement_batches/lines` `bank_transactions` `settlement_matches` `discrepancies(+actions)` | 两层匹配、五类差异、人工处置工作流 |
| L4 月度汇总层 | `monthly_reports` `monthly_report_versions` `closed_period_corrections(+deltas/actions)` `correction_statements` | 不可变快照与版本链，关闭周期更正审批与合并说明 |

关键机制：
- **两层幂等**：L1 来源消息去重 + L2 `business_key` 去重，CDC 重放/重复 webhook/轮询补拉均不重复进账本。
- **乱序安全**：状态投影按业务发生时间 `event_time` 重放，迟到的旧事件不能回退订单/支付状态。
- **三币种留痕**：每笔金额保留交易币、结算币、记账币，以及汇率来源、适用时点、精度规则和原始金额。
- **只追加账本**：9 张证据/审批表由 PostgreSQL 触发器拒绝 UPDATE/DELETE/TRUNCATE（详见 `db/DATA_LAYER.md`）。
- **自动任务只提示**：对账引擎只生成 `PROPOSED` 匹配与 `OPEN` 差异；`UNKNOWN` 未知差必须人工调账留痕后才能关闭。
- **关闭后更正必须审批**：关闭周期补单/解析修复直接摄入会被拒绝；只能提交更正申请（原因+影响范围+审批人），
  系统生成待审批**差额记录**与新版本**报表预览**，审批通过才过账并追加报表版本，驳回则永不进入账本；
  同一关闭周期多笔已通过未结更正**合并出具一份更正说明**，不逐笔零散发布。
  **已归档原始账本、已关闭批次金额与关闭人、报表关闭结论**由触发器保护，不可直接修改。

## 二、一键启动（Docker Compose，推荐）

前置：Docker + Docker Compose。

```bash
cd /workspace/04
docker compose up -d --build          # 启动 db / backend / frontend
docker compose run --rm seed          # 装载 8 类模拟场景并跑完整处置/更正/版本链
```

访问：
- 前端（财务处置队列、批次对账、事件账本、报表版本、关闭周期更正审批）：http://localhost:8080
- 后端 API：http://localhost:8000 ，健康检查 http://localhost:8000/health

停止与清库：
```bash
docker compose down            # 停止
docker compose down -v         # 停止并删除数据库卷（彻底重置）
```

## 三、本地开发启动（无 Docker 时）

需要 Python 3.11+、Node 20+、PostgreSQL 14+。

### 3.1 准备数据库
```bash
createdb recon   # 或在已有实例中建库，账号密码按需调整
```

### 3.2 后端
```bash
cd backend
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export RECON_DATABASE_URL="postgresql+asyncpg://<user>:<pwd>@<host>:5432/recon"
export PYTHONPATH="$(pwd)"
uvicorn app.main:app --reload --port 8000     # 启动时自动建表并安装只追加触发器
python scripts/seed.py --reset                 # 可选：装载模拟场景数据
```

### 3.3 前端
```bash
cd frontend
npm install
VITE_API_TARGET=http://localhost:8000 npm run dev    # http://localhost:5173，/api 自动代理
```

前端生产构建：`npm run build`（产物在 `frontend/dist`，容器内由 Nginx 托管并反代后端）。

## 四、测试账号 / 操作身份

系统为内部财务工具，处置动作以**操作人工号**标识（无独立登录体系）：
- 演示与种子脚本统一使用操作人：`finance.chen`；**审批人**：`finance.lead`（提交人与审批人不得同一人，后端强制）。
- 前端处置弹窗的“处理人”字段默认填充 `finance.chen`，可改为实际工号；所有认领、调账、关闭动作都带操作人留痕。
- 关闭周期更正审批页的提交人默认 `finance.chen`、审批人默认 `finance.lead`，审批动作只有申请登记的审批人可执行。
- 数据库连接（Compose）：用户 `recon` / 密码 `recon` / 库 `recon`。

## 五、验证方式

### 5.1 自动化验收（推荐）
```bash
cd backend && source .venv/bin/activate
export RECON_DATABASE_URL="postgresql+asyncpg://recon:recon@<host>:5432/recon"
bash scripts/verify.sh        # = pytest 全套 + seed 端到端
# 或分步：
python -m pytest tests/ -q    # 36 个用例
python scripts/seed.py --reset
```

测试覆盖（`backend/tests/`）：
| 文件 | 验收点 |
|---|---|
| `test_01_idempotency.py` | CDC 重放、重复 webhook、轮询补拉不重复进账本 |
| `test_02_ordering.py` | webhook 乱序下支付状态正确、旧订单状态迟到不回退 |
| `test_03_lifecycle.py` | 部分退款、拒付反转、账本借贷守恒、三币种与汇率溯源 |
| `test_04_reconciliation.py` | 两层匹配、五类差异归因、对账幂等、未知差禁止自动关闭、合并入账分配 |
| `test_05_workflow.py` | 认领/附件/调账建议/过账/关闭轨迹、关闭前置校验、快照生成 |
| `test_06_corrections.py` | 结算修订新版本、汇率更正账、报表版本链、历史快照不被改写、关闭后补单经审批出新版本 |
| `test_07_append_only.py` | UPDATE/DELETE/TRUNCATE 触发器、证据列不可改、生命周期列可改 |
| `test_08_correction_approval.py` | 提交只生成差额+预览且不入账、直接补录被拒、驳回永不入账、审批后过账/守恒复核/新报表版本/影响说明、解析修复不动历史分录、同周期多笔合并一份说明、关闭结论列不可回改、审批状态机不可回退 |

### 5.2 模拟场景（`backend/app/demo/events.py`，结算周期 2026-10）
S1 CDC 重放 · S2 webhook 乱序与状态回退防护 · S3 支付后延迟入账（11-15 才到银行）·
S4 部分退款（ORD-005 退 40/100）· S5 拒付反转（ORD-006）· S6 结算单修订 v1→v2（手续费后补）·
S7 汇率更正 USD/CNY 7.20→7.25 · S8 多笔合并银行入账；另含金额差（ORD-007）、汇率差（ORD-009 EUR）、
幽灵结算行与银行残差两类未知差、关闭周期补单与解析修复（走更正审批）。

### 5.2.1 关闭周期更正审批操作路径（前端 / API）
1. 进入前端「关闭周期更正审批」页，点击**提交更正申请**：填写更正类型（补单/解析修复）、渠道/商户、
   归属关闭周期、**更正原因**、**影响范围**、指定**审批人**（不得与提交人相同）。
   - 补单：粘贴待补录原始事件 payload，系统在 L1 留存证据、生成待审批差额行与报表新版本预览，
     **不进入 L2 账本**。
   - 解析修复：填写被修复业务事件 ID 与正确本金/手续费，系统只计算差额，历史分录行保持原值。
2. 审批人在详情弹窗点击**通过**或**驳回**：
   - 通过：按差额生成一条 `CORRECTION_ADJUSTMENT` 事件与平衡分录 → 重新校验账本守恒 →
     追加新报表版本（旧版本 `SUPERSEDED` 原样保留）→ 输出影响说明（订单号/渠道/金额/报表版本）。
   - 驳回：终态，差额永不进入账本，报表版本与归档快照不变。
3. 同一关闭周期存在多笔**已通过且未结**更正时，点击**合并出具更正说明**（或「一键收口」），
   系统汇总为一份 `CS-YYYY-MM-CHANNEL-<report_id>-<序号>` 说明并列出受影响订单、渠道、金额与报表版本；
   已发布的更正置为 `CONSOLIDATED`，不会逐笔零散发布。
4. 对应 API：`POST /api/corrections`、`/api/corrections/{id}/approve|reject`、
   `GET /api/corrections/{id}/impact`、`POST /api/reports/{id}/correction-statement`、
   `POST /api/correction-statements/consolidate`、`GET /api/correction-statements`。

### 5.3 本环境实际执行过的验证（结论如实记录）
- `python -m pytest tests/ -q`：**36 passed**（用户态 PostgreSQL 15，端口 5433）。
- `python scripts/seed.py --reset`：端到端跑通，31 条原始消息中 2 条重复被丢弃、生成 28 个业务事件；
  v1/v2 批次差异全部人工处置关闭；汇率更正追加 72 行平衡更正分录；
  **关闭周期补单直接摄入被拒** → 提交更正申请（差额 4 行、报表预览 v4、分录数不变）→
  解析修复申请被驳回（分录数不变）→ 补单审批通过（过账 4 行、守恒复核通过、报表 v4）→
  第二笔解析修复审批通过（报表 v5）→ **两笔已通过更正合并为一份说明**
  `CS-2026-10-STRIPE-0001-01`（受影响订单 ORD-008、渠道 STRIPE、净影响 -34.08 CNY）；
  最终报表 v5 CURRENT（v1~v4 SUPERSEDED），全账本借贷各 17,935.08 CNY 平衡。
- API 冒烟：`/health`、`/api/reports`、`/api/corrections`、`/api/correction-statements`；
  `POST /api/corrections` 提交后 `current_version` 不变；`POST /api/corrections/{id}/approve`
  返回守恒复核 `balanced=true` 且报表版本 +1；重复摄入同一消息返回 `duplicate=true` 且账本无变化。
- 前端 `npm run build` 构建成功（含新增“关闭周期更正审批”页面）。
- 说明：本环境无 Docker 守护进程，**未实际执行镜像构建**，Dockerfile/compose 已按标准编写并通过 YAML 校验，
  请在具备 Docker 的环境按第二节验证。

## 六、主要 API（FastAPI，OpenAPI 见 `/docs`）

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/events/ingest` `/api/events/ingest-batch` | 原始事件摄入（两层幂等） |
| POST | `/api/fx/rates` | 汇率发布/更正（自动版本号） |
| GET | `/api/events/raw` `/api/events/business` | L1/L2 事件查询 |
| GET | `/api/ledger/entries` `/api/ledger/balances` | 分录与守恒视图 |
| GET | `/api/states/orders` `/api/states/payments` | 状态投影 |
| POST | `/api/reconcile/run-all` | 自动对账（只生成建议） |
| GET/POST | `/api/batches`、`/api/batches/{id}/reconcile|close|snapshot` | 批次对账与关闭 |
| GET/POST | `/api/matches`、`/api/matches/{id}/confirm` | 两层匹配与人工确认 |
| GET/POST | `/api/discrepancies`、`/{id}/claim|attach|adjust-suggest|adjust-post|close` | 差异处置队列 |
| GET | `/api/reports`、`/api/reports/{id}/versions` | 月度报表与版本快照 |
| POST | `/api/reports/regenerate` | 汇率/修订等事后事件触发新报表版本 |
| GET/POST | `/api/corrections`、`/{id}` | 关闭周期更正申请列表/提交（差额+预览，不入账） |
| POST | `/api/corrections/{id}/approve`、`/reject` | 审批通过（过账+守恒复核+新版本+影响说明）/ 驳回 |
| GET | `/api/corrections/{id}/impact` | 影响说明（订单号/渠道/金额/报表版本） |
| GET/POST | `/api/correction-statements`、`/{id}` | 合并更正说明查询 |
| POST | `/api/reports/{id}/correction-statement` | 该关闭周期多笔已通过更正合并出具一份说明 |
| POST | `/api/correction-statements/consolidate` | 批量收口（每关闭周期一份，不逐笔零散发布） |

## 七、目录结构

```
.
├── backend/
│   ├── app/
│   │   ├── models.py            # 四层表结构与只追加清单
│   │   ├── enums.py             # 事件/状态/差异/科目枚举
│   │   ├── init_db.py           # 建表 + 只追加触发器
│   │   ├── services/
│   │   │   ├── ledger.py        # 幂等摄入、双分录、状态投影、关闭周期摄入守卫
│   │   │   ├── fx.py            # 历史汇率时点取值
│   │   │   ├── reconciliation.py# 两层匹配与五类差异引擎
│   │   │   ├── workflow.py      # 人工处置与批次关闭
│   │   │   ├── corrections.py   # 更正账与报表版本
│   │   │   └── correction_approval.py # 关闭周期更正审批、差额过账、合并说明
│   │   ├── routers/             # events / reconciliation API
│   │   └── demo/                # 8 类模拟事件与场景装载器
│   ├── tests/                   # 36 个 pytest 验收用例
│   └── scripts/seed.py verify.sh
├── frontend/                    # Vue 3 + Vite + vue-router + axios（含关闭周期更正审批页）
├── db/DATA_LAYER.md             # 数据层与触发器设计说明
└── docker-compose.yml           # db + backend + frontend(+seed profile)
```

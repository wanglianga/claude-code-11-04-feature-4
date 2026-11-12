"""关闭周期更正审批（补单 / 解析修复）。

规则：
  - 已关闭周期的原始账本、上报快照与关闭结论**只读**：本服务从不 UPDATE/DELETE
    business_events、ledger_entries、monthly_report_versions，也不改 settlement_batches /
    monthly_reports 的关闭金额与关闭人（数据库触发器二次保证）。
  - 提交 = 生成**待审批差额记录**（correction_deltas）+ **新版本报表预览**（只存在申请行上，
    不落 monthly_report_versions）。驳回的更正永远不进入账本。
  - 审批通过 = 按差额生成一条 CORRECTION_ADJUSTMENT 业务事件与借贷平衡分录，
    重新校验账本守恒，追加正式报表版本，并输出影响说明（订单号/渠道/金额/报表）。
  - 同一关闭周期存在多笔已通过且未结的更正时，合并出具**一份**更正说明
    （correction_statements），而不是逐笔零散发布。
"""
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from ..enums import (
    Account, CorrectionActionType, CorrectionStatus, CorrectionType,
    EventSource, EventType, EntityType,
)
from ..models import (
    BusinessEvent, ClosedPeriodCorrection, CorrectionAction, CorrectionDelta,
    CorrectionStatement, LedgerEntry, MonthlyReport, RawEvent, SettlementBatch,
)
from ..schemas import RawEventIn
from .corrections import build_snapshot, ensure_monthly_report_version
from .fx import get_rate
from .money import quantize


class CorrectionError(Exception):
    pass


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _dt(v) -> datetime:
    return datetime.fromisoformat(v) if isinstance(v, str) else v


def _fingerprint(payload: dict) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str).encode()
    ).hexdigest()


async def _log(db: AsyncSession, correction_id: int, action: str,
               operator: str, comment: str | None = None,
               detail: dict | None = None) -> None:
    db.add(CorrectionAction(
        correction_id=correction_id, action_type=action,
        operator=operator, comment=comment, detail=detail,
    ))


# ---------------------------------------------------------------- 定位关闭周期
async def resolve_closed_period(
    db: AsyncSession, channel: str, merchant_id: str, report_month: str
) -> tuple[MonthlyReport, SettlementBatch]:
    """找到归属的已关闭报表及其最后关闭的批次版本。未关闭周期不走审批流。"""
    report = (
        await db.execute(
            select(MonthlyReport).where(
                MonthlyReport.channel == channel,
                MonthlyReport.merchant_id == merchant_id,
                MonthlyReport.report_month == report_month,
            )
        )
    ).scalar_one_or_none()
    if report is None or report.status != "CLOSED":
        raise CorrectionError(
            f"{channel}/{merchant_id}/{report_month} 不是已关闭周期，"
            f"请按常规事件摄入流程处理"
        )
    batch = (
        await db.execute(
            select(SettlementBatch)
            .where(
                SettlementBatch.channel == channel,
                SettlementBatch.merchant_id == merchant_id,
                SettlementBatch.status == "CLOSED",
                func.to_char(SettlementBatch.period_end, "YYYY-MM") == report_month,
            )
            .order_by(SettlementBatch.version.desc())
            .limit(1)
        )
    ).scalar_one_or_none()
    if batch is None:
        raise CorrectionError(f"已关闭周期 {report_month} 找不到关闭批次")
    return report, batch


# ---------------------------------------------------------------- 差额计算
async def _pending_preview_entries(
    db: AsyncSession, report: MonthlyReport
) -> list[LedgerEntry]:
    """同周期全部待审批差额的预览分录（未落账），用于新版本报表预览。"""
    rows = (
        await db.execute(
            select(CorrectionDelta)
            .join(
                ClosedPeriodCorrection,
                ClosedPeriodCorrection.id == CorrectionDelta.correction_id,
            )
            .where(
                ClosedPeriodCorrection.report_id == report.id,
                ClosedPeriodCorrection.status == CorrectionStatus.PENDING,
            )
        )
    ).scalars().all()
    return [_preview_entry(d, report) for d in rows]


def _preview_entry(d: CorrectionDelta, report: MonthlyReport) -> LedgerEntry:
    return LedgerEntry(
        business_event_id=0, account=d.account, direction=d.direction,
        txn_amount=Decimal("0"), txn_currency=d.settle_currency,
        settle_amount=d.settle_amount, settle_currency=d.settle_currency,
        book_amount=d.book_amount, book_currency=d.book_currency,
        decimal_places=2, raw_amount=Decimal("0"),
        effective_month=report.report_month,
        channel=report.channel, merchant_id=report.merchant_id,
        order_ref=d.order_ref,
    )


async def _build_preview(
    db: AsyncSession, report: MonthlyReport, extra_deltas: list[CorrectionDelta]
) -> dict:
    """账本事实 + 同周期待审批差额 + 本次新差额 => 新版本报表预览（不改关闭结论）。"""
    pending = await _pending_preview_entries(db, report)
    pending += [_preview_entry(d, report) for d in extra_deltas]
    snapshot = await build_snapshot(
        db, report.channel, report.merchant_id, "CNY", report.report_month,
        extra_entries=pending,
    )
    snapshot["preview_of_report_version"] = report.current_version + 1
    snapshot["pending_corrections_included"] = True
    snapshot["fingerprint"] = _fingerprint(snapshot)
    return snapshot


def _delta(order_ref, channel_ref, line_type, account, direction,
           settle_amt, settle_ccy, book_amt, book_ccy,
           impact_book_amt=Decimal("0"), impact_settle_amt=Decimal("0"),
           memo=None) -> CorrectionDelta:
    return CorrectionDelta(
        correction_id=0, line_no=0, order_ref=order_ref, channel_ref=channel_ref,
        line_type=line_type, account=account, direction=direction,
        settle_amount=quantize(settle_amt), settle_currency=settle_ccy,
        book_amount=quantize(book_amt), book_currency=book_ccy,
        impact_settle_amount=quantize(impact_settle_amt),
        impact_book_amount=quantize(impact_book_amt), memo=memo,
    )


async def _supplement_deltas(
    db: AsyncSession, batch: SettlementBatch, raw: RawEvent
) -> tuple[list[CorrectionDelta], list[dict]]:
    """补单：按待补录原始事件的业务事实计算对账面（归属期）的影响。"""
    p = raw.payload
    event_type = p.get("event_type")
    order_ref = p.get("order_ref") or p.get("entity_ref")
    settle_ccy = p.get("settle_currency") or p.get("txn_currency") or batch.currency
    book_ccy = p.get("book_currency", "CNY")
    txn_ccy = p.get("txn_currency", settle_ccy)
    at = _dt(p.get("event_time") or raw.event_occurred_at)
    r1, _, _, _ = await get_rate(db, txn_ccy, settle_ccy, at)
    r2, _, _, _ = await get_rate(db, settle_ccy, book_ccy, at)
    amt = Decimal(str(p.get("amount", "0")))
    fee = Decimal(str(p.get("fee_amount", "0")))
    settle_amount = quantize(amt * r1)
    book_amount = quantize(settle_amount * r2)

    lines: list[dict] = []
    deltas: list[CorrectionDelta] = []
    net_settle = settle_amount - quantize(fee * r1)  # 净应收变动（本金 - 手续费）
    net_book = quantize(net_settle * r2)
    if event_type == EventType.PAYMENT_CAPTURED:
        deltas.append(_delta(order_ref, p.get("channel_ref"), "GROSS_CAPTURE",
                             Account.SETTLEMENT_RECEIVABLE, "DR",
                             settle_amount, settle_ccy, abs(book_amount), book_ccy,
                             impact_book_amt=net_book, impact_settle_amt=net_settle,
                             memo="补单：捕获应收"))
        deltas.append(_delta(order_ref, p.get("channel_ref"), "REVENUE",
                             Account.REVENUE, "CR",
                             settle_amount, settle_ccy, abs(book_amount), book_ccy,
                             memo="补单：确认收入"))
        if fee > 0:
            fee_settle = quantize(fee * r1)
            fee_book = quantize(fee_settle * r2)
            deltas.append(_delta(order_ref, p.get("channel_ref"), "FEE",
                                 Account.PAYMENT_FEE, "DR",
                                 fee_settle, settle_ccy, abs(fee_book), book_ccy,
                                 memo="补单：手续费"))
            deltas.append(_delta(order_ref, p.get("channel_ref"), "FEE_RECEIVABLE",
                                 Account.SETTLEMENT_RECEIVABLE, "CR",
                                 fee_settle, settle_ccy, abs(fee_book), book_ccy,
                                 memo="补单：手续费冲减应收"))
    elif event_type == EventType.REFUND_SUCCEEDED:
        deltas.append(_delta(order_ref, p.get("channel_ref"), "REFUND",
                             Account.REVENUE, "DR",
                             settle_amount, settle_ccy, abs(book_amount), book_ccy,
                             impact_book_amt=-net_book, impact_settle_amt=-net_settle,
                             memo="补单：退款冲回收入"))
        deltas.append(_delta(order_ref, p.get("channel_ref"), "REFUND_RECEIVABLE",
                             Account.SETTLEMENT_RECEIVABLE, "CR",
                             settle_amount, settle_ccy, abs(book_amount), book_ccy,
                             memo="补单：退款冲减应收"))
        if fee > 0:
            fee_settle = quantize(fee * r1)
            fee_book = quantize(fee_settle * r2)
            deltas.append(_delta(order_ref, p.get("channel_ref"), "REFUND_FEE",
                                 Account.SETTLEMENT_RECEIVABLE, "DR",
                                 fee_settle, settle_ccy, abs(fee_book), book_ccy,
                                 memo="补单：退还手续费"))
            deltas.append(_delta(order_ref, p.get("channel_ref"), "REFUND_FEE_OFFSET",
                                 Account.PAYMENT_FEE, "CR",
                                 fee_settle, settle_ccy, abs(fee_book), book_ccy,
                                 memo="补单：手续费退还冲回"))
    elif event_type == EventType.CHARGEBACK_OPENED:
        deltas.append(_delta(order_ref, p.get("channel_ref"), "CHARGEBACK",
                             Account.CHARGEBACK, "DR",
                             settle_amount, settle_ccy, abs(book_amount), book_ccy,
                             impact_book_amt=-book_amount, impact_settle_amt=-settle_amount,
                             memo="补单：拒付"))
        deltas.append(_delta(order_ref, p.get("channel_ref"), "CHARGEBACK_RECEIVABLE",
                             Account.SETTLEMENT_RECEIVABLE, "CR",
                             settle_amount, settle_ccy, abs(book_amount), book_ccy,
                             memo="补单：拒付冲减应收"))
    elif event_type == EventType.CHARGEBACK_REVERSED:
        deltas.append(_delta(order_ref, p.get("channel_ref"), "CHARGEBACK_REVERSAL",
                             Account.SETTLEMENT_RECEIVABLE, "DR",
                             settle_amount, settle_ccy, abs(book_amount), book_ccy,
                             impact_book_amt=book_amount, impact_settle_amt=settle_amount,
                             memo="补单：拒付撤销"))
        deltas.append(_delta(order_ref, p.get("channel_ref"), "CHARGEBACK_REVERSAL_OFFSET",
                             Account.CHARGEBACK, "CR",
                             settle_amount, settle_ccy, abs(book_amount), book_ccy,
                             memo="补单：拒付撤销冲回"))
    elif event_type == EventType.MANUAL_ADJUSTMENT:
        book_amt = Decimal(str(p.get("book_amount", "0")))
        deltas.append(_delta(order_ref, p.get("channel_ref"), "MANUAL",
                             p.get("debit_account", Account.MANUAL_ADJUSTMENT), "DR",
                             abs(book_amt), settle_ccy, abs(book_amt), book_ccy,
                             impact_book_amt=book_amt, impact_settle_amt=book_amt,
                             memo="补单：人工调增"))
        deltas.append(_delta(order_ref, p.get("channel_ref"), "MANUAL_OFFSET",
                             p.get("credit_account", Account.MANUAL_ADJUSTMENT), "CR",
                             abs(book_amt), settle_ccy, abs(book_amt), book_ccy,
                             memo="补单：人工调减"))
    else:
        raise CorrectionError(f"补单事件类型 {event_type} 不支持关闭周期更正")

    for d in deltas:
        lines.append({
            "order_ref": d.order_ref, "channel_ref": d.channel_ref,
            "line_type": d.line_type, "account": d.account,
            "direction": d.direction,
            "settle_amount": str(d.settle_amount),
            "book_amount": str(d.book_amount),
            "impact_settle_amount": str(d.impact_settle_amount),
            "impact_book_amount": str(d.impact_book_amount),
            "memo": d.memo,
        })
    return deltas, lines


def _parsing_fix_deltas(
    target: BusinessEvent, corrected_amount: Decimal, corrected_fee: Decimal,
    settle_ccy: str, book_ccy: str, rate: Decimal, note: str,
) -> tuple[list[CorrectionDelta], list[dict]]:
    """解析修复：对已入账事实按正确金额重算本金/手续费差额（只追加差额，不改原行）。"""
    p = target.payload
    old_gross = Decimal(str(p.get("amount", "0")))
    old_fee = Decimal(str(p.get("fee_amount", "0")))
    d_gross = quantize(corrected_amount - old_gross)
    d_fee = quantize(corrected_fee - old_fee)
    if d_gross == 0 and d_fee == 0:
        raise CorrectionError("解析修复金额与原事实一致，无差额可申请")

    order_ref = target.order_ref or target.entity_ref
    lines: list[dict] = []
    deltas: list[CorrectionDelta] = []

    def add(line_type, account, direction, settle_amt, memo,
            impact_amt=Decimal("0"), impact_settle=Decimal("0")):
        d = _delta(
            order_ref, target.channel_ref, line_type, account, direction,
            abs(settle_amt), settle_ccy, abs(quantize(settle_amt * rate)), book_ccy,
            impact_book_amt=impact_amt, impact_settle_amt=impact_settle, memo=memo,
        )
        deltas.append(d)
        lines.append({
            "order_ref": d.order_ref, "channel_ref": d.channel_ref,
            "line_type": d.line_type, "account": d.account,
            "direction": d.direction,
            "settle_amount": str(d.settle_amount),
            "book_amount": str(d.book_amount),
            "impact_settle_amount": str(d.impact_settle_amount),
            "impact_book_amount": str(d.impact_book_amount),
            "memo": d.memo,
        })

    # 解析修复是"调增/调减"语义：变化量为正 -> 正常方向；为负 -> 借贷反转
    # 净应收影响 = 本金变化 - 手续费变化，只记在首行避免重复汇总
    net_settle = quantize(d_gross - d_fee)
    net_book = quantize(net_settle * rate)
    if d_gross != 0:
        direction = "DR" if d_gross > 0 else "CR"
        reverse = "CR" if d_gross > 0 else "DR"
        add("FIX_GROSS", Account.SETTLEMENT_RECEIVABLE, direction, abs(d_gross),
            f"{note}：本金差额", impact_amt=net_book, impact_settle=net_settle)
        add("FIX_REVENUE", Account.REVENUE, reverse, abs(d_gross),
            f"{note}：收入差额")
    if d_fee != 0:
        direction = "DR" if d_fee > 0 else "CR"
        reverse = "CR" if d_fee > 0 else "DR"
        add("FIX_FEE", Account.PAYMENT_FEE, direction, abs(d_fee),
            f"{note}：手续费差额")
        add("FIX_FEE_RECEIVABLE", Account.SETTLEMENT_RECEIVABLE, reverse, abs(d_fee),
            f"{note}：手续费冲减应收")
    return deltas, lines


# ---------------------------------------------------------------- 提交
async def submit_correction(
    db: AsyncSession, *, correction_type: str, channel: str, merchant_id: str,
    report_month: str, reason: str, impact_scope: str, approver: str,
    operator: str, declared_impact: dict | None = None,
    supplement_payload: dict | None = None,
    parsing_fix: dict | None = None,
) -> ClosedPeriodCorrection:
    """提交关闭周期更正申请：生成差额记录与新版本报表预览，不动账本。"""
    if correction_type not in (CorrectionType.SUPPLEMENT, CorrectionType.PARSING_FIX):
        raise CorrectionError(f"不支持的更正类型 {correction_type}")
    if not reason.strip() or not impact_scope.strip():
        raise CorrectionError("更正原因与影响范围必须填写")
    if not approver.strip():
        raise CorrectionError("必须指定审批人")
    if approver.strip() == operator.strip():
        raise CorrectionError("提交人与审批人不得为同一人（审批须独立留痕）")

    report, batch = await resolve_closed_period(db, channel, merchant_id, report_month)
    report_month = report.report_month

    source_raw_id = target_be_id = None
    if correction_type == CorrectionType.SUPPLEMENT:
        if not supplement_payload:
            raise CorrectionError("补单更正必须携带待补录的原始事件")
        payload = dict(supplement_payload)
        if not payload.get("event_time"):
            raise CorrectionError("补单原始事件必须提供业务发生时间 event_time")
        if _dt(payload["event_time"]).strftime("%Y-%m") != report_month:
            raise CorrectionError(
                f"补单业务发生时间不属于关闭周期 {report_month}，"
                f"请按常规摄入流程处理"
            )
        raw_in = RawEventIn(
            idempotency_key=payload.get(
                "idempotency_key",
                f"CLOSED-CORRECTION:{report.id}:{_fingerprint(payload)[:16]}",
            ),
            source=EventSource.MANUAL,
            event_occurred_at=_dt(payload["event_time"]),
            topic="closed.period.supplement",
            payload=payload,
        )
        # L1 留存原始证据；L2 入账（process_event）被刻意跳过，待审批通过后才补充差额
        from .ledger import ingest_raw_event  # 避免循环导入
        raw, dup = await ingest_raw_event(db, raw_in)
        existing = (
            await db.execute(
                select(BusinessEvent).where(
                    BusinessEvent.business_key
                    == payload.get(
                        "business_key",
                        f"{payload.get('event_type')}:"
                        f"{payload.get('entity_ref')}:v{payload.get('event_version', 1)}",
                    )
                )
            )
        ).scalar_one_or_none()
        if existing is not None:
            raise CorrectionError("该业务事实已入账，关闭周期补单不同意重复补录")
        if not dup and raw.status == "RECEIVED":
            raw.status = "PENDING_APPROVAL"
            raw.reject_reason = "closed period supplement awaiting approval; not posted"
        source_raw_id = raw.id
        deltas, declared_lines = await _supplement_deltas(db, batch, raw)
    else:
        if not parsing_fix:
            raise CorrectionError("解析修复必须提供被修复事实与正确金额")
        target = await db.get(BusinessEvent, int(parsing_fix["business_event_id"]))
        if target is None:
            raise CorrectionError("被修复的业务事件不存在")
        if target.event_time.strftime("%Y-%m") != report_month:
            raise CorrectionError("解析修复目标不属于该关闭周期的归属月份")
        at = target.event_time
        settle_ccy = target.payload.get("settle_currency", batch.currency)
        book_ccy = target.payload.get("book_currency", "CNY")
        r2, _, _, _ = await get_rate(db, settle_ccy, book_ccy, at)
        deltas, declared_lines = _parsing_fix_deltas(
            target,
            Decimal(str(parsing_fix["corrected_amount"])) if parsing_fix.get("corrected_amount") is not None else Decimal(str(target.payload.get("amount", "0"))),
            Decimal(str(parsing_fix["corrected_fee"])) if parsing_fix.get("corrected_fee") is not None else Decimal(str(target.payload.get("fee_amount", "0"))),
            settle_ccy, book_ccy, r2, parsing_fix.get("note", "解析修复"),
        )
        target_be_id = target.id

    total_settle = quantize(sum((d.impact_settle_amount for d in deltas), Decimal("0")))
    total_book = quantize(sum((d.impact_book_amount for d in deltas), Decimal("0")))
    if abs(total_book) < Decimal("0.01"):
        raise CorrectionError("更正差额为 0，无需提交审批")

    seq = (await db.execute(
        select(func.count()).select_from(ClosedPeriodCorrection)
    )).scalar_one() + 1
    correction_ref = f"CPC-{report_month}-{seq:04d}"
    # 预览须在申请行插入前构建：preview_report 属于申请证据列，落库后 trigger 禁止改写
    preview = await _build_preview(db, report, deltas)
    correction = ClosedPeriodCorrection(
        correction_ref=correction_ref,
        correction_type=correction_type,
        status=CorrectionStatus.PENDING,
        report_id=report.id, batch_id=batch.id, report_month=report_month,
        channel=channel, merchant_id=merchant_id, currency=report.currency,
        source_raw_event_id=source_raw_id, target_business_event_id=target_be_id,
        reason=reason.strip(), impact_scope=impact_scope.strip(),
        declared_impact={
            **(declared_impact or {}),
            "lines": declared_lines,
            "total_settle": str(total_settle),
            "total_book": str(total_book),
        },
        submitted_by=operator, approver=approver.strip(),
        preview_report=preview,
    )
    db.add(correction)
    await db.flush()

    for i, d in enumerate(deltas, start=1):
        d.correction_id = correction.id
        d.line_no = i
        db.add(d)
    await db.flush()

    await _log(db, correction.id, CorrectionActionType.SUBMIT, operator,
               comment=reason.strip(),
               detail={"approver": correction.approver,
                       "preview_version": preview.get("preview_of_report_version"),
                       "total_book": str(total_book)})
    await db.flush()
    return correction


# ---------------------------------------------------------------- 审批
async def approve_correction(
    db: AsyncSession, correction_id: int, operator: str, comment: str | None = None
) -> dict:
    """审批通过：过账差额 -> 校验账本守恒 -> 追加报表新版本 -> 输出影响说明。"""
    c = await db.get(ClosedPeriodCorrection, correction_id)
    if c is None:
        raise CorrectionError("更正申请不存在")
    if c.status != CorrectionStatus.PENDING:
        raise CorrectionError(f"更正 {c.correction_ref} 当前为 {c.status}，不可重复审批")
    if operator.strip() != c.approver:
        raise CorrectionError("审批人必须与申请登记的审批人一致")
    if operator.strip() == c.submitted_by:
        raise CorrectionError("提交人不得自行审批")

    report = await db.get(MonthlyReport, c.report_id)
    batch = await db.get(SettlementBatch, c.batch_id)
    if report is None or batch is None:
        raise CorrectionError("更正归属的报表/批次不存在")
    if report.status != "CLOSED":
        raise CorrectionError("归属报表已不再处于关闭状态，请重新提交")
    deltas = (
        await db.execute(
            select(CorrectionDelta)
            .where(CorrectionDelta.correction_id == c.id)
            .order_by(CorrectionDelta.line_no)
        )
    ).scalars().all()

    # 差额过账：一条业务事件承载全部差额行，借贷由差额自身成对保证
    total_dr = sum((d.book_amount for d in deltas if d.direction == "DR"), Decimal("0"))
    total_cr = sum((d.book_amount for d in deltas if d.direction == "CR"), Decimal("0"))
    if quantize(total_dr) != quantize(total_cr):
        raise CorrectionError(
            f"差额行借贷不守恒：DR={quantize(total_dr)} CR={quantize(total_cr)}，拒绝过账"
        )
    # L1 补单证据的状态随审批结论流转（证据列本身不可改）
    if c.source_raw_event_id is not None:
        raw = await db.get(RawEvent, c.source_raw_event_id)
        if raw is not None:
            raw.status = "PROCESSED"
            raw.processed_at = _now()
            raw.reject_reason = f"posted via approved correction {c.correction_ref}"
    business_key = f"{EventType.CORRECTION_ADJUSTMENT}:{c.correction_ref}:v1"
    be = BusinessEvent(
        business_key=business_key,
        entity_type=EntityType.CORRECTION,
        event_type=EventType.CORRECTION_ADJUSTMENT,
        event_version=1,
        entity_ref=c.correction_ref,
        channel=c.channel, merchant_id=c.merchant_id,
        order_ref=deltas[0].order_ref if deltas else None,
        source=EventSource.MANUAL,
        event_time=_now(),
        payload={
            "correction_id": c.id,
            "correction_ref": c.correction_ref,
            "correction_type": c.correction_type,
            "report_month": c.report_month,
            "approved_by": operator,
            "reason": c.reason,
            "lines": [
                {"line_no": d.line_no, "order_ref": d.order_ref,
                 "channel": d.channel or c.channel, "account": d.account,
                 "settle_amount": str(d.settle_amount),
                 "book_amount": str(d.book_amount)}
                for d in deltas
            ],
        },
        supersedes_event_id=c.target_business_event_id,
    )
    db.add(be)
    await db.flush()

    entries: list[LedgerEntry] = []
    by_order: dict[str, dict] = {}
    for d in deltas:
        entries.append(LedgerEntry(
            business_event_id=be.id, account=d.account, direction=d.direction,
            txn_amount=Decimal("0"), txn_currency=d.settle_currency,
            settle_amount=d.settle_amount, settle_currency=d.settle_currency,
            book_amount=d.book_amount, book_currency=d.book_currency,
            rate_source="CORRECTION_APPROVED", rate_effective_at=be.event_time,
            decimal_places=2, raw_amount=d.settle_amount,
            effective_month=c.report_month,
            channel=c.channel, merchant_id=c.merchant_id, order_ref=d.order_ref,
        ))
        key = d.order_ref or "-"
        row = by_order.setdefault(key, {
            "order_ref": d.order_ref or None,
            "channel": c.channel,
            "channel_ref": d.channel_ref,
            "settle_amount": Decimal("0"), "book_amount": Decimal("0"),
            "currency": d.settle_currency, "line_types": [],
        })
        row["settle_amount"] += d.impact_settle_amount
        row["book_amount"] += d.impact_book_amount
        if d.line_type not in row["line_types"]:
            row["line_types"].append(d.line_type)
    db.add_all(entries)
    await db.flush()

    # 通过后重新校验账本守恒（全账本 + 本周期）
    conservation = await _conservation_check(db, c.report_month, c.channel, c.merchant_id)

    # 追加正式报表版本（旧归档版本仅 SUPERSEDED，原样保留）
    version = await ensure_monthly_report_version(
        db, batch,
        reason=f"{c.correction_ref} {c.correction_type} 审批通过：{c.reason[:120]}",
    )

    impact = {
        "correction_ref": c.correction_ref,
        "correction_type": c.correction_type,
        "report_month": c.report_month,
        "channel": c.channel,
        "merchant_id": c.merchant_id,
        "currency": c.currency,
        "declared_impact": c.declared_impact.get("total_book") if c.declared_impact else None,
        "affected_orders": [
            {
                **row,
                "settle_amount": str(quantize(row["settle_amount"])),
                "book_amount": str(quantize(row["book_amount"])),
            }
            for row in by_order.values()
        ],
        "affected_reports": [{
            "report_id": report.id,
            "report_month": c.report_month,
            "channel": c.channel,
            "merchant_id": c.merchant_id,
            "previous_version": version["version"] - 1 if not version.get("reused") else version["version"],
            "new_version": version["version"],
            "version_reused": version.get("reused", False),
        }],
        "delta_business_event_id": be.id,
        "delta_lines": len(deltas),
        "total_settle": str(quantize(sum((d.impact_settle_amount for d in deltas), Decimal("0")))) ,
        "total_book": str(quantize(sum((d.impact_book_amount for d in deltas), Decimal("0")))) ,
    }

    c.status = CorrectionStatus.APPROVED
    c.delta_business_event_id = be.id
    c.decided_at = _now()
    c.decision_comment = comment
    c.conservation_check = conservation
    c.impact_detail = impact
    await _log(db, c.id, CorrectionActionType.APPROVE, operator,
               comment=comment, detail={"business_event_id": be.id,
                                        "report_version": version})
    await db.flush()
    return {
        "correction_ref": c.correction_ref,
        "status": c.status,
        "delta_business_event_id": be.id,
        "correction_entries": len(entries),
        "ledger_conservation": conservation,
        "report_version": version,
        "impact_statement": impact,
    }


async def reject_correction(
    db: AsyncSession, correction_id: int, operator: str, comment: str
) -> ClosedPeriodCorrection:
    """审批驳回：差额永不进入账本，原始账本与报表版本不受任何影响。"""
    c = await db.get(ClosedPeriodCorrection, correction_id)
    if c is None:
        raise CorrectionError("更正申请不存在")
    if c.status != CorrectionStatus.PENDING:
        raise CorrectionError(f"更正 {c.correction_ref} 当前为 {c.status}，不可驳回")
    if operator.strip() != c.approver:
        raise CorrectionError("审批人必须与申请登记的审批人一致")
    if not comment or not comment.strip():
        raise CorrectionError("驳回必须填写审批意见")
    decided_at = _now()
    impact = {
        "rejected_by": operator,
        "rejected_at": decided_at.isoformat(),
        "comment": comment.strip(),
        "posted_to_ledger": False,
    }
    # 先完成所有副作用（L1 状态、轨迹），最后一次性写入终态列，
    # 避免中途 autoflush 先写 status 再写结论列（触发器要求结论列与状态同批写入）
    if c.source_raw_event_id is not None:
        raw = await db.get(RawEvent, c.source_raw_event_id)
        if raw is not None:
            raw.status = "REJECTED"
            raw.processed_at = decided_at
            raw.reject_reason = (
                f"closed period correction {c.correction_ref} rejected; not posted"
            )
    await _log(db, c.id, CorrectionActionType.REJECT, operator, comment.strip(),
               detail={"posted_to_ledger": False})
    c.status = CorrectionStatus.REJECTED
    c.decided_at = decided_at
    c.decision_comment = comment.strip()
    c.impact_detail = impact
    await db.flush()
    return c


async def _conservation_check(db: AsyncSession, report_month: str,
                              channel: str, merchant_id: str) -> dict:
    """账本守恒复核：全账本与关闭周期口径均要求借贷相等。"""
    async def totals(where):
        stmt = select(
            LedgerEntry.direction,
            func.coalesce(func.sum(LedgerEntry.book_amount), 0),
        )
        if where is not None:
            stmt = stmt.where(where)
        rows = (await db.execute(stmt.group_by(LedgerEntry.direction))).all()
        return {d: Decimal(str(v)) for d, v in rows}

    global_totals = await totals(None)
    period_totals = await totals(
        (LedgerEntry.effective_month == report_month)
        & (LedgerEntry.channel == channel)
        & (LedgerEntry.merchant_id == merchant_id)
    )
    g_dr = quantize(global_totals.get("DR", Decimal("0")))
    g_cr = quantize(global_totals.get("CR", Decimal("0")))
    p_dr = quantize(period_totals.get("DR", Decimal("0")))
    p_cr = quantize(period_totals.get("CR", Decimal("0")))
    balanced = g_dr == g_cr and p_dr == p_cr
    if not balanced:
        raise CorrectionError(
            f"更正过账后账本不守恒：全局 DR={g_dr}/CR={g_cr}，"
            f"周期 DR={p_dr}/CR={p_cr}"
        )
    return {
        "checked_at": _now().isoformat(),
        "ledger": {"total_debit": str(g_dr), "total_credit": str(g_cr)},
        "report_month_scope": {
            "report_month": report_month, "channel": channel,
            "merchant_id": merchant_id,
            "total_debit": str(p_dr), "total_credit": str(p_cr),
        },
        "balanced": True,
    }


# ---------------------------------------------------------------- 合并出具更正说明
async def publish_consolidated_statement(
    db: AsyncSession, report_id: int, operator: str
) -> CorrectionStatement:
    """把同一关闭周期全部 APPROVED 未结更正合并为一份更正说明并发布。

    列表为快照：受影响订单号、渠道、金额与报表版本一次性汇总；发布后涉及的更正
    标记 CONSOLIDATED，不会逐笔零散发布。
    """
    report = await db.get(MonthlyReport, report_id)
    if report is None:
        raise CorrectionError("报表不存在")
    if not operator or not operator.strip():
        raise CorrectionError("发布人不能为空")

    pending = (
        await db.execute(
            select(ClosedPeriodCorrection)
            .where(
                ClosedPeriodCorrection.report_id == report_id,
                ClosedPeriodCorrection.status == CorrectionStatus.APPROVED,
            )
            .order_by(ClosedPeriodCorrection.id)
        )
    ).scalars().all()
    if not pending:
        raise CorrectionError("该关闭周期没有已通过且待合并的更正，无需出具说明")

    affected_orders: dict[str, dict] = {}
    correction_rows: list[dict] = []
    total_settle = total_book = Decimal("0")
    report_versions: set[int] = set()
    for c in pending:
        total_settle += Decimal(str(c.impact_detail.get("total_settle", "0")))
        total_book += Decimal(str(c.impact_detail.get("total_book", "0")))
        for v in c.impact_detail.get("affected_reports", []):
            report_versions.add(int(v.get("new_version", 0)))
        for o in c.impact_detail.get("affected_orders", []):
            key = o.get("order_ref") or f"NO-ORDER-{c.id}"
            agg = affected_orders.setdefault(key, {
                "order_ref": o.get("order_ref"), "channel": o.get("channel"),
                "channel_refs": [], "settle_amount": Decimal("0"),
                "book_amount": Decimal("0"), "currency": o.get("currency"),
            })
            agg["settle_amount"] += Decimal(str(o.get("settle_amount", "0")))
            agg["book_amount"] += Decimal(str(o.get("book_amount", "0")))
            for ref in [o.get("channel_ref")]:
                if ref and ref not in agg["channel_refs"]:
                    agg["channel_refs"].append(ref)
        correction_rows.append({
            "correction_id": c.id,
            "correction_ref": c.correction_ref,
            "correction_type": c.correction_type,
            "reason": c.reason,
            "impact_scope": c.impact_scope,
            "submitted_by": c.submitted_by,
            "approver": c.approver,
            "approved_at": c.decided_at.isoformat() if c.decided_at else None,
            "total_book": c.impact_detail.get("total_book"),
            "total_settle": c.impact_detail.get("total_settle"),
            "delta_business_event_id": c.delta_business_event_id,
        })

    # 说明编号按报表内序号递增：同一关闭周期后续再收口时生成新的一份，不覆盖旧说明
    seq = (await db.execute(
        select(func.count()).select_from(CorrectionStatement).where(
            CorrectionStatement.report_id == report.id
        )
    )).scalar_one() + 1
    statement_ref = f"CS-{report.report_month}-{report.channel}-{report.id:04d}-{seq:02d}"
    snapshot = {
        "statement_ref": statement_ref,
        "report": {
            "report_id": report.id, "report_month": report.report_month,
            "channel": report.channel, "merchant_id": report.merchant_id,
            "currency": report.currency,
            "report_versions_issued": sorted(report_versions),
        },
        "correction_count": len(pending),
        "corrections": correction_rows,
        "affected_orders": [
            {
                "order_ref": v["order_ref"], "channel": v["channel"],
                "channel_refs": v["channel_refs"],
                "settle_amount": str(quantize(v["settle_amount"])),
                "book_amount": str(quantize(v["book_amount"])),
                "currency": v["currency"],
            }
            for v in sorted(affected_orders.values(), key=lambda x: (x["order_ref"] or ""))
        ],
        "affected_channels": sorted({c.channel for c in pending}),
        "affected_reports": ["{}-{}".format(report.report_month, report.channel)],
        "totals": {
            "settle_amount": str(quantize(total_settle)),
            "book_amount": str(quantize(total_book)),
            "book_currency": "CNY",
        },
        "architecture_note": (
            "原始账本与关闭结论未做任何修改；本说明汇总审批通过的差额过账与随之产生的报表新版本。"
        ),
    }

    statement = CorrectionStatement(
        statement_ref=statement_ref, report_id=report.id,
        report_month=report.report_month, channel=report.channel,
        merchant_id=report.merchant_id, currency=report.currency,
        correction_count=len(pending),
        snapshot=snapshot, published_by=operator.strip(),
    )
    db.add(statement)
    await db.flush()

    for c in pending:
        c.status = CorrectionStatus.CONSOLIDATED
        c.statement_id = statement.id
        await _log(db, c.id, CorrectionActionType.CONSOLIDATE, operator,
                   comment=f"合并出具更正说明 {statement_ref}",
                   detail={"statement_id": statement.id})
    await db.flush()
    return statement


async def build_impact_statement(
    db: AsyncSession, correction_id: int
) -> dict:
    """读取某更正的审批后影响说明（未审批通过时返回预览口径）。"""
    c = await db.get(ClosedPeriodCorrection, correction_id)
    if c is None:
        raise CorrectionError("更正申请不存在")
    return {
        "correction_ref": c.correction_ref,
        "correction_type": c.correction_type,
        "status": c.status,
        "report_month": c.report_month,
        "reason": c.reason,
        "impact_scope": c.impact_scope,
        "submitted_by": c.submitted_by,
        "approver": c.approver,
        "submitted_at": c.submitted_at.isoformat() if c.submitted_at else None,
        "decided_at": c.decided_at.isoformat() if c.decided_at else None,
        "decision_comment": c.decision_comment,
        "delta_business_event_id": c.delta_business_event_id,
        "conservation_check": c.conservation_check,
        "impact_detail": c.impact_detail,
        "preview_report": c.preview_report,
        "posted_to_ledger": c.status in (CorrectionStatus.APPROVED,
                                         CorrectionStatus.CONSOLIDATED),
    }


async def ensure_consolidation_if_needed(
    db: AsyncSession, report_id: int | None = None, operator: str | None = None
) -> list[CorrectionStatement]:
    """自动收口：把各关闭周期已通过未结的更正合并为一份说明（同一周期只发一份）。"""
    stmt = select(ClosedPeriodCorrection.report_id).where(
        ClosedPeriodCorrection.status == CorrectionStatus.APPROVED
    )
    if report_id is not None:
        stmt = stmt.where(ClosedPeriodCorrection.report_id == report_id)
    report_ids = sorted({r for (r,) in (await db.execute(stmt.distinct())).all()})
    out: list[CorrectionStatement] = []
    for rid in report_ids:
        first = (
            await db.execute(
                select(ClosedPeriodCorrection)
                .where(
                    ClosedPeriodCorrection.report_id == rid,
                    ClosedPeriodCorrection.status == CorrectionStatus.APPROVED,
                )
                .order_by(ClosedPeriodCorrection.id)
                .limit(1)
            )
        ).scalar_one_or_none()
        if first is not None:
            publisher = (operator or "").strip() or first.approver
            out.append(await publish_consolidated_statement(db, rid, publisher))
    return out

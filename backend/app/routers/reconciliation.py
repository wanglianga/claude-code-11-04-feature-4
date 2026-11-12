"""结算批次、匹配、差异处置、关闭周期更正审批与报表查询 API。"""
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..database import get_db
from ..models import (
    BankTransaction, ClosedPeriodCorrection, CorrectionAction, CorrectionDelta,
    CorrectionStatement, Discrepancy, DiscrepancyAction, MonthlyReport,
    MonthlyReportVersion, SettlementBatch, SettlementLine, SettlementMatch,
)
from ..schemas import (
    AdjustPostIn, AdjustSuggestIn, AttachIn, BatchOut, ClaimIn, CloseBatchIn,
    CloseDiscrepancyIn, CorrectionDecisionIn, CorrectionSubmitIn, DiscrepancyOut,
    MatchConfirmIn, StatementPublishIn,
)
from ..services.reconciliation import run_reconciliation
from ..services.workflow import (
    claim, attach, suggest_adjustment, post_adjustment,
    close_discrepancy, confirm_match, close_batch, batch_snapshot,
)
from ..services.correction_approval import (
    CorrectionError, approve_correction, build_impact_statement,
    ensure_consolidation_if_needed, publish_consolidated_statement,
    reject_correction, submit_correction,
)

router = APIRouter(prefix="/api", tags=["reconciliation"])


# ---------------------------------------------------------------- 批次
@router.get("/batches", response_model=list[BatchOut])
async def list_batches(status: str | None = None,
                       db: AsyncSession = Depends(get_db)):
    stmt = select(SettlementBatch).order_by(
        SettlementBatch.period_end.desc(), SettlementBatch.version
    )
    if status:
        stmt = stmt.where(SettlementBatch.status == status)
    return (await db.execute(stmt)).scalars().all()


@router.get("/batches/{batch_id}")
async def get_batch(batch_id: int, db: AsyncSession = Depends(get_db)):
    batch = await db.get(SettlementBatch, batch_id)
    if batch is None:
        raise HTTPException(404, "批次不存在")
    lines = (
        await db.execute(
            select(SettlementLine).where(SettlementLine.batch_id == batch_id)
        )
    ).scalars().all()
    return {
        "batch": BatchOut.model_validate(batch),
        "lines": [
            {
                "id": l.id, "line_ref": l.line_ref, "order_ref": l.order_ref,
                "channel_ref": l.channel_ref, "line_type": l.line_type,
                "gross_amount": str(l.gross_amount),
                "fee_amount": str(l.fee_amount),
                "net_amount": str(l.net_amount),
                "currency": l.currency, "matched": l.matched,
            }
            for l in lines
        ],
    }


@router.post("/batches/{batch_id}/reconcile")
async def reconcile(batch_id: int, db: AsyncSession = Depends(get_db)):
    try:
        result = await run_reconciliation(db, batch_id)
        await db.commit()
        return result
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/batches/{batch_id}/close")
async def close(batch_id: int, data: CloseBatchIn,
                db: AsyncSession = Depends(get_db)):
    try:
        result = await close_batch(db, batch_id, data.operator)
        await db.commit()
        return result
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.get("/batches/{batch_id}/snapshot")
async def snapshot(batch_id: int, db: AsyncSession = Depends(get_db)):
    try:
        return await batch_snapshot(db, batch_id)
    except Exception as exc:
        raise HTTPException(400, str(exc))


# ---------------------------------------------------------------- 匹配
@router.get("/matches")
async def list_matches(level: str | None = None,
                       batch_id: int | None = None,
                       db: AsyncSession = Depends(get_db)):
    stmt = select(SettlementMatch).order_by(SettlementMatch.id)
    if level:
        stmt = stmt.where(SettlementMatch.match_level == level)
    if batch_id:
        stmt = stmt.where(SettlementMatch.settlement_batch_id == batch_id)
    rows = (await db.execute(stmt)).scalars().all()
    return [
        {
            "id": m.id, "match_level": m.match_level,
            "settlement_batch_id": m.settlement_batch_id,
            "business_event_id": m.business_event_id,
            "settlement_line_id": m.settlement_line_id,
            "bank_transaction_id": m.bank_transaction_id,
            "allocated_amount": str(m.allocated_amount),
            "currency": m.currency, "status": m.status,
            "match_method": m.match_method,
            "confirmed_at": m.confirmed_at,
        }
        for m in rows
    ]


@router.post("/matches/{match_id}/confirm")
async def confirm(match_id: int, data: MatchConfirmIn,
                  db: AsyncSession = Depends(get_db)):
    try:
        m = await confirm_match(db, match_id, data.operator)
        await db.commit()
        return {"id": m.id, "status": m.status, "match_method": m.match_method}
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


# ---------------------------------------------------------------- 银行流水
@router.get("/bank-transactions")
async def list_bank(db: AsyncSession = Depends(get_db)):
    rows = (
        await db.execute(select(BankTransaction).order_by(BankTransaction.booked_at))
    ).scalars().all()
    return [
        {
            "id": b.id, "bank_ref": b.bank_ref, "channel": b.channel,
            "currency": b.currency, "amount": str(b.amount),
            "booked_at": b.booked_at, "memo": b.memo,
        }
        for b in rows
    ]


# ---------------------------------------------------------------- 差异处置队列
@router.get("/discrepancies", response_model=list[DiscrepancyOut])
async def list_discrepancies(
    status: str | None = None, kind: str | None = None,
    batch_id: int | None = None, assignee: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(Discrepancy).order_by(Discrepancy.id)
    if status:
        stmt = stmt.where(Discrepancy.status == status)
    if kind:
        stmt = stmt.where(Discrepancy.kind == kind)
    if batch_id:
        stmt = stmt.where(Discrepancy.batch_id == batch_id)
    if assignee:
        stmt = stmt.where(Discrepancy.assignee == assignee)
    return (await db.execute(stmt)).scalars().all()


@router.get("/discrepancies/{disc_id}")
async def get_discrepancy(disc_id: int, db: AsyncSession = Depends(get_db)):
    d = await db.get(Discrepancy, disc_id)
    if d is None:
        raise HTTPException(404, "差异不存在")
    actions = (
        await db.execute(
            select(DiscrepancyAction)
            .where(DiscrepancyAction.discrepancy_id == disc_id)
            .order_by(DiscrepancyAction.id)
        )
    ).scalars().all()
    return {
        "discrepancy": DiscrepancyOut.model_validate(d),
        "actions": [
            {
                "id": a.id, "action_type": a.action_type,
                "operator": a.operator, "comment": a.comment,
                "attachment": a.attachment, "created_at": a.created_at,
            }
            for a in actions
        ],
    }


@router.post("/discrepancies/{disc_id}/claim", response_model=DiscrepancyOut)
async def do_claim(disc_id: int, data: ClaimIn,
                   db: AsyncSession = Depends(get_db)):
    try:
        d = await claim(db, disc_id, data.operator, data.comment)
        await db.commit()
        return d
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/discrepancies/{disc_id}/attach", response_model=DiscrepancyOut)
async def do_attach(disc_id: int, data: AttachIn,
                    db: AsyncSession = Depends(get_db)):
    try:
        d = await attach(db, disc_id, data.operator, data.attachment, data.comment)
        await db.commit()
        return d
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/discrepancies/{disc_id}/adjust-suggest", response_model=DiscrepancyOut)
async def do_suggest(disc_id: int, data: AdjustSuggestIn,
                     db: AsyncSession = Depends(get_db)):
    try:
        d = await suggest_adjustment(
            db, disc_id, data.operator, data.adjust_amount,
            data.account, data.comment,
        )
        await db.commit()
        return d
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/discrepancies/{disc_id}/adjust-post", response_model=DiscrepancyOut)
async def do_post(disc_id: int, data: AdjustPostIn,
                  db: AsyncSession = Depends(get_db)):
    try:
        d = await post_adjustment(db, disc_id, data.operator, data.comment)
        await db.commit()
        return d
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/discrepancies/{disc_id}/close", response_model=DiscrepancyOut)
async def do_close(disc_id: int, data: CloseDiscrepancyIn,
                   db: AsyncSession = Depends(get_db)):
    try:
        d = await close_discrepancy(db, disc_id, data.operator, data.comment)
        await db.commit()
        return d
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


# ---------------------------------------------------------------- 关闭周期更正审批
def _correction_out(c: ClosedPeriodCorrection) -> dict:
    return {
        "id": c.id, "correction_ref": c.correction_ref,
        "correction_type": c.correction_type, "status": c.status,
        "report_id": c.report_id, "batch_id": c.batch_id,
        "report_month": c.report_month, "channel": c.channel,
        "merchant_id": c.merchant_id, "currency": c.currency,
        "reason": c.reason, "impact_scope": c.impact_scope,
        "declared_impact": c.declared_impact,
        "submitted_by": c.submitted_by, "submitted_at": c.submitted_at,
        "approver": c.approver, "decided_at": c.decided_at,
        "decision_comment": c.decision_comment,
        "delta_business_event_id": c.delta_business_event_id,
        "preview_report": c.preview_report,
        "conservation_check": c.conservation_check,
        "impact_detail": c.impact_detail, "statement_id": c.statement_id,
        "posted_to_ledger": c.status in ("APPROVED", "CONSOLIDATED"),
    }


@router.get("/corrections")
async def list_corrections(
    status: str | None = None, report_id: int | None = None,
    report_month: str | None = None,
    db: AsyncSession = Depends(get_db),
):
    stmt = select(ClosedPeriodCorrection).order_by(ClosedPeriodCorrection.id.desc())
    if status:
        stmt = stmt.where(ClosedPeriodCorrection.status == status)
    if report_id:
        stmt = stmt.where(ClosedPeriodCorrection.report_id == report_id)
    if report_month:
        stmt = stmt.where(ClosedPeriodCorrection.report_month == report_month)
    return [_correction_out(c) for c in (await db.execute(stmt)).scalars().all()]


@router.get("/corrections/{correction_id}")
async def get_correction(correction_id: int, db: AsyncSession = Depends(get_db)):
    c = await db.get(ClosedPeriodCorrection, correction_id)
    if c is None:
        raise HTTPException(404, "更正申请不存在")
    deltas = (
        await db.execute(
            select(CorrectionDelta)
            .where(CorrectionDelta.correction_id == correction_id)
            .order_by(CorrectionDelta.line_no)
        )
    ).scalars().all()
    actions = (
        await db.execute(
            select(CorrectionAction)
            .where(CorrectionAction.correction_id == correction_id)
            .order_by(CorrectionAction.id)
        )
    ).scalars().all()
    return {
        **_correction_out(c),
        "deltas": [
            {
                "id": d.id, "line_no": d.line_no,
                "order_ref": d.order_ref, "channel_ref": d.channel_ref,
                "line_type": d.line_type, "account": d.account,
                "direction": d.direction,
                "settle_amount": str(d.settle_amount),
                "settle_currency": d.settle_currency,
                "book_amount": str(d.book_amount),
                "book_currency": d.book_currency,
                "impact_settle_amount": str(d.impact_settle_amount),
                "impact_book_amount": str(d.impact_book_amount),
                "memo": d.memo,
            }
            for d in deltas
        ],
        "actions": [
            {
                "id": a.id, "action_type": a.action_type,
                "operator": a.operator, "comment": a.comment,
                "detail": a.detail, "created_at": a.created_at,
            }
            for a in actions
        ],
    }


@router.post("/corrections")
async def create_correction(data: CorrectionSubmitIn,
                            db: AsyncSession = Depends(get_db)):
    """提交关闭周期更正申请：生成差额记录与新版本报表预览，不进入账本。"""
    try:
        c = await submit_correction(
            db, correction_type=data.correction_type, channel=data.channel,
            merchant_id=data.merchant_id, report_month=data.report_month,
            reason=data.reason, impact_scope=data.impact_scope,
            approver=data.approver, operator=data.operator,
            declared_impact=data.declared_impact,
            supplement_payload=data.supplement, parsing_fix=data.parsing_fix,
        )
        await db.commit()
        return _correction_out(c)
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/corrections/{correction_id}/approve")
async def approve(correction_id: int, data: CorrectionDecisionIn,
                  db: AsyncSession = Depends(get_db)):
    """审批通过：差额过账 -> 校验账本守恒 -> 追加报表新版本 -> 影响说明。"""
    try:
        result = await approve_correction(
            db, correction_id, data.operator, data.comment
        )
        await db.commit()
        return result
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/corrections/{correction_id}/reject")
async def reject(correction_id: int, data: CorrectionDecisionIn,
                 db: AsyncSession = Depends(get_db)):
    """审批驳回：差额永不进入账本，原始账本与归档结论不受影响。"""
    try:
        c = await reject_correction(
            db, correction_id, data.operator, data.comment or ""
        )
        await db.commit()
        return _correction_out(c)
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.get("/corrections/{correction_id}/impact")
async def correction_impact(correction_id: int,
                            db: AsyncSession = Depends(get_db)):
    """影响说明：受影响订单号、渠道、金额与报表版本。"""
    try:
        return await build_impact_statement(db, correction_id)
    except CorrectionError as exc:
        raise HTTPException(404, str(exc))


@router.get("/correction-statements")
async def list_statements(report_id: int | None = None,
                          db: AsyncSession = Depends(get_db)):
    stmt = select(CorrectionStatement).order_by(CorrectionStatement.id.desc())
    if report_id:
        stmt = stmt.where(CorrectionStatement.report_id == report_id)
    return [
        {
            "id": s.id, "statement_ref": s.statement_ref,
            "report_id": s.report_id, "report_month": s.report_month,
            "channel": s.channel, "merchant_id": s.merchant_id,
            "correction_count": s.correction_count,
            "published_by": s.published_by, "published_at": s.published_at,
        }
        for s in (await db.execute(stmt)).scalars().all()
    ]


@router.get("/correction-statements/{statement_id}")
async def get_statement(statement_id: int, db: AsyncSession = Depends(get_db)):
    s = await db.get(CorrectionStatement, statement_id)
    if s is None:
        raise HTTPException(404, "更正说明不存在")
    return {
        "id": s.id, "statement_ref": s.statement_ref,
        "report_id": s.report_id, "report_month": s.report_month,
        "channel": s.channel, "merchant_id": s.merchant_id,
        "correction_count": s.correction_count,
        "published_by": s.published_by, "published_at": s.published_at,
        "snapshot": s.snapshot,
    }


@router.post("/correction-statements/consolidate")
async def consolidate_statements(report_id: int | None = None,
                                 data: StatementPublishIn | None = None,
                                 db: AsyncSession = Depends(get_db)):
    """批量收口：把各关闭周期已通过未结更正一次性合并出具（每周期一份）。"""
    try:
        operator = (data.operator if data else None) or "finance.lead"
        statements = await ensure_consolidation_if_needed(db, report_id, operator)
        await db.commit()
        return {
            "published": [
                {"statement_ref": s.statement_ref,
                 "report_id": s.report_id,
                 "correction_count": s.correction_count}
                for s in statements
            ],
            "count": len(statements),
            "operator": operator,
        }
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


@router.post("/reports/{report_id}/correction-statement")
async def publish_statement(report_id: int, data: StatementPublishIn,
                            db: AsyncSession = Depends(get_db)):
    """把该关闭周期全部已通过未结更正合并为一份更正说明（不逐笔零散发布）。"""
    try:
        s = await publish_consolidated_statement(db, report_id, data.operator)
        await db.commit()
        return {
            "id": s.id, "statement_ref": s.statement_ref,
            "correction_count": s.correction_count,
            "snapshot": s.snapshot,
        }
    except Exception as exc:
        await db.rollback()
        raise HTTPException(400, str(exc))


# ---------------------------------------------------------------- 月度报表
@router.get("/reports")
async def list_reports(db: AsyncSession = Depends(get_db)):
    rows = (await db.execute(select(MonthlyReport))).scalars().all()
    return [
        {
            "id": r.id, "channel": r.channel, "merchant_id": r.merchant_id,
            "currency": r.currency, "report_month": r.report_month,
            "status": r.status, "current_version": r.current_version,
            "closed_at": r.closed_at,
        }
        for r in rows
    ]


@router.get("/reports/{report_id}/versions")
async def report_versions(report_id: int, db: AsyncSession = Depends(get_db)):
    rows = (
        await db.execute(
            select(MonthlyReportVersion)
            .where(MonthlyReportVersion.report_id == report_id)
            .order_by(MonthlyReportVersion.version)
        )
    ).scalars().all()
    return [
        {
            "id": v.id, "version": v.version,
            "version_status": v.version_status,
            "reason": v.reason, "created_at": v.created_at,
            "snapshot": v.snapshot,
        }
        for v in rows
    ]

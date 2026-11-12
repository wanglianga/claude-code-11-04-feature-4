"""测试与种子脚本共用的场景装载器：按阶段装载，返回句柄供断言。"""
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from . import events as demo
from ..enums import CorrectionType, DiscrepancyStatus
from ..models import BusinessEvent, Discrepancy, MonthlyReportVersion, SettlementBatch
from ..services.correction_approval import (
    approve_correction, publish_consolidated_statement, submit_correction,
)
from ..services.ledger import ingest_raw_event, process_event
from ..services.reconciliation import run_reconciliation
from ..services.workflow import (
    claim, close_batch, close_discrepancy, post_adjustment, suggest_adjustment,
)

OPERATOR = "finance.chen"
APPROVER = "finance.lead"


async def ingest(db: AsyncSession, event):
    raw, dup = await ingest_raw_event(db, event)
    be = None
    if not dup:
        be = await process_event(db, raw)
    return raw, dup, be


async def load_base_events(db: AsyncSession) -> dict:
    """阶段 1：汇率 + 订单/支付 + 银行流水 + 结算单 v1。"""
    counts = {"raw": 0, "duplicate": 0, "business": 0}
    for ev in [*demo.fx_events(), *demo.order_payment_events()]:
        _, dup, be = await ingest(db, ev)
        counts["raw"] += 1
        counts["duplicate"] += int(dup)
        counts["business"] += int(be is not None)
    for ev in demo.bank_events():
        await ingest(db, ev)
    _, _, sb1_be = await ingest(db, demo.settlement_v1_event())
    await db.commit()
    batch = (
        await db.execute(
            select(SettlementBatch).where(
                SettlementBatch.batch_ref == "SB-202610",
                SettlementBatch.version == 1,
            )
        )
    ).scalar_one()
    return {"counts": counts, "batch": batch}


async def resolve_and_close(db: AsyncSession, batch: SettlementBatch) -> dict:
    """阶段 2：人工处置全部差异并关闭批次。"""
    result = await run_reconciliation(db, batch.id)
    discs = (
        await db.execute(
            select(Discrepancy).where(
                Discrepancy.batch_id == batch.id,
                Discrepancy.status != DiscrepancyStatus.CLOSED,
            )
        )
    ).scalars().all()
    for d in discs:
        await claim(db, d.id, OPERATOR, f"认领 {d.title}")
        await suggest_adjustment(
            db, d.id, OPERATOR,
            adjust_amount=-d.diff_amount if d.diff_amount != 0 else Decimal("5.00"),
            account="MANUAL_ADJUSTMENT", comment=f"调账建议：{d.title}",
        )
        await post_adjustment(db, d.id, OPERATOR, "复核后过账")
        await close_discrepancy(db, d.id, OPERATOR, "关闭确认")
    closed = await close_batch(db, batch.id, OPERATOR)
    await db.commit()
    return {"reconcile": result, "closed": closed, "resolved": len(discs)}


async def load_revision_and_close(db: AsyncSession) -> tuple[SettlementBatch, dict]:
    """阶段 3：结算单修订 v2 -> 对账 -> 处置 -> 关闭。"""
    _, _, _ = await ingest(db, demo.settlement_v2_event())
    await db.commit()
    batch = (
        await db.execute(
            select(SettlementBatch).where(
                SettlementBatch.batch_ref == "SB-202610",
                SettlementBatch.version == 2,
            )
        )
    ).scalar_one()
    info = await resolve_and_close(db, batch)
    return batch, info


async def load_fx_correction(db: AsyncSession) -> BusinessEvent:
    """阶段 4：汇率更正。"""
    _, _, be = await ingest(db, demo.fx_correction_event())
    await db.commit()
    return be


async def load_late_refund(db: AsyncSession):
    """阶段 5：关闭周期补单走更正审批流。

    提交更正申请（生成差额记录与报表预览，不入账）-> 审批通过（差额过账 + 新报表版本）
    -> 同周期更正合并为一份更正说明。
    """
    late_event = demo.late_october_refund_event()
    correction = await submit_correction(
        db,
        correction_type=CorrectionType.SUPPLEMENT,
        channel=demo.CHANNEL,
        merchant_id=demo.MERCHANT,
        report_month="2026-10",
        reason="关闭后发现 10-28 退款迟报，补录退款事实",
        impact_scope=f"{late_event.payload.get('order_ref')} / {demo.CHANNEL} / 退款 10 USD",
        approver=APPROVER,
        operator=OPERATOR,
        declared_impact={
            "orders": [late_event.payload.get("order_ref")],
            "channels": [demo.CHANNEL],
        },
        supplement_payload={**late_event.payload, "idempotency_key": late_event.idempotency_key},
    )
    await db.flush()
    approval = await approve_correction(db, correction.id, APPROVER, "核对渠道退款单，通过")
    await db.flush()
    statement = await publish_consolidated_statement(db, correction.report_id, APPROVER)
    await db.commit()
    versions = await report_versions(db)
    return approval, versions, statement


async def report_versions(db: AsyncSession) -> list[MonthlyReportVersion]:
    return list(
        (await db.execute(
            select(MonthlyReportVersion).order_by(MonthlyReportVersion.version)
        )).scalars().all()
    )

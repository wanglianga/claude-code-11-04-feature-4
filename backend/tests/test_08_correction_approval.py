"""关闭周期更正审批：未审批不入账、驳回永不入账、通过后守恒校验与合并更正说明。"""
import pytest
from decimal import Decimal

from sqlalchemy import func, select, text

from app.demo.loader import load_base_events, resolve_and_close
from app.enums import CorrectionStatus, VersionStatus
from app.models import (
    BusinessEvent, ClosedPeriodCorrection, CorrectionDelta, CorrectionStatement,
    LedgerEntry, MonthlyReport, MonthlyReportVersion, RawEvent, SettlementBatch,
)
from app.services.correction_approval import (
    CorrectionError, approve_correction, build_impact_statement,
    publish_consolidated_statement, reject_correction, submit_correction,
)
from app.services.ledger import IngestError, ingest_raw_event, process_event

OPERATOR = "finance.chen"
APPROVER = "finance.lead"
REPORT_MONTH = "2026-10"


async def _closed_period(db) -> dict:
    base = await load_base_events(db)
    info = await resolve_and_close(db, base["batch"])
    report = (await db.execute(select(MonthlyReport))).scalar_one()
    return {"base": base, "closed": info, "report": report}


async def _ledger_totals(db, where=None):
    stmt = select(LedgerEntry.direction, func.coalesce(func.sum(LedgerEntry.book_amount), 0))
    if where is not None:
        stmt = stmt.where(where)
    rows = (await db.execute(stmt.group_by(LedgerEntry.direction))).all()
    return {d: Decimal(str(v)) for d, v in rows}


def _supplement_payload(order_ref: str, amount: str, fee: str = "0",
                        event_type: str = "PAYMENT_CAPTURED") -> dict:
    return {
        "event_type": event_type, "entity_type": "CAPTURE" if event_type == "PAYMENT_CAPTURED" else "REFUND",
        "entity_ref": f"{order_ref}-LATE", "order_ref": order_ref,
        "channel_ref": f"CH-{order_ref}", "channel": "STRIPE", "merchant_id": "M1",
        "business_key": f"{event_type}:{order_ref}-LATE:v1",
        "event_time": "2026-10-28T10:00:00+00:00",
        "amount": amount, "fee_amount": fee,
        "txn_currency": "USD", "settle_currency": "USD", "book_currency": "CNY",
        "decimal_places": 2,
    }


async def test_submission_creates_delta_and_preview_without_posting(db):
    """提交只生成差额与预览：账本无新增分录，原始账本与关闭结论不变。"""
    ctx = await _closed_period(db)
    report = ctx["report"]
    batch = await db.get(SettlementBatch, ctx["base"]["batch"].id)
    before_entries = (await db.execute(
        select(func.count()).select_from(LedgerEntry)
    )).scalar_one()
    before_events = (await db.execute(
        select(func.count()).select_from(BusinessEvent)
    )).scalar_one()
    before_versions = (await db.execute(
        select(func.count()).select_from(MonthlyReportVersion)
    )).scalar_one()
    before_closed_at = (report.closed_at, batch.closed_at, batch.net_amount)
    before_snapshot = (await db.execute(
        select(MonthlyReportVersion).where(MonthlyReportVersion.version == 1)
    )).scalar_one().snapshot

    c = await submit_correction(
        db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
        report_month=REPORT_MONTH,
        reason="关闭后收到渠道补单：ORD-010 的捕获事实补录",
        impact_scope="ORD-010 / STRIPE / 本金 20 USD，影响 2026-10 报表",
        approver=APPROVER, operator=OPERATOR,
        declared_impact={"orders": ["ORD-010"], "channels": ["STRIPE"]},
        supplement_payload=_supplement_payload("ORD-010", "20", "0.6"),
    )
    await db.commit()

    assert c.status == CorrectionStatus.PENDING
    assert c.approver == APPROVER and c.submitted_by == OPERATOR
    deltas = (await db.execute(
        select(CorrectionDelta).where(CorrectionDelta.correction_id == c.id)
    )).scalars().all()
    assert len(deltas) == 4  # 本金 DR/CR + 手续费 DR/CR
    # 差额自身借贷平衡
    dr = sum((d.book_amount for d in deltas if d.direction == "DR"), Decimal("0"))
    cr = sum((d.book_amount for d in deltas if d.direction == "CR"), Decimal("0"))
    assert dr == cr and dr > 0

    # 账本/事件/归档版本均未变化；批次与报表关闭结论原样
    assert (await db.execute(select(func.count()).select_from(LedgerEntry))).scalar_one() == before_entries
    assert (await db.execute(select(func.count()).select_from(BusinessEvent))).scalar_one() == before_events
    assert (await db.execute(select(func.count()).select_from(MonthlyReportVersion))).scalar_one() == before_versions
    assert (report.closed_at, batch.closed_at, batch.net_amount) == before_closed_at
    reloaded = (await db.execute(
        select(MonthlyReportVersion).where(MonthlyReportVersion.version == 1)
    )).scalar_one()
    assert reloaded.snapshot == before_snapshot
    # 预览确有新版本口径，但只是申请行上的预览
    assert c.preview_report["preview_of_report_version"] == report.current_version + 1
    assert c.preview_report["conservation_check"]["balanced"] is True

    # L1 留存了原始消息，但状态是待审批，且未进入 L2 账本
    raw = await db.get(RawEvent, c.source_raw_event_id)
    assert raw is not None and raw.status == "PENDING_APPROVAL"


async def test_direct_ingest_into_closed_period_is_blocked(db):
    """绕过审批直接补录到已关闭周期 -> 摄入被拒，账本无变化。"""
    await _closed_period(db)
    from app.demo import events as demo

    before = (await db.execute(select(func.count()).select_from(LedgerEntry))).scalar_one()
    late = demo.late_october_refund_event()
    raw, dup = await ingest_raw_event(db, late)
    with pytest.raises(IngestError, match="已关闭"):
        await process_event(db, raw)
    await db.rollback()
    assert (await db.execute(select(func.count()).select_from(LedgerEntry))).scalar_one() == before


async def test_rejected_correction_never_enters_ledger(db):
    await _closed_period(db)
    before_ledger = await _ledger_totals(db)
    before_versions = (await db.execute(
        select(func.count()).select_from(MonthlyReportVersion)
    )).scalar_one()

    c = await submit_correction(
        db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
        report_month=REPORT_MONTH, reason="待驳回的补单",
        impact_scope="ORD-011 / STRIPE", approver=APPROVER, operator=OPERATOR,
        supplement_payload=_supplement_payload("ORD-011", "30", "1"),
    )
    await reject_correction(db, c.id, APPROVER, "材料不足，驳回")
    await db.commit()

    refreshed = await db.get(ClosedPeriodCorrection, c.id)
    assert refreshed.status == CorrectionStatus.REJECTED
    assert refreshed.delta_business_event_id is None
    assert refreshed.impact_detail["posted_to_ledger"] is False
    # L1 证据随审批结论流转，但仍不进 L2 账本
    raw = await db.get(RawEvent, refreshed.source_raw_event_id)
    assert raw.status == "REJECTED"
    # 账本与报表版本均无新痕迹
    assert await _ledger_totals(db) == before_ledger
    assert (await db.execute(
        select(func.count()).select_from(MonthlyReportVersion)
    )).scalar_one() == before_versions
    # 驳回是终态：不可再审批
    with pytest.raises(CorrectionError):
        await approve_correction(db, c.id, APPROVER, "尝试二审批")
    await db.rollback()


async def test_approval_posts_delta_rechecks_conservation_and_versions(db):
    await _closed_period(db)
    c = await submit_correction(
        db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
        report_month=REPORT_MONTH,
        reason="关闭后补单：ORD-010 捕获 20 USD",
        impact_scope="ORD-010 / STRIPE / 20 USD",
        approver=APPROVER, operator=OPERATOR,
        supplement_payload=_supplement_payload("ORD-010", "20", "0.6"),
    )
    await db.commit()
    correction_id = c.id

    await approve_correction(db, correction_id, APPROVER, "核对补单材料一致，通过")
    await db.commit()
    refreshed = await db.get(ClosedPeriodCorrection, correction_id)
    assert refreshed.status == CorrectionStatus.APPROVED
    assert refreshed.delta_business_event_id is not None
    assert refreshed.conservation_check["balanced"] is True

    # 审批人约束：提交人本人不可自批
    c2 = await submit_correction(
        db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
        report_month=REPORT_MONTH, reason="第二笔补单",
        impact_scope="ORD-012", approver=APPROVER, operator=OPERATOR,
        supplement_payload=_supplement_payload("ORD-012", "10"),
    )
    delta_ev_id = refreshed.delta_business_event_id
    try:
        await approve_correction(db, c2.id, OPERATOR, "提交人自批")
    except CorrectionError as exc:
        assert "审批人" in str(exc)
    else:
        raise AssertionError("提交人自批应被拒绝")

    be = await db.get(BusinessEvent, delta_ev_id)
    assert be.event_type == "CORRECTION_ADJUSTMENT"

    # 差额分录借贷平衡，且原账本不变（追加而非修改）
    entries = (await db.execute(
        select(LedgerEntry).where(LedgerEntry.business_event_id == be.id)
    )).scalars().all()
    dr = sum((e.book_amount for e in entries if e.direction == "DR"), Decimal("0"))
    cr = sum((e.book_amount for e in entries if e.direction == "CR"), Decimal("0"))
    assert dr == cr and dr > 0
    assert all(e.effective_month == REPORT_MONTH for e in entries)

    totals = await _ledger_totals(db)
    assert totals["DR"] == totals["CR"]

    # 通过后重新校验账本守恒并留痕
    assert refreshed.conservation_check["balanced"] is True
    assert refreshed.conservation_check["report_month_scope"]["report_month"] == REPORT_MONTH

    # 报表新版本追加，旧版本 SUPERSEDED 且内容不变
    versions = (await db.execute(
        select(MonthlyReportVersion).order_by(MonthlyReportVersion.version)
    )).scalars().all()
    assert len(versions) >= 2
    assert versions[-1].version_status == VersionStatus.CURRENT
    assert versions[-1].snapshot["conservation_check"]["balanced"] is True
    assert sum(1 for v in versions if v.version_status == VersionStatus.CURRENT) == 1
    report = await db.get(MonthlyReport, refreshed.report_id)
    # 关闭结论不变：仍为已关闭，关闭时点不被改写
    assert report.status == "CLOSED"

    # 影响说明列出受影响订单号、渠道、金额与报表
    impact = await build_impact_statement(db, c.id)
    assert impact["posted_to_ledger"] is True
    orders = impact["impact_detail"]["affected_orders"]
    assert any(o["order_ref"] == "ORD-010" and o["channel"] == "STRIPE" for o in orders)
    report_links = impact["impact_detail"]["affected_reports"]
    assert report_links and report_links[0]["new_version"] >= 2
    assert impact["impact_detail"]["total_book"] != "0"


async def test_parsing_fix_delta_and_approval(db):
    """解析修复：按正确金额重算差额，历史分录行保持原值。"""
    await _closed_period(db)
    target = (await db.execute(
        select(BusinessEvent).where(BusinessEvent.business_key == "PAYMENT_CAPTURED:P-008:v1")
    )).scalar_one()
    original_entries = (await db.execute(
        select(LedgerEntry).where(LedgerEntry.business_event_id == target.id)
    )).scalars().all()
    original = [(e.id, e.book_amount, e.book_currency) for e in original_entries]

    before_versions = (await db.execute(
        select(func.count()).select_from(MonthlyReportVersion)
    )).scalar_one()

    c = await submit_correction(
        db, correction_type="PARSING_FIX", channel="STRIPE", merchant_id="M1",
        report_month=REPORT_MONTH,
        reason="ORD-008 源报文解析修复：本金 70 -> 75（小数位错读）",
        impact_scope="ORD-008 / STRIPE / 本金差额 +5 USD",
        approver=APPROVER, operator=OPERATOR,
        parsing_fix={"business_event_id": target.id, "corrected_amount": "75",
                     "note": "解析修复"},
    )
    await db.commit()
    assert c.status == CorrectionStatus.PENDING
    assert (await db.execute(
        select(func.count()).select_from(MonthlyReportVersion)
    )).scalar_one() == before_versions

    await approve_correction(db, c.id, APPROVER, "已核对渠道对账单")
    await db.commit()

    refreshed = await db.get(ClosedPeriodCorrection, c.id)
    assert refreshed.status == CorrectionStatus.APPROVED
    assert refreshed.target_business_event_id == target.id
    # 被修复的历史分录原样保留
    for eid, amt, ccy in original:
        e = await db.get(LedgerEntry, eid)
        assert (e.book_amount, e.book_currency) == (amt, ccy)
    # 差额只追加一条 CORRECTION_ADJUSTMENT 事件
    be = await db.get(BusinessEvent, refreshed.delta_business_event_id)
    assert be.supersedes_event_id == target.id
    impact = refreshed.impact_detail
    assert impact["affected_orders"][0]["order_ref"] == "ORD-008"
    # 本金差 5 USD * 7.2 = 36 CNY
    assert Decimal(impact["affected_orders"][0]["book_amount"]) == Decimal("36.00")


async def test_multiple_approved_corrections_publish_one_statement(db):
    """同一关闭周期多笔已通过未结更正 -> 合并为一份说明，不逐笔发布。"""
    ctx = await _closed_period(db)
    c1 = await submit_correction(
        db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
        report_month=REPORT_MONTH, reason="补单 A", impact_scope="ORD-010",
        approver=APPROVER, operator=OPERATOR,
        supplement_payload=_supplement_payload("ORD-010", "20", "0.6"),
    )
    c2 = await submit_correction(
        db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
        report_month=REPORT_MONTH, reason="补单 B", impact_scope="ORD-011",
        approver=APPROVER, operator=OPERATOR,
        supplement_payload=_supplement_payload(
            "ORD-011", "15", "0.45", event_type="REFUND_SUCCEEDED"),
    )
    await db.commit()

    await approve_correction(db, c1.id, APPROVER, "A 通过")
    await approve_correction(db, c2.id, APPROVER, "B 通过")
    await db.commit()

    # 审批后仍是 APPROVED 未结，尚未逐笔发布任何说明
    assert (await db.execute(
        select(func.count()).select_from(CorrectionStatement)
    )).scalar_one() == 0
    assert {c.status for c in (await db.execute(
        select(ClosedPeriodCorrection)
    )).scalars().all()} == {CorrectionStatus.APPROVED}

    statement = await publish_consolidated_statement(db, ctx["report"].id, APPROVER)
    await db.commit()

    assert statement.correction_count == 2
    snap = statement.snapshot
    assert snap["correction_count"] == 2
    orders = {o["order_ref"] for o in snap["affected_orders"]}
    assert {"ORD-010", "ORD-011"} <= orders
    assert snap["affected_channels"] == ["STRIPE"]
    assert snap["totals"]["book_currency"] == "CNY"
    assert len(snap["report"]["report_versions_issued"]) >= 1

    # 发布后两笔更正均标记 CONSOLIDATED，说明行只追加
    for cid in (c1.id, c2.id):
        c = await db.get(ClosedPeriodCorrection, cid)
        assert c.status == CorrectionStatus.CONSOLIDATED
        assert c.statement_id == statement.id
    # 再次发布没有可合并的更正，不会重复出具
    with pytest.raises(CorrectionError, match="没有"):
        await publish_consolidated_statement(db, ctx["report"].id, APPROVER)
    await db.rollback()


async def test_approval_requires_closed_period_and_distinct_approver(db):
    await _closed_period(db)
    # 提交人 = 审批人 -> 拒绝
    with pytest.raises(CorrectionError, match="审批人"):
        await submit_correction(
            db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
            report_month=REPORT_MONTH, reason="x", impact_scope="y",
            approver=OPERATOR, operator=OPERATOR,
            supplement_payload=_supplement_payload("ORD-020", "1"),
        )
    await db.rollback()
    # 未关闭周期不走审批流
    with pytest.raises(CorrectionError, match="已关闭"):
        await submit_correction(
            db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
            report_month="2026-09", reason="x", impact_scope="y",
            approver=APPROVER, operator=OPERATOR,
            supplement_payload=_supplement_payload("ORD-021", "1"),
        )
    await db.rollback()


async def test_correction_application_columns_and_state_machine_are_protected(db):
    """审批表：申请证据列不可改；终态不可再改；delete/truncate 被拒。"""
    await _closed_period(db)
    c = await submit_correction(
        db, correction_type="SUPPLEMENT", channel="STRIPE", merchant_id="M1",
        report_month=REPORT_MONTH, reason="状态机保护测试", impact_scope="ORD-013",
        approver=APPROVER, operator=OPERATOR,
        supplement_payload=_supplement_payload("ORD-013", "4"),
    )
    await db.commit()
    cid = c.id
    with pytest.raises(Exception, match="application columns"):
        await db.execute(text(
            f"UPDATE closed_period_corrections SET reason = 'tampered' WHERE id = {cid}"
        ))
    await db.rollback()
    with pytest.raises(Exception, match="append-only"):
        await db.execute(text(f"DELETE FROM closed_period_corrections WHERE id = {cid}"))
    await db.rollback()
    # 状态机：PENDING 不能直接跳到 CONSOLIDATED
    with pytest.raises(Exception, match="illegal correction status transition"):
        await db.execute(text(
            f"UPDATE closed_period_corrections SET status = 'CONSOLIDATED' WHERE id = {cid}"
        ))
    await db.rollback()
    # 正常流转允许：PENDING -> REJECTED，之后终态不可再改
    await reject_correction(db, cid, APPROVER, "驳回并冻结")
    await db.commit()
    with pytest.raises(Exception, match="terminal status"):
        await db.execute(text(
            f"UPDATE closed_period_corrections SET decision_comment = 'rewritten' WHERE id = {cid}"
        ))
    await db.rollback()


async def test_closed_conclusion_columns_are_immutable(db):
    """数据库层保证：关闭批次的金额/关闭人、报表关闭时点不可回改。"""
    ctx = await _closed_period(db)
    batch_id = ctx["base"]["batch"].id
    report_id = ctx["report"].id
    with pytest.raises(Exception, match="closed"):
        await db.execute(text(
            f"UPDATE settlement_batches SET net_amount = net_amount + 1 WHERE id = {batch_id}"
        ))
    await db.rollback()
    with pytest.raises(Exception, match="closed"):
        await db.execute(text(
            f"UPDATE settlement_batches SET closed_by = 'tamper' WHERE id = {batch_id}"
        ))
    await db.rollback()
    with pytest.raises(Exception, match="closed"):
        await db.execute(text(
            f"UPDATE monthly_reports SET closed_at = now() WHERE id = {report_id}"
        ))
    await db.rollback()

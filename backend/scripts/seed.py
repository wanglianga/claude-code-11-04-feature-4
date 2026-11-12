"""一键装载模拟数据并演示完整处置流程。

执行顺序（时间线乱序由数据集本身保证）：
  1. 汇率 + 订单/支付事件（含 CDC 重放、webhook 乱序、退款、拒付反转）
  2. 10 月结算单 v1 -> 自动对账（只给建议）-> 人工逐条处置 -> 关闭 -> 报表 v1
  3. 验证只追加保护（直接 SQL 篡改应被触发器拒绝）
  4. 结算单修订 v2 -> 对账 -> 处置 -> 关闭 -> 报表 v2（v1 标记 SUPERSEDED）
  5. 汇率更正 -> 更正账 -> 报表 v3
  6. 关闭周期补单：直接摄入被拒 -> 提交更正申请（差额+预览，不入账）
     -> 解析修复申请被驳回（不入账）-> 两笔通过的更正合并为一份说明 -> 报表 v4
"""
import argparse
import asyncio
import json
import sys
from decimal import Decimal
from pathlib import Path

# 允许脱离 PYTHONPATH 直接 `python scripts/seed.py` 运行
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import func, text
from sqlalchemy.future import select

from app.database import SessionLocal, engine
from app.demo import events as demo
from app.enums import DiscrepancyStatus
from app.init_db import init_db
from app.models import (
    BusinessEvent, ClosedPeriodCorrection, Discrepancy, LedgerEntry,
    MonthlyReport, MonthlyReportVersion, SettlementBatch,
)
from app.services.ledger import IngestError, ingest_raw_event, process_event
from app.services.reconciliation import run_reconciliation
from app.services.workflow import (
    claim, suggest_adjustment, post_adjustment, close_discrepancy, close_batch,
)
from app.services.correction_approval import (
    approve_correction, publish_consolidated_statement, reject_correction,
    submit_correction,
)

OPERATOR = "finance.chen"
APPROVER = "finance.lead"


async def _ingest(db, event):
    raw, dup = await ingest_raw_event(db, event)
    be = None
    if not dup:
        be = await process_event(db, raw)
    return raw, dup, be


async def _resolve_all(db, batch_id: int) -> dict:
    """模拟财务人员逐条处置该批次全部未关闭差异。"""
    discs = (
        await db.execute(
            select(Discrepancy).where(
                Discrepancy.batch_id == batch_id,
                Discrepancy.status != DiscrepancyStatus.CLOSED,
            )
        )
    ).scalars().all()
    summary: dict[str, int] = {}
    for d in discs:
        summary[d.kind] = summary.get(d.kind, 0) + 1
        await claim(db, d.id, OPERATOR, f"认领 {d.title}")
        # 全部走“调账建议 -> 过账 -> 关闭”，UNKNOWN 也必须留人工调账痕迹
        await suggest_adjustment(
            db, d.id, OPERATOR,
            adjust_amount=-d.diff_amount if d.diff_amount != 0 else Decimal("5.00"),
            account="MANUAL_ADJUSTMENT",
            comment=f"调账建议：{d.title}",
        )
        await post_adjustment(db, d.id, OPERATOR, "复核后过账")
        await close_discrepancy(db, d.id, OPERATOR, "材料齐全，关闭确认")
    return summary


async def main(reset: bool = False) -> None:
    if reset:
        print("== 0. 重建 schema（--reset） ==")
        await engine.dispose()
        async with engine.begin() as conn:
            await conn.execute(text("DROP SCHEMA public CASCADE"))
            await conn.execute(text("CREATE SCHEMA public"))
    await init_db()
    async with SessionLocal() as db:
        print("== 1. 摄入汇率与订单/支付事件 ==")
        raw_count = dup_count = be_count = 0
        for ev in [*demo.fx_events(), *demo.order_payment_events()]:
            _, dup, be = await _ingest(db, ev)
            raw_count += 1
            dup_count += int(dup)
            be_count += int(be is not None)
        await db.commit()
        print(f"   原始消息 {raw_count} 条，其中重复投递 {dup_count} 条，新增业务事件 {be_count} 条")

        print("== 2. 摄入银行流水（合并入账/延迟入账）与结算单 v1 ==")
        for ev in demo.bank_events():
            await _ingest(db, ev)
        await _ingest(db, demo.settlement_v1_event())
        await db.commit()
        sb1 = (
            await db.execute(
                select(SettlementBatch).where(
                    SettlementBatch.batch_ref == "SB-202610",
                    SettlementBatch.version == 1,
                )
            )
        ).scalar_one()

        print("== 3. 自动对账（引擎只生成建议，不核销） ==")
        r1 = await run_reconciliation(db, sb1.id)
        await db.commit()
        print("   " + json.dumps(r1["by_kind"], ensure_ascii=False))
        assert r1["discrepancies_open"] >= 4, "应至少产生金额/汇率/未知/时间差四类差异"

        print("== 4. 人工处置差异并关闭批次 v1 ==")
        summary = await _resolve_all(db, sb1.id)
        print(f"   人工处置差异分类统计：{summary}")
        closed1 = await close_batch(db, sb1.id, OPERATOR)
        await db.commit()
        print(f"   批次关闭：{closed1}")

        print("== 5. 只追加保护验证（预期触发器报错） ==")
        try:
            await db.execute(
                text("UPDATE ledger_entries SET book_amount = 0 WHERE id = (SELECT min(id) FROM ledger_entries)")
            )
            raise SystemExit("错误：只追加触发器未生效！")
        except Exception as exc:  # 预期异常
            assert "append-only" in str(exc)
            print(f"   账本篡改被拒绝：{exc.args[0][:80]}")
            await db.rollback()
        try:
            await db.execute(text("DELETE FROM business_events WHERE 1=1"))
            raise SystemExit("错误：业务事件可被删除！")
        except Exception as exc:
            assert "append-only" in str(exc)
            print(f"   事件删除被拒绝：{exc.args[0][:80]}")
            await db.rollback()

        print("== 6. 结算单修订 v2（关闭后更正不改旧快照） ==")
        await _ingest(db, demo.settlement_v2_event())
        await db.commit()
        sb2 = (
            await db.execute(
                select(SettlementBatch).where(
                    SettlementBatch.batch_ref == "SB-202610",
                    SettlementBatch.version == 2,
                )
            )
        ).scalar_one()
        r2 = await run_reconciliation(db, sb2.id)
        await db.commit()
        print(f"   v2 差异：{json.dumps(r2['by_kind'], ensure_ascii=False)}")
        await _resolve_all(db, sb2.id)
        closed2 = await close_batch(db, sb2.id, OPERATOR)
        await db.commit()
        print(f"   v2 关闭：{closed2}")

        print("== 7. 汇率更正 7.20 -> 7.25（只追加更正账） ==")
        _, _, fx_be = await _ingest(db, demo.fx_correction_event())
        await db.commit()
        correction_entries = (
            await db.execute(
                select(LedgerEntry).where(LedgerEntry.business_event_id == fx_be.id)
            )
        ).scalars().all()
        dr = sum((e.book_amount for e in correction_entries if e.direction == "DR"), Decimal("0"))
        cr = sum((e.book_amount for e in correction_entries if e.direction == "CR"), Decimal("0"))
        print(f"   更正分录 {len(correction_entries)} 行，借 {dr} / 贷 {cr}（平衡：{dr == cr}）")
        assert dr == cr

        print("== 8. 关闭周期更正审批（补单 / 解析修复） ==")
        # 8.1 绕过审批直接补录 -> 必须被拒
        late_raw, _ = await ingest_raw_event(db, demo.late_october_refund_event())
        try:
            await process_event(db, late_raw)
            raise SystemExit("错误：关闭周期补单被直接入账！")
        except IngestError as exc:
            print(f"   直接补录被拒：{exc}")
            await db.rollback()
        # 8.2 提交补单更正申请：差额+预览生成，但不入账
        entries_before = (await db.execute(
            select(func.count()).select_from(LedgerEntry)
        )).scalar_one()
        late_event = demo.late_october_refund_event()
        supp = await submit_correction(
            db, correction_type="SUPPLEMENT", channel=demo.CHANNEL,
            merchant_id=demo.MERCHANT, report_month="2026-10",
            reason="关闭后发现 10-28 退款迟报，补录退款事实",
            impact_scope="ORD-008 / STRIPE / 退款 10 USD（手续费 0.3）",
            approver=APPROVER, operator=OPERATOR,
            declared_impact={"orders": ["ORD-008"], "channels": [demo.CHANNEL]},
            supplement_payload={**late_event.payload,
                                "idempotency_key": late_event.idempotency_key},
        )
        await db.commit()
        entries_after_submit = (await db.execute(
            select(func.count()).select_from(LedgerEntry)
        )).scalar_one()
        print(f"   提交 {supp.correction_ref} [{supp.status}]："
              f"差额行未入账（分录 {entries_before} -> {entries_after_submit}），"
              f"报表预览 v{supp.preview_report.get('preview_of_report_version')}")
        assert entries_after_submit == entries_before, "待审批差额不得入账"

        # 8.3 解析修复申请：审批驳回 -> 永不进入账本
        target = (await db.execute(
            select(BusinessEvent).where(
                BusinessEvent.business_key == "PAYMENT_CAPTURED:P-008:v1"
            )
        )).scalar_one()
        parsing = await submit_correction(
            db, correction_type="PARSING_FIX", channel=demo.CHANNEL,
            merchant_id=demo.MERCHANT, report_month="2026-10",
            reason="ORD-008 源报文解析修复：本金 70 -> 75",
            impact_scope="ORD-008 / STRIPE / 本金差额 +5 USD",
            approver=APPROVER, operator=OPERATOR,
            parsing_fix={"business_event_id": target.id,
                         "corrected_amount": "75", "note": "解析修复"},
        )
        await reject_correction(db, parsing.id, APPROVER, "渠道对账单未支持该修正，驳回")
        await db.commit()
        entries_after_reject = (await db.execute(
            select(func.count()).select_from(LedgerEntry)
        )).scalar_one()
        print(f"   解析修复 {parsing.correction_ref} 被驳回：分录 {entries_after_reject} 行未变")
        assert entries_after_reject == entries_before, "驳回的更正不得进入账本"

        # 8.4 审批通过补单：差额过账 + 守恒复核 + 新报表版本
        approval = await approve_correction(
            db, supp.id, APPROVER, "核对渠道退款单与银行流水，通过"
        )
        await db.commit()
        print(f"   {supp.correction_ref} 审批通过：差额分录 {approval['correction_entries']} 行，"
              f"守恒复核={approval['ledger_conservation']['balanced']}，"
              f"报表版本={approval['report_version']}")

        # 8.5 同周期多笔已通过未结更正合并出具一份说明
        second = await submit_correction(
            db, correction_type="PARSING_FIX", channel=demo.CHANNEL,
            merchant_id=demo.MERCHANT, report_month="2026-10",
            reason="ORD-008 解析修复复核通过，按正确金额补差",
            impact_scope="ORD-008 / STRIPE / 本金差额 +5 USD",
            approver=APPROVER, operator=OPERATOR,
            parsing_fix={"business_event_id": target.id,
                         "corrected_amount": "75", "note": "解析修复"},
        )
        await db.commit()
        await approve_correction(
            db, second.id, APPROVER, "复核对账单后通过"
        )
        await db.commit()
        report = (await db.execute(select(MonthlyReport))).scalar_one()
        statement = await publish_consolidated_statement(db, report.id, APPROVER)
        await db.commit()
        snap = statement.snapshot
        print(f"   合并更正说明 {statement.statement_ref}：{statement.correction_count} 笔更正，"
              f"受影响订单 {[o['order_ref'] for o in snap['affected_orders']]}，"
              f"渠道 {snap['affected_channels']}，"
              f"金额 {snap['totals']['book_amount']} {snap['totals']['book_currency']}")

        print("== 9. 最终汇总 ==")
        corrections = (await db.execute(
            select(ClosedPeriodCorrection).order_by(ClosedPeriodCorrection.id)
        )).scalars().all()
        for c in corrections:
            print(f"   更正 {c.correction_ref} [{c.status}] "
                  f"审批人={c.approver} 差额事件={c.delta_business_event_id} "
                  f"说明={c.statement_id}")
        versions = (
            await db.execute(
                select(MonthlyReportVersion).order_by(MonthlyReportVersion.version)
            )
        ).scalars().all()
        for v in versions:
            print(
                f"   报表 v{v.version} [{v.version_status}] "
                f"借贷平衡={v.snapshot['conservation_check']['balanced']} "
                f"事件数={v.snapshot['business_event_count']} 原因={v.reason}"
            )
        balances = (
            await db.execute(
                select(
                    LedgerEntry.direction,
                    func.sum(LedgerEntry.book_amount),
                ).group_by(LedgerEntry.direction)
            )
        ).all()
        print(f"   全账本借贷：{balances}")
        print("SEED_DONE")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="装载差异处理中心模拟数据")
    parser.add_argument(
        "--reset", action="store_true",
        help="先 DROP/CREATE public schema 再装载（演示环境一键重置）",
    )
    args = parser.parse_args()
    asyncio.run(main(reset=args.reset))

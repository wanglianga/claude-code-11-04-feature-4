"""建表与只追加触发器。

只追加表的证据列（payload、金额、业务键、事件时间等）禁止 UPDATE/DELETE；
raw_events 的处理生命周期列（status/processed_at/reject_reason）允许更新，
但不允许改动任何原始证据。
"""
from sqlalchemy import text

from .database import engine
from .models import Base, APPEND_ONLY_TABLES

APPEND_ONLY_FN = """
CREATE OR REPLACE FUNCTION recon_reject_mutation() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'TRUNCATE' THEN
        RAISE EXCEPTION 'table % is append-only: TRUNCATE is not allowed', TG_TABLE_NAME;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'table % is append-only: DELETE is not allowed', TG_TABLE_NAME;
    END IF;
    -- raw_events 仅允许处理生命周期列变化
    IF TG_TABLE_NAME = 'raw_events' THEN
        IF NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
           OR NEW.source IS DISTINCT FROM OLD.source
           OR NEW.event_occurred_at IS DISTINCT FROM OLD.event_occurred_at
           OR NEW.topic IS DISTINCT FROM OLD.topic
           OR NEW.payload IS DISTINCT FROM OLD.payload
           OR NEW.received_at IS DISTINCT FROM OLD.received_at THEN
            RAISE EXCEPTION 'raw_events evidentiary columns are immutable';
        END IF;
        RETURN NEW;
    END IF;
    -- monthly_report_versions 仅允许版本状态列 CURRENT->SUPERSEDED，快照证据列禁改
    IF TG_TABLE_NAME = 'monthly_report_versions' THEN
        IF NEW.report_id IS DISTINCT FROM OLD.report_id
           OR NEW.version IS DISTINCT FROM OLD.version
           OR NEW.snapshot IS DISTINCT FROM OLD.snapshot
           OR NEW.reason IS DISTINCT FROM OLD.reason
           OR NEW.created_at IS DISTINCT FROM OLD.created_at THEN
            RAISE EXCEPTION 'monthly_report_versions snapshot columns are immutable';
        END IF;
        RETURN NEW;
    END IF;
    -- closed_period_corrections 的审批状态/结论列允许流转，申请证据列禁改
    IF TG_TABLE_NAME = 'closed_period_corrections' THEN
        IF NEW.correction_ref IS DISTINCT FROM OLD.correction_ref
           OR NEW.correction_type IS DISTINCT FROM OLD.correction_type
           OR NEW.report_id IS DISTINCT FROM OLD.report_id
           OR NEW.report_month IS DISTINCT FROM OLD.report_month
           OR NEW.channel IS DISTINCT FROM OLD.channel
           OR NEW.merchant_id IS DISTINCT FROM OLD.merchant_id
           OR NEW.currency IS DISTINCT FROM OLD.currency
           OR NEW.reason IS DISTINCT FROM OLD.reason
           OR NEW.impact_scope IS DISTINCT FROM OLD.impact_scope
           OR NEW.declared_impact IS DISTINCT FROM OLD.declared_impact
           OR NEW.submitted_by IS DISTINCT FROM OLD.submitted_by
           OR NEW.submitted_at IS DISTINCT FROM OLD.submitted_at
           OR NEW.preview_report IS DISTINCT FROM OLD.preview_report THEN
            RAISE EXCEPTION 'closed_period_corrections application columns are immutable';
        END IF;
        -- 终态行不可再改（包括结论列）
        IF OLD.status IN ('REJECTED', 'CONSOLIDATED') AND NEW.status = OLD.status THEN
            RAISE EXCEPTION 'correction in terminal status % is immutable', OLD.status;
        END IF;
        -- 状态机：PENDING -> APPROVED / REJECTED，APPROVED -> CONSOLIDATED
        IF NOT (
            (OLD.status = 'PENDING' AND NEW.status IN ('APPROVED', 'REJECTED'))
            OR (OLD.status = 'APPROVED' AND NEW.status IN ('APPROVED', 'CONSOLIDATED'))
        ) THEN
            RAISE EXCEPTION 'illegal correction status transition % -> %', OLD.status, NEW.status;
        END IF;
        -- 结论列在离开 PENDING / 合并出具时一次性写入，此后不再变化
        IF OLD.status = 'PENDING' AND NEW.status IN ('APPROVED', 'REJECTED') THEN
            IF NEW.status = 'REJECTED' AND NEW.decision_comment IS NULL THEN
                RAISE EXCEPTION 'rejection must carry a decision comment';
            END IF;
        ELSIF NEW.decided_at IS DISTINCT FROM OLD.decided_at
           OR NEW.decision_comment IS DISTINCT FROM OLD.decision_comment THEN
            RAISE EXCEPTION 'correction decision columns are write-once';
        END IF;
        IF OLD.statement_id IS NOT NULL
           AND NEW.statement_id IS DISTINCT FROM OLD.statement_id THEN
            RAISE EXCEPTION 'correction statement link is write-once';
        END IF;
        IF OLD.delta_business_event_id IS NOT NULL
           AND NEW.delta_business_event_id IS DISTINCT FROM OLD.delta_business_event_id THEN
            RAISE EXCEPTION 'correction delta event is write-once';
        END IF;
        IF OLD.status <> 'PENDING'
           AND NEW.conservation_check IS DISTINCT FROM OLD.conservation_check THEN
            RAISE EXCEPTION 'correction conservation check is write-once';
        END IF;
        IF OLD.impact_detail IS DISTINCT FROM NEW.impact_detail
           AND OLD.status IN ('REJECTED', 'CONSOLIDATED') THEN
            RAISE EXCEPTION 'correction impact statement is immutable once decided';
        END IF;
        IF OLD.delta_business_event_id IS NOT NULL
           AND NEW.delta_business_event_id IS DISTINCT FROM OLD.delta_business_event_id THEN
            RAISE EXCEPTION 'correction delta event is write-once';
        END IF;
        IF OLD.status <> 'PENDING'
           AND NEW.conservation_check IS DISTINCT FROM OLD.conservation_check THEN
            RAISE EXCEPTION 'correction conservation check is write-once';
        END IF;
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'table % is append-only: UPDATE is not allowed', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;
"""

# 关闭结论保护：批次/报表一经关闭，关闭时间、关闭人与金额口径不得回改，
# 更正只能通过审批后的新版本在 L4 汇总层体现。
CLOSED_CONCLUSION_FN = """
CREATE OR REPLACE FUNCTION recon_protect_closed_conclusion() RETURNS trigger AS $$
BEGIN
    IF OLD.status <> 'CLOSED' THEN
        RETURN NEW;
    END IF;
    IF NEW.status <> 'CLOSED' THEN
        RAISE EXCEPTION 'closed conclusion on table % cannot be reopened', TG_TABLE_NAME;
    END IF;
    IF NEW.closed_at IS DISTINCT FROM OLD.closed_at
       AND NEW.closed_at IS NOT NULL THEN
        RAISE EXCEPTION 'closed conclusion timestamp is immutable on %', TG_TABLE_NAME;
    END IF;
    IF TG_TABLE_NAME = 'settlement_batches' THEN
        IF NEW.batch_ref IS DISTINCT FROM OLD.batch_ref
           OR NEW.version IS DISTINCT FROM OLD.version
           OR NEW.channel IS DISTINCT FROM OLD.channel
           OR NEW.merchant_id IS DISTINCT FROM OLD.merchant_id
           OR NEW.period_start IS DISTINCT FROM OLD.period_start
           OR NEW.period_end IS DISTINCT FROM OLD.period_end
           OR NEW.currency IS DISTINCT FROM OLD.currency
           OR NEW.gross_amount IS DISTINCT FROM OLD.gross_amount
           OR NEW.fee_amount IS DISTINCT FROM OLD.fee_amount
           OR NEW.net_amount IS DISTINCT FROM OLD.net_amount
           OR NEW.closed_by IS DISTINCT FROM OLD.closed_by THEN
            RAISE EXCEPTION 'closed settlement batch conclusion is immutable';
        END IF;
    END IF;
    IF TG_TABLE_NAME = 'monthly_reports' THEN
        IF NEW.channel IS DISTINCT FROM OLD.channel
           OR NEW.merchant_id IS DISTINCT FROM OLD.merchant_id
           OR NEW.currency IS DISTINCT FROM OLD.currency
           OR NEW.report_month IS DISTINCT FROM OLD.report_month
           OR NEW.current_version < OLD.current_version THEN
            RAISE EXCEPTION 'closed monthly report conclusion is immutable';
        END IF;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
"""

CLOSED_CONCLUSION_TPL = """
DROP TRIGGER IF EXISTS trg_closed_conclusion_{tbl} ON {tbl};
CREATE TRIGGER trg_closed_conclusion_{tbl}
    BEFORE UPDATE ON {tbl}
    FOR EACH ROW EXECUTE FUNCTION recon_protect_closed_conclusion();
"""

# 审批流转表：安装与只追加表相同的行级触发器（状态机在函数内特判）
CORRECTION_TABLES = ["closed_period_corrections"]
CLOSED_CONCLUSION_TABLES = ["settlement_batches", "monthly_reports"]

TRIGGER_TPL = """
DROP TRIGGER IF EXISTS trg_append_only_{tbl} ON {tbl};
CREATE TRIGGER trg_append_only_{tbl}
    BEFORE UPDATE OR DELETE ON {tbl}
    FOR EACH ROW EXECUTE FUNCTION recon_reject_mutation();
"""

TRUNCATE_TRIGGER_TPL = """
DROP TRIGGER IF EXISTS trg_block_truncate_{tbl} ON {tbl};
CREATE TRIGGER trg_block_truncate_{tbl}
    BEFORE TRUNCATE ON {tbl}
    FOR EACH STATEMENT EXECUTE FUNCTION recon_reject_mutation();
"""


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # 函数定义是单条 CREATE 命令（$$ 内分号属于函数体），整条执行
        await conn.execute(text(APPEND_ONLY_FN))
        # 触发器模板含两条命令，asyncpg 预编译不支持多命令，逐句执行
        for tbl in [*APPEND_ONLY_TABLES, *CORRECTION_TABLES]:
            for tpl in (TRIGGER_TPL, TRUNCATE_TRIGGER_TPL):
                for stmt in (s.strip() for s in tpl.format(tbl=tbl).split(";") if s.strip()):
                    await conn.execute(text(stmt))
        await conn.execute(text(CLOSED_CONCLUSION_FN))
        for tbl in CLOSED_CONCLUSION_TABLES:
            for stmt in (s.strip() for s in CLOSED_CONCLUSION_TPL.format(tbl=tbl).split(";") if s.strip()):
                await conn.execute(text(stmt))

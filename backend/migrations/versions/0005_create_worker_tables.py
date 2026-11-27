"""Create inbox and downstream order processing results.

Revision ID: 0005_create_worker_tables
Revises: 0004_outbox_leases
"""
from alembic import op
import sqlalchemy as sa

revision = "0005_create_worker_tables"
down_revision = "0004_outbox_leases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "processed_messages",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("event_id", sa.String(length=36), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("schema_version", sa.Integer(), nullable=False),
        sa.Column("aggregate_type", sa.String(length=50), nullable=False),
        sa.Column("aggregate_id", sa.String(length=100), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("event_id", name="uq_processed_messages_event_id"),
    )
    op.create_index(
        "ix_processed_messages_processed_at",
        "processed_messages",
        ["processed_at"],
    )

    op.create_table(
        "order_processing_results",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "event_id",
            sa.String(length=36),
            sa.ForeignKey("processed_messages.event_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "order_id",
            sa.Integer(),
            sa.ForeignKey("orders.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("customer_reference", sa.String(length=100), nullable=False),
        sa.Column("total_amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("processing_status", sa.String(length=40), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("total_amount >= 0", name="ck_order_processing_results_total_non_negative"),
        sa.CheckConstraint(
            "processing_status IN ('READY_FOR_FULFILLMENT')",
            name="ck_order_processing_results_status_valid",
        ),
        sa.UniqueConstraint("event_id", name="uq_order_processing_results_event_id"),
        sa.UniqueConstraint("order_id", name="uq_order_processing_results_order_id"),
    )
    op.create_index(
        "ix_order_processing_results_processed_at",
        "order_processing_results",
        ["processed_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_order_processing_results_processed_at", table_name="order_processing_results")
    op.drop_table("order_processing_results")
    op.drop_index("ix_processed_messages_processed_at", table_name="processed_messages")
    op.drop_table("processed_messages")

"""Add lease columns for concurrent outbox publishers.

Revision ID: 0004_outbox_leases
Revises: 0003_create_outbox
"""
from alembic import op
import sqlalchemy as sa

revision = "0004_outbox_leases"
down_revision = "0003_create_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "outbox_events",
        sa.Column("locked_until", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "outbox_events",
        sa.Column("locked_by", sa.String(length=100), nullable=True),
    )
    op.create_index(
        "ix_outbox_events_status_available_at",
        "outbox_events",
        ["status", "available_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_events_status_available_at",
        table_name="outbox_events",
    )
    op.drop_column("outbox_events", "locked_by")
    op.drop_column("outbox_events", "locked_until")

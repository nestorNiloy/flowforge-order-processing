from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.session import Base


class ProcessedMessage(Base):
    """Inbox record that makes event consumption idempotent."""

    __tablename__ = "processed_messages"
    __table_args__ = (
        UniqueConstraint("event_id", name="uq_processed_messages_event_id"),
        Index("ix_processed_messages_processed_at", "processed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[str] = mapped_column(String(36), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False)
    aggregate_type: Mapped[str] = mapped_column(String(50), nullable=False)
    aggregate_id: Mapped[str] = mapped_column(String(100), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class OrderProcessingResult(Base):
    """Observable downstream effect created once for each order event."""

    __tablename__ = "order_processing_results"
    __table_args__ = (
        CheckConstraint(
            "total_amount >= 0",
            name="ck_order_processing_results_total_non_negative",
        ),
        CheckConstraint(
            "processing_status IN ('READY_FOR_FULFILLMENT')",
            name="ck_order_processing_results_status_valid",
        ),
        UniqueConstraint("event_id", name="uq_order_processing_results_event_id"),
        UniqueConstraint("order_id", name="uq_order_processing_results_order_id"),
        Index("ix_order_processing_results_processed_at", "processed_at"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    event_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("processed_messages.event_id", ondelete="CASCADE"),
        nullable=False,
    )
    order_id: Mapped[int] = mapped_column(
        ForeignKey("orders.id", ondelete="RESTRICT"),
        nullable=False,
    )
    customer_reference: Mapped[str] = mapped_column(String(100), nullable=False)
    total_amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    processing_status: Mapped[str] = mapped_column(
        String(40), nullable=False, default="READY_FOR_FULFILLMENT"
    )
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

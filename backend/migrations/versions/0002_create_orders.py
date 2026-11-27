"""Create orders and order_items tables and enforce product inventory checks.

Revision ID: 0002_create_orders
Revises: 0001_create_products
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_create_orders"
down_revision = "0001_create_products"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_check_constraint(
        "ck_products_available_quantity_non_negative",
        "products",
        "available_quantity >= 0",
    )
    op.create_check_constraint(
        "ck_products_unit_price_non_negative",
        "products",
        "unit_price >= 0",
    )

    op.create_table(
        "orders",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("customer_reference", sa.String(length=100), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("total_amount", sa.Numeric(precision=12, scale=2), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("total_amount >= 0", name="ck_orders_total_amount_non_negative"),
        sa.CheckConstraint("status IN ('CONFIRMED', 'CANCELLED')", name="ck_orders_status_valid"),
    )
    op.create_index("ix_orders_customer_reference", "orders", ["customer_reference"])

    op.create_table(
        "order_items",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("order_id", sa.Integer(), nullable=False),
        sa.Column("product_id", sa.Integer(), nullable=False),
        sa.Column("product_name_snapshot", sa.String(length=160), nullable=False),
        sa.Column("unit_price_snapshot", sa.Numeric(precision=10, scale=2), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.CheckConstraint("quantity > 0", name="ck_order_items_quantity_positive"),
        sa.CheckConstraint("unit_price_snapshot >= 0", name="ck_order_items_price_non_negative"),
        sa.ForeignKeyConstraint(["order_id"], ["orders.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["product_id"], ["products.id"], ondelete="RESTRICT"),
        sa.UniqueConstraint("order_id", "product_id", name="uq_order_items_order_product"),
    )
    op.create_index("ix_order_items_order_id", "order_items", ["order_id"])


def downgrade() -> None:
    op.drop_index("ix_order_items_order_id", table_name="order_items")
    op.drop_table("order_items")
    op.drop_index("ix_orders_customer_reference", table_name="orders")
    op.drop_table("orders")
    op.drop_constraint("ck_products_unit_price_non_negative", "products", type_="check")
    op.drop_constraint("ck_products_available_quantity_non_negative", "products", type_="check")

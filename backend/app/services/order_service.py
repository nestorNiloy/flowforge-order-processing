from decimal import Decimal

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.models.order import Order, OrderItem, OrderStatus
from app.models.product import Product
from app.schemas.order import OrderCreate


class ProductNotFoundError(Exception):
    def __init__(self, product_id: int):
        self.product_id = product_id


class InsufficientStockError(Exception):
    def __init__(self, product_id: int):
        self.product_id = product_id


def create_order(db: Session, payload: OrderCreate) -> Order:
    """Create an order and reserve all requested stock in one transaction.

    The conditional UPDATE is the concurrency boundary: PostgreSQL only decrements
    stock when enough units remain, preventing read-then-write overselling.
    """
    with db.begin():
        snapshots: list[tuple[int, str, Decimal, int]] = []
        total = Decimal("0.00")

        # A stable lock/update order reduces deadlock risk for overlapping orders.
        for item in sorted(payload.items, key=lambda line: line.product_id):
            exists = db.scalar(select(Product.id).where(Product.id == item.product_id))
            if exists is None:
                raise ProductNotFoundError(item.product_id)

            reserved = db.execute(
                update(Product)
                .where(
                    Product.id == item.product_id,
                    Product.available_quantity >= item.quantity,
                )
                .values(available_quantity=Product.available_quantity - item.quantity)
                .returning(Product.id, Product.name, Product.unit_price)
            ).one_or_none()

            if reserved is None:
                raise InsufficientStockError(item.product_id)

            product_id, product_name, unit_price = reserved
            line_total = unit_price * item.quantity
            total += line_total
            snapshots.append((product_id, product_name, unit_price, item.quantity))

        order = Order(
            customer_reference=payload.customer_reference.strip(),
            status=OrderStatus.CONFIRMED.value,
            total_amount=total,
        )
        order.items = [
            OrderItem(
                product_id=product_id,
                product_name_snapshot=product_name,
                unit_price_snapshot=unit_price,
                quantity=quantity,
            )
            for product_id, product_name, unit_price, quantity in snapshots
        ]
        db.add(order)
        db.flush()
        db.refresh(order)
        return order


def get_order(db: Session, order_id: int) -> Order | None:
    from sqlalchemy.orm import selectinload

    return db.scalar(
        select(Order).options(selectinload(Order.items)).where(Order.id == order_id)
    )

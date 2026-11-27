from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class OrderItemCreate(BaseModel):
    product_id: int = Field(gt=0)
    quantity: int = Field(gt=0, le=100_000)


class OrderCreate(BaseModel):
    customer_reference: str = Field(min_length=1, max_length=100)
    items: list[OrderItemCreate] = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def product_ids_must_be_unique(self):
        product_ids = [item.product_id for item in self.items]
        if len(product_ids) != len(set(product_ids)):
            raise ValueError("Each product may appear only once in an order.")
        return self


class OrderItemRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    product_id: int
    product_name_snapshot: str
    unit_price_snapshot: Decimal
    quantity: int


class OrderRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    customer_reference: str
    status: str
    total_amount: Decimal
    created_at: datetime
    items: list[OrderItemRead]

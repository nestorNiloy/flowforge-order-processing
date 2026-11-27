from fastapi import FastAPI, HTTPException, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.core.config import get_settings
from app.db.session import engine
from app.routers.orders import router as orders_router

settings = get_settings()
app = FastAPI(title=settings.app_name, version="0.2.0")
app.include_router(orders_router)


@app.get("/", tags=["system"])
def root() -> dict[str, str]:
    return {"name": settings.app_name, "status": "running"}


@app.get("/health", tags=["system"])
def health() -> dict[str, str]:
    """Report application and database connectivity."""
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
    except SQLAlchemyError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"status": "degraded", "database": "unavailable"},
        ) from exc
    return {"status": "ok", "database": "connected"}

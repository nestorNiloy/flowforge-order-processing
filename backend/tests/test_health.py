from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError

from app.main import app

client = TestClient(app)


def test_root_returns_running_status():
    response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {"name": "FlowForge", "status": "running"}


def test_health_reports_database_connection(monkeypatch):
    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _statement):
            return None

    monkeypatch.setattr("app.main.engine.connect", lambda: FakeConnection())

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "connected"}


def test_health_returns_503_when_database_is_unavailable(monkeypatch):
    def fail_to_connect():
        raise OperationalError("SELECT 1", {}, Exception("database unavailable"))

    monkeypatch.setattr("app.main.engine.connect", fail_to_connect)

    response = client.get("/health")

    assert response.status_code == 503
    assert response.json()["detail"] == {
        "status": "degraded",
        "database": "unavailable",
    }

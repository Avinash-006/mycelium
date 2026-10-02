import pytest

from mycelium import ConfigError, load_config_from_string
from mycelium.config import MyceliumConfig


@pytest.mark.parametrize(
    "storage,field",
    [
        ("redis", "retention_seconds"),
        ("redis", "in_flight_ttl"),
        ("postgres", "retention_seconds"),
    ],
)
@pytest.mark.parametrize(
    "value",
    [
        "true",
        "false",
        ".nan",
        ".inf",
        "-.inf",
        "0",
        "-1",
        "-0.5",
        '"300"',
    ],
)
def test_action_ledger_timing_config_errors(storage, field, value, monkeypatch):
    if storage == "redis":
        import mycelium.storage.redis_ledger as redis_mod

        def unexpected_storage(*args, **kwargs):
            pytest.fail("invalid timing fields must fail before storage construction")

        monkeypatch.setattr(redis_mod, "RedisLedgerStorage", unexpected_storage)
        yaml_text = (
            "action_ledger:\n"
            "  storage: redis\n"
            "  url: redis://localhost:6379/0\n"
            f"  {field}: {value}\n"
        )
        cfg = load_config_from_string(yaml_text)
    else:
        import mycelium.storage.postgres_ledger as postgres_mod

        def unexpected_storage(*args, **kwargs):
            pytest.fail("invalid timing fields must fail before storage construction")

        monkeypatch.setattr(postgres_mod, "PostgresLedgerStorage", unexpected_storage)
        yaml_text = (
            "action_ledger:\n"
            "  storage: postgres\n"
            "  dsn: postgresql://localhost/test\n"
            f"  {field}: {value}\n"
        )
        cfg = load_config_from_string(yaml_text)

    with pytest.raises(ConfigError, match=rf"ledger\.{field}"):
        MyceliumConfig._build_ledger_storage(cfg.action_ledger)


def test_redis_task_ledger_construction_unaffected(monkeypatch):
    import mycelium.storage.redis_ledger as redis_mod

    class DummyRedis:
        @classmethod
        def from_url(cls, *args, **kwargs):
            return object()

    monkeypatch.setattr(redis_mod, "_require_redis", lambda: type("Mod", (), {"Redis": DummyRedis}))

    # Default construction with omitted timing fields
    storage = MyceliumConfig._build_task_ledger_storage(
        {"storage": "redis", "url": "redis://localhost:6379/0"}
    )
    assert isinstance(storage, redis_mod.RedisTaskLedgerStorage)
    assert storage._inner._in_flight_ttl == 604800.0

    # Custom in_flight_ttl
    storage_custom = MyceliumConfig._build_task_ledger_storage(
        {"storage": "redis", "url": "redis://localhost:6379/0", "in_flight_ttl": 3600}
    )
    assert isinstance(storage_custom, redis_mod.RedisTaskLedgerStorage)
    assert storage_custom._inner._in_flight_ttl == 3600.0


def test_redis_action_ledger_construction(monkeypatch):
    import mycelium.storage.redis_ledger as redis_mod

    class DummyRedis:
        @classmethod
        def from_url(cls, *args, **kwargs):
            return object()

    monkeypatch.setattr(redis_mod, "_require_redis", lambda: type("Mod", (), {"Redis": DummyRedis}))

    # Default construction with omitted timing fields
    storage = MyceliumConfig._build_ledger_storage(
        {"storage": "redis", "url": "redis://localhost:6379/0"}
    )
    assert isinstance(storage, redis_mod.RedisLedgerStorage)
    assert storage._inner._in_flight_ttl == 604800.0
    assert storage.retention_seconds is None

    # Custom timing
    storage_custom = MyceliumConfig._build_ledger_storage(
        {
            "storage": "redis",
            "url": "redis://localhost:6379/0",
            "in_flight_ttl": 1800,
            "retention_seconds": 86400,
        }
    )
    assert isinstance(storage_custom, redis_mod.RedisLedgerStorage)
    assert storage_custom._inner._in_flight_ttl == 1800.0
    assert storage_custom.retention_seconds == 86400.0


def test_valid_redis_timing_forwarded(monkeypatch):
    import mycelium.storage.redis_ledger as redis_mod

    captured = {}
    sentinel = object()

    def storage(url, **kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(redis_mod, "RedisLedgerStorage", storage)
    result = MyceliumConfig._build_ledger_storage(
        {
            "storage": "redis",
            "url": "redis://localhost:6379/0",
            "in_flight_ttl": 3600,
            "retention_seconds": 86400.5,
        }
    )
    assert result is sentinel
    assert captured["in_flight_ttl"] == 3600.0
    assert captured["retention_seconds"] == 86400.5


def test_redis_timing_defaults_and_null(monkeypatch):
    import mycelium.storage.redis_ledger as redis_mod

    captured = {}
    sentinel = object()

    def storage(url, **kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(redis_mod, "RedisLedgerStorage", storage)

    # Defaults when omitted
    MyceliumConfig._build_ledger_storage({"storage": "redis", "url": "redis://localhost:6379/0"})
    assert captured["in_flight_ttl"] == 604800.0
    assert captured["retention_seconds"] is None

    # Explicit null
    captured.clear()
    MyceliumConfig._build_ledger_storage(
        {
            "storage": "redis",
            "url": "redis://localhost:6379/0",
            "in_flight_ttl": None,
            "retention_seconds": None,
        }
    )
    assert captured["in_flight_ttl"] is None
    assert captured["retention_seconds"] is None


def test_valid_postgres_timing_forwarded(monkeypatch):
    import mycelium.storage.postgres_ledger as postgres_mod

    captured = {}
    sentinel = object()

    def storage(dsn, **kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(postgres_mod, "PostgresLedgerStorage", storage)
    result = MyceliumConfig._build_ledger_storage(
        {
            "storage": "postgres",
            "dsn": "postgresql://localhost/test",
            "retention_seconds": 1209600,
        }
    )
    assert result is sentinel
    assert captured["retention_seconds"] == 1209600.0

    # Default when omitted
    captured.clear()
    MyceliumConfig._build_ledger_storage(
        {"storage": "postgres", "dsn": "postgresql://localhost/test"}
    )
    assert captured["retention_seconds"] is None


@pytest.mark.parametrize(
    "value", [True, False, float("nan"), float("inf"), float("-inf"), 0, -1, -0.5]
)
def test_direct_redis_storage_constructor_validation(value, monkeypatch):
    import mycelium.storage.redis_ledger as redis_mod

    # Mock redis module so we do not attempt actual connection
    class DummyRedis:
        @classmethod
        def from_url(cls, *args, **kwargs):
            return object()

    monkeypatch.setattr(redis_mod, "_require_redis", lambda: type("Mod", (), {"Redis": DummyRedis}))

    with pytest.raises(ValueError, match="in_flight_ttl"):
        redis_mod.RedisLedgerStorage("redis://localhost:6379/0", in_flight_ttl=value)

    with pytest.raises(ValueError, match="retention_seconds"):
        redis_mod.RedisLedgerStorage("redis://localhost:6379/0", retention_seconds=value)


@pytest.mark.parametrize(
    "value", [True, False, float("nan"), float("inf"), float("-inf"), 0, -1, -0.5]
)
def test_direct_postgres_storage_constructor_validation(value, monkeypatch):
    pytest.importorskip("psycopg")
    import mycelium.storage.postgres_ledger as postgres_mod

    with pytest.raises(ValueError, match="retention_seconds"):
        postgres_mod.PostgresLedgerStorage("postgresql://localhost/test", retention_seconds=value)

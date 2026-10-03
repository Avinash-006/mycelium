import pytest

from mycelium import ConfigError, load_config_from_string
from mycelium.config import MyceliumConfig


@pytest.mark.parametrize("field", ["pool_min_size", "pool_max_size"])
@pytest.mark.parametrize(
    "value", ["true", "false", ".nan", ".inf", "-.inf", "0", "-1", "1.5", '"2"', "null"]
)
def test_postgres_pool_size_config_errors(field, value, monkeypatch):
    import mycelium.storage.postgres_ledger as postgres

    def unexpected_storage(*args, **kwargs):
        pytest.fail("invalid pool sizes must fail before storage construction")

    monkeypatch.setattr(postgres, "PostgresLedgerStorage", unexpected_storage)
    cfg = load_config_from_string(
        "action_ledger:\n  storage: postgres\n  dsn: postgresql://localhost/test\n"
        f"  {field}: {value}\n"
    )
    with pytest.raises(ConfigError, match=rf"ledger\.{field}"):
        MyceliumConfig._build_ledger_storage(cfg.action_ledger)


def test_pool_minimum_cannot_exceed_maximum():
    with pytest.raises(ConfigError, match="pool_min_size.*pool_max_size"):
        MyceliumConfig._build_ledger_storage(
            {
                "storage": "postgres",
                "dsn": "postgresql://localhost/test",
                "pool_min_size": 3,
                "pool_max_size": 2,
            }
        )


@pytest.mark.parametrize(
    "sizes, expected", [({}, (1, 10)), ({"pool_min_size": 2, "pool_max_size": 3}, (2, 3))]
)
def test_valid_pool_sizes_forwarded(sizes, expected, monkeypatch):
    import mycelium.storage.postgres_ledger as postgres

    captured = {}
    sentinel = object()

    def storage(dsn, **kwargs):
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(postgres, "PostgresLedgerStorage", storage)
    result = MyceliumConfig._build_ledger_storage(
        {"storage": "postgres", "dsn": "postgresql://localhost/test", **sizes}
    )
    assert result is sentinel
    assert (captured["pool_min_size"], captured["pool_max_size"]) == expected

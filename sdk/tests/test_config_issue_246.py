import pytest

from mycelium import ConfigError, load_config_from_string
from mycelium.config import MyceliumConfig


@pytest.mark.parametrize("value", ['"false"', '"true"', '""', "0", "1", "null", "[]", "{}"])
def test_flush_on_complete_requires_boolean(value):
    with pytest.raises(ConfigError, match=r"state_flush\.flush_on_complete"):
        load_config_from_string(f"state_flush:\n  flush_on_complete: {value}\n")


@pytest.mark.parametrize("value", ["false", 0, 1, None, [], {}])
def test_direct_builder_rejects_invalid_boolean(value, monkeypatch):
    cfg = MyceliumConfig(
        tools={}, registry_allowed=[], runner_settings={}, state_flush={"flush_on_complete": value}
    )

    def unexpected_storage(*args):
        pytest.fail("invalid configuration must fail before creating storage")

    monkeypatch.setattr(cfg, "_build_state_flush_storage", unexpected_storage)
    with pytest.raises(ConfigError, match=r"state_flush\.flush_on_complete"):
        cfg.build_state_flush()


@pytest.mark.parametrize(
    "yaml, expected",
    [("{}", True), ("{flush_on_complete: true}", True), ("{flush_on_complete: false}", False)],
)
def test_flush_boolean_defaults(yaml, expected):
    cfg = load_config_from_string(f"state_flush: {yaml}\n")
    assert cfg.build_state_flush()._flush_on_complete is expected

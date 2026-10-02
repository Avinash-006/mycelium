import pytest

from mycelium import ConfigError, load_config_from_string


@pytest.mark.parametrize("value", ['"false"', '"true"', '""', "0", "1", "null", "[]", "{}"])
def test_mapping_enabled_requires_boolean(value):
    with pytest.raises(ConfigError, match=r"message_validator\.enabled"):
        load_config_from_string(f"message_validator:\n  enabled: {value}\n")


@pytest.mark.parametrize(
    "yaml, expected",
    [
        ("true", True),
        ("false", False),
        ("{enabled: true}", True),
        ("{enabled: false}", False),
        ("{}", True),
    ],
)
def test_boolean_and_default_enabled(yaml, expected):
    cfg = load_config_from_string(f"message_validator: {yaml}\n")
    assert cfg.message_validator is expected
    assert (cfg.build_message_validator() is not None) is expected

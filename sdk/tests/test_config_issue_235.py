import pytest

from mycelium import ConfigError, load_config_from_string


def config(ttl):
    return load_config_from_string(f"""
destructive_confirm:
  enabled: true
  tools:
    delete:
      operation: delete
      object: {{type: file, id_from: path}}
      grant: {{ttl_seconds: {ttl}}}
""")


@pytest.mark.parametrize("ttl", [".nan", ".inf", "-.inf", "0", "-1", "true", "false"])
def test_invalid_grant_ttl(ttl):
    with pytest.raises(ConfigError, match=r"delete\.grant\.ttl_seconds"):
        config(ttl)


@pytest.mark.parametrize("ttl", ["1", "0.5", "300"])
def test_finite_grant_ttl(ttl):
    cfg = config(ttl)
    assert cfg.destructive_confirm["tools"]["delete"]["grant"]["ttl_seconds"] == float(ttl)

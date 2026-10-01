"""Ledger payload retention must not change execution identity or replay safety."""

import pytest

from mycelium import (
    LedgerHardBlockError,
    LedgerPayloadPolicy,
    SqliteLedgerStorage,
    get_ledger,
    ledger,
    ledger_sync,
    load_config_from_string,
)
from mycelium.ledger_storage import InMemoryLedgerStorage
from mycelium.tool_boundary import ToolBoundaryError


def test_omitted_args_preserve_identity_and_block_drift() -> None:
    storage = InMemoryLedgerStorage()
    calls = []

    @ledger_sync(storage=storage, payload_policy=LedgerPayloadPolicy(store_args=False))
    def send(message_body: str) -> str:
        calls.append(message_body)
        return "sent"

    assert send("private", request_id="mail-1") == "sent"
    entry = get_ledger(send).get("mail-1")
    assert entry is not None
    assert entry.args == []
    assert entry.kwargs == {"request_id": "mail-1"}
    assert entry.args_digest
    assert "private" not in str(entry.to_dict())
    assert send("private", request_id="mail-1") == "sent"
    with pytest.raises(ToolBoundaryError):
        send("changed", request_id="mail-1")
    assert calls == ["private"]


def test_omitted_result_blocks_replay_without_reexecution() -> None:
    calls = []

    @ledger_sync(payload_policy=LedgerPayloadPolicy(store_result=False))
    def charge() -> dict[str, str]:
        calls.append("charge")
        return {"receipt": "sensitive"}

    assert charge(request_id="charge-1") == {"receipt": "sensitive"}
    entry = get_ledger(charge).get("charge-1")
    assert entry is not None and entry.result is None and not entry.result_retained
    with pytest.raises(LedgerHardBlockError, match="no replayable result"):
        charge(request_id="charge-1")
    assert calls == ["charge"]


def test_redacted_result_blocks_replay() -> None:
    @ledger_sync(payload_policy=LedgerPayloadPolicy(redact_fields=("token",)))
    def tool() -> dict[str, object]:
        return {"nested": [{"token": "secret", "ok": True}]}

    assert tool(request_id="redact-1") == {"nested": [{"token": "secret", "ok": True}]}
    entry = get_ledger(tool).get("redact-1")
    assert entry is not None
    assert entry.result == {"nested": [{"token": "[REDACTED]", "ok": True}]}
    assert not entry.result_retained
    with pytest.raises(LedgerHardBlockError, match="no replayable result"):
        tool(request_id="redact-1")


def test_sqlite_reopen_preserves_omission_and_drift_check(tmp_path) -> None:
    path = tmp_path / "ledger.db"

    @ledger_sync(
        storage=SqliteLedgerStorage(path),
        payload_policy=LedgerPayloadPolicy(store_args=False),
    )
    def send(body: str) -> str:
        return "sent"

    assert send("secret body", request_id="mail-2") == "sent"
    assert b"secret body" not in path.read_bytes()

    @ledger_sync(
        storage=SqliteLedgerStorage(path),
        payload_policy=LedgerPayloadPolicy(store_args=False),
    )
    def send(body: str) -> str:  # noqa: F811
        pytest.fail("completed transition must not execute again")

    assert send("secret body", request_id="mail-2") == "sent"
    with pytest.raises(ToolBoundaryError):
        send("different body", request_id="mail-2")


def test_yaml_policy_is_applied() -> None:
    config = load_config_from_string(
        """
transition: {agent_id: test-agent, policy_version: '1'}
action_ledger:
  storage: memory
  tools: [send]
  payload_policy:
    store_args: false
    store_result: false
tools:
  send:
    side_effect_class: keyed_mutate
"""
    )

    @config.apply
    def send(body: str) -> str:
        return "sent"

    assert send("private", request_id="mail-3") == "sent"
    entry = get_ledger(send).get("mail-3")
    assert entry is not None and entry.args == [] and not entry.result_retained
    with pytest.raises(LedgerHardBlockError, match="no replayable result"):
        send("private", request_id="mail-3")


async def test_async_omitted_result_refuses_replay() -> None:
    calls = []

    @ledger(payload_policy=LedgerPayloadPolicy(store_result=False))
    async def send() -> None:
        calls.append("send")

    assert await send(request_id="async-1") is None
    with pytest.raises(LedgerHardBlockError, match="no replayable result"):
        await send(request_id="async-1")
    assert calls == ["send"]


def test_legitimate_none_is_replayable() -> None:
    calls = []

    @ledger_sync()
    def send() -> None:
        calls.append("send")

    assert send(request_id="none-1") is None
    assert send(request_id="none-1") is None
    assert calls == ["send"]

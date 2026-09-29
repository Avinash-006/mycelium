"""Language-neutral composite parent control over ordinary sidecar effects."""

from __future__ import annotations

import pytest

from mycelium.action_ledger import ActionLedger
from mycelium.ledger_storage import FileLedgerStorage
from mycelium.sidecar import SidecarConfig, SidecarError, SidecarService


def _service(tmp_path) -> SidecarService:
    config = SidecarConfig(
        host="127.0.0.1",
        port=0,
        tenant_id="tenant-a",
        application_id="app-a",
        token="c" * 43,
        ledger_path=tmp_path / "ledger.json",
        outcome_path=tmp_path / "outcomes.json",
    )
    return SidecarService(ActionLedger(FileLedgerStorage(config.ledger_path)), config)


def _identity(name: str) -> dict:
    return {
        "tenant_id": "tenant-a",
        "application_id": "app-a",
        "business_request_id": f"job-1:{name}",
        "canonicalization_version": "jcs-1",
        "destination": {"id": name},
        "execution_scope": {"tenant": "tenant-a", "job": "job-1"},
        "identity_version": "1",
        "input": {"value": name},
        "tool_contract_version": "1",
        "tool_id": name,
    }


def _manifest() -> dict:
    return {
        "tenant_id": "tenant-a",
        "application_id": "app-a",
        "operation_id": "job-1",
        "definition": "publish-v1",
        "steps": [
            {"step_id": "create", "tool_id": "create"},
            {"step_id": "publish", "tool_id": "publish"},
        ],
    }


def _command(parent: dict, **extra) -> dict:
    return {
        "tenant_id": "tenant-a",
        "application_id": "app-a",
        "owner_id": parent["owner_id"],
        "fence": parent["fence"],
        **extra,
    }


def test_resume_replays_first_child_and_rejects_stale_parent(tmp_path) -> None:
    service = _service(tmp_path)
    parent = service.claim_composite(_manifest())
    first = service.composite_command(
        "job-1", "claim", _command(
            parent, identity=_identity("create"),
            decision={"allowed": True, "verdicts": [], "denied_reasons": []},
        ), "create"
    )
    assert first["disposition"] == "EXECUTE"
    with pytest.raises(SidecarError, match="previous composite step is unresolved"):
        service.composite_command(
            "job-1", "claim", _command(parent, identity=_identity("publish")), "publish"
        )
    child_handle = {"effect_owner_id": first["owner_id"], "effect_fence": first["fence"]}
    service.composite_command(
        "job-1", "boundary", _command(parent, identity=_identity("create"), **child_handle),
        "create"
    )
    service.composite_command(
        "job-1", "complete", _command(
            parent, identity=_identity("create"), result={"created": "record-1"}, **child_handle
        ), "create"
    )
    service.composite_command("job-1", "release", _command(parent))

    resumed = service.claim_composite(_manifest())
    assert resumed["fence"] > parent["fence"]
    with pytest.raises(SidecarError) as stale:
        service.composite_command("job-1", "finish", _command(parent))
    assert stale.value.code == "STALE_FENCE"
    replay = service.composite_command(
        "job-1", "claim", _command(resumed, identity=_identity("create")), "create"
    )
    assert replay["disposition"] == "RETURN_STORED_RESULT"
    assert replay["result"] == {"created": "record-1"}
    service.composite_command(
        "job-1", "resolve", _command(resumed, identity=_identity("create")), "create"
    )
    second = service.composite_command(
        "job-1", "claim", _command(
            resumed, identity=_identity("publish"),
            decision={"allowed": True, "verdicts": [], "denied_reasons": []},
        ), "publish"
    )
    assert second["disposition"] == "EXECUTE"
    second_handle = {"effect_owner_id": second["owner_id"], "effect_fence": second["fence"]}
    service.composite_command(
        "job-1", "boundary", _command(resumed, identity=_identity("publish"), **second_handle),
        "publish"
    )
    service.composite_command(
        "job-1", "complete", _command(
            resumed, identity=_identity("publish"), result={"published": True}, **second_handle
        ), "publish"
    )
    assert service.composite_command("job-1", "finish", _command(resumed))["status"] == "COMPLETED"
    with pytest.raises(SidecarError) as drift:
        service.claim_composite({**_manifest(), "definition": "publish-v2"})
    assert drift.value.code == "DEFINITION_DRIFT"

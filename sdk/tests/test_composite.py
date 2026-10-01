"""Behavioral coverage for the durable straight-line composite protocol."""

from __future__ import annotations

import asyncio
import builtins
import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from mycelium import (
    CompositeAuthorityError,
    CompositeBusyError,
    CompositeDefinitionDriftError,
    CompositeUnsupportedError,
    SideEffectClass,
    SqliteLedgerStorage,
    ToolTransitionBinding,
    composite,
    composite_choice,
    composite_items,
    ledger,
    ledger_sync,
    register_composite_helper,
    side_effect,
    side_effect_async,
)
from mycelium.composite import CompositeInvocation, _ControlStore, _PreparedChild


def _binding() -> ToolTransitionBinding:
    return ToolTransitionBinding.for_tool(
        agent_id="composite-test",
        policy_version="1",
        side_effect_class=SideEffectClass.KEYED_MUTATE,
        provider_idempotency_key_param="idempotency_key",
    )


def test_sqlite_composite_replays_completed_children_and_runs_remaining_step(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []
    crash = {"enabled": True}

    @ledger_sync(storage=storage, transition_binding=_binding())
    def create(idempotency_key: str) -> dict[str, str]:
        with side_effect():
            calls.append("A")
        return {"commit": "c1"}

    @ledger_sync(storage=storage, transition_binding=_binding())
    def push(idempotency_key: str) -> dict[str, str]:
        with side_effect():
            calls.append("B")
        return {"pushed": "c1"}

    @ledger_sync(storage=storage, transition_binding=_binding())
    def track(idempotency_key: str, pushed: dict[str, str]) -> dict[str, str]:
        with side_effect():
            calls.append("C")
        return {"tracked": pushed["pushed"]}

    def crash_after_b() -> None:
        if crash["enabled"]:
            raise RuntimeError("simulated process failure")

    register_composite_helper(crash_after_b)

    @composite(storage, operation_id_from=lambda _args, kwargs: kwargs["job_id"])
    def publish(job_id: str) -> dict[str, str]:
        create(idempotency_key="create-1")
        pushed = push(idempotency_key="push-1")
        crash_after_b()
        return track(idempotency_key="track-1", pushed=pushed)

    with pytest.raises(RuntimeError, match="simulated"):
        publish(job_id="job-1")
    assert calls == ["A", "B"]

    crash["enabled"] = False
    assert publish(job_id="job-1") == {"tracked": "c1"}
    assert calls == ["A", "B", "C"]

    controls = json.loads((tmp_path / "ledger.sqlite.composites.json").read_text())
    record = controls["mycelium:job-1"]
    assert record["status"] == "COMPLETED"
    assert len(record["manifest"]["steps"]) == 3
    assert record["manifest_digest"]


def test_composite_rejects_unsupported_control_flow(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        return "ok"

    with pytest.raises(CompositeUnsupportedError, match="branch cannot return early"):

        @composite(storage)
        def unsupported(operation_id: str) -> str:
            if operation_id:
                return effect(idempotency_key="x")
            return effect(idempotency_key="y")


def test_bounded_loop_resumes_each_iteration_without_repeating_effects(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[int] = []
    crash = {"enabled": True}

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str, index: int) -> int:
        with side_effect():
            calls.append(index)
        return index

    def crash_before_third(index: int) -> None:
        if index == 2 and crash["enabled"]:
            raise RuntimeError("simulated loop crash")

    register_composite_helper(crash_before_third)

    @composite(storage)
    def batch(operation_id: str) -> int:
        for index in range(3):
            crash_before_third(index)
            result = effect(idempotency_key=f"{operation_id}:{index}", index=index)
        return result

    with pytest.raises(RuntimeError, match="simulated loop crash"):
        batch(operation_id="batch-1")
    assert calls == [0, 1]
    crash["enabled"] = False
    assert batch(operation_id="batch-1") == 2
    assert calls == [0, 1, 2]
    assert batch(operation_id="batch-1") == 2
    assert calls == [0, 1, 2]

    records = json.loads((tmp_path / "ledger.sqlite.composites.json").read_text())
    record = records["mycelium:batch-1"]
    steps = record["manifest"]["steps"]
    assert len(steps) == len({step["step_id"] for step in steps}) == 3
    assert record["status"] == "COMPLETED"


def test_bounded_loop_rejects_unpinned_or_unbounded_shapes(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        return "ok"

    with pytest.raises(CompositeUnsupportedError, match="literal integer"):

        @composite(storage)
        def dynamic(operation_id: str, count: int) -> str:
            for index in range(count):
                result = effect(idempotency_key=str(index))
            return result

    with pytest.raises(CompositeUnsupportedError, match="between 1 and 32"):

        @composite(storage)
        def too_many(operation_id: str) -> str:
            for index in range(33):
                result = effect(idempotency_key=str(index))
            return result

    with pytest.raises(CompositeUnsupportedError, match="loop variable cannot be reassigned"):

        @composite(storage)
        def reassigned(operation_id: str) -> str:
            for index in range(2):
                index = 5
                result = effect(idempotency_key=str(index))
            return result

    with pytest.raises(CompositeUnsupportedError, match="unsupported executable syntax"):

        @composite(storage)
        def nested(operation_id: str) -> str:
            for index in range(2):
                for inner in range(2):
                    result = effect(idempotency_key=f"{index}:{inner}")
            return result


def test_bounded_loop_rejects_shadowed_range_before_child_effect(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[int] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str, index: int) -> int:
        with side_effect():
            calls.append(index)
        return index

    with pytest.raises(CompositeUnsupportedError, match="cannot bind range as an argument"):

        @composite(storage)
        def parameter(operation_id: str, range) -> int:
            for index in range(2):
                result = effect(idempotency_key=f"{operation_id}:{index}", index=index)
            return result

    range = builtins.range

    @composite(storage)
    def mutable_closure(operation_id: str) -> int:
        for index in range(2):
            result = effect(idempotency_key=f"{operation_id}:{index}", index=index)
        return result

    range = lambda count: (7, 8)  # noqa: E731
    with pytest.raises(CompositeUnsupportedError, match="built-in range"):
        mutable_closure(operation_id="shadowed")
    assert calls == []


def test_bounded_loop_pins_shape_with_explicit_definition(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        with side_effect():
            calls.append(idempotency_key)
        return idempotency_key

    @composite(storage, definition="batch-v1")
    def batch(operation_id: str) -> str:
        for index in range(2):
            result = effect(idempotency_key=f"{operation_id}:{index}")
        return result

    assert batch(operation_id="batch-1") == "batch-1:1"

    @composite(storage, definition="batch-v1")
    def batch(operation_id: str) -> str:
        for index in range(3):
            result = effect(idempotency_key=f"{operation_id}:{index}")
        return result

    with pytest.raises(CompositeDefinitionDriftError):
        batch(operation_id="batch-1")
    assert calls == ["batch-1:0", "batch-1:1"]


async def test_async_bounded_loop_uses_distinct_child_steps(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[int] = []

    @ledger(storage=storage, transition_binding=_binding())
    async def effect(idempotency_key: str, index: int) -> int:
        async with side_effect_async():
            calls.append(index)
        return index

    @composite(storage)
    async def batch(operation_id: str) -> int:
        for index in range(2):
            result = await effect(idempotency_key=f"{operation_id}:{index}", index=index)
        return result

    assert await batch(operation_id="async-batch") == 1
    assert await batch(operation_id="async-batch") == 1
    assert calls == [0, 1]


def test_existing_composite_child_replays_after_request_identity_change(
    tmp_path, monkeypatch
) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        with side_effect():
            calls.append(idempotency_key)
        return idempotency_key

    @composite(storage)
    def single(operation_id: str) -> str:
        return effect(idempotency_key=f"{operation_id}:effect")

    with monkeypatch.context() as patch:
        patch.setattr(_PreparedChild, "request_id", lambda self, base: base)
        assert single(operation_id="legacy") == "legacy:effect"

    assert single(operation_id="legacy") == "legacy:effect"
    assert calls == ["legacy:effect"]


def test_host_items_loop_resumes_and_rejects_changed_items(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []
    crash = {"enabled": True}

    @ledger_sync(storage=storage, transition_binding=_binding())
    def publish(idempotency_key: str, item: str) -> str:
        with side_effect():
            calls.append(item)
        return item

    def crash_before_last(item: str) -> None:
        if item == "C" and crash["enabled"]:
            raise RuntimeError("simulated item crash")

    register_composite_helper(crash_before_last)

    @composite(storage)
    def batch(operation_id: str, items: list[str]) -> str:
        for item in composite_items(items, max_items=3):
            crash_before_last(item)
            result = publish(idempotency_key=f"{operation_id}:{item}", item=item)
        return result

    with pytest.raises(RuntimeError, match="simulated item crash"):
        batch(operation_id="batch-1", items=["A", "B", "C"])
    assert calls == ["A", "B"]
    with pytest.raises(CompositeDefinitionDriftError):
        batch(operation_id="batch-1", items=["B", "A", "C"])
    with pytest.raises(CompositeDefinitionDriftError):
        batch(operation_id="batch-1", items=["A", "B"])
    assert calls == ["A", "B"]

    crash["enabled"] = False
    assert batch(operation_id="batch-1", items=["A", "B", "C"]) == "C"
    assert batch(operation_id="batch-1", items=["A", "B", "C"]) == "C"
    assert calls == ["A", "B", "C"]
    records = json.loads((tmp_path / "ledger.sqlite.composites.json").read_text())
    steps = records["mycelium:batch-1"]["manifest"]["steps"]
    assert len(steps) == len({step["step_id"] for step in steps}) == 3


def test_host_items_loop_rejects_invalid_input_before_effect(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def publish(idempotency_key: str, item: str) -> str:
        with side_effect():
            calls.append(item)
        return item

    @composite(storage)
    def batch(operation_id: str, items: list[str]) -> str:
        for item in composite_items(items, max_items=2):
            result = publish(idempotency_key=f"{operation_id}:{item}", item=item)
        return result

    for invalid in ([], ["A", "B", "C"], ("A",), [float("nan")], [{1: "A"}]):
        with pytest.raises(CompositeUnsupportedError):
            batch(operation_id="invalid", items=invalid)  # type: ignore[arg-type]
    with pytest.raises(CompositeUnsupportedError, match="active composite"):
        composite_items(["A"], max_items=2)
    assert calls == []


def test_host_items_loop_detects_mutation_before_iteration(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def publish(idempotency_key: str, item: str) -> str:
        with side_effect():
            calls.append(item)
        return item

    items = ["A", "B"]

    def change_items() -> None:
        items.reverse()

    register_composite_helper(change_items)

    @composite(storage)
    def batch(operation_id: str, items: list[str]) -> str:
        change_items()
        for item in composite_items(items, max_items=2):
            result = publish(idempotency_key=f"{operation_id}:{item}", item=item)
        return result

    with pytest.raises(CompositeDefinitionDriftError, match="changed before its loop"):
        batch(operation_id="mutated", items=items)
    assert calls == []


async def test_async_host_items_loop_replays_completed_children(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger(storage=storage, transition_binding=_binding())
    async def publish(idempotency_key: str, item: str) -> str:
        async with side_effect_async():
            calls.append(item)
        return item

    @composite(storage)
    async def batch(operation_id: str, items: list[str]) -> str:
        for item in composite_items(items, max_items=2):
            result = await publish(idempotency_key=f"{operation_id}:{item}", item=item)
        return result

    assert await batch(operation_id="async-items", items=["A", "B"]) == "B"
    assert await batch(operation_id="async-items", items=["A", "B"]) == "B"
    assert calls == ["A", "B"]


def test_input_boolean_branch_pins_path_and_replays_only_selected_children(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []
    crash = {"enabled": True}

    @ledger_sync(storage=storage, transition_binding=_binding())
    def approve(idempotency_key: str) -> str:
        with side_effect():
            calls.append("approve")
        return "approved"

    @ledger_sync(storage=storage, transition_binding=_binding())
    def deny(idempotency_key: str) -> str:
        with side_effect():
            calls.append("deny")
        return "denied"

    @ledger_sync(storage=storage, transition_binding=_binding())
    def record(idempotency_key: str, decision: str) -> str:
        with side_effect():
            calls.append(f"record:{decision}")
        return decision

    def crash_after_branch() -> None:
        if crash["enabled"]:
            raise RuntimeError("simulated crash")

    register_composite_helper(crash_after_branch)

    @composite(storage)
    def decide(operation_id: str, approved: bool) -> str:
        if approved:
            decision = approve(idempotency_key="approve")
        else:
            decision = deny(idempotency_key="deny")
        crash_after_branch()
        return record(idempotency_key="record", decision=decision)

    with pytest.raises(RuntimeError, match="simulated crash"):
        decide(operation_id="job-1", approved=True)
    assert calls == ["approve"]
    with pytest.raises(CompositeDefinitionDriftError):
        decide(operation_id="job-1", approved=False)
    assert calls == ["approve"]

    crash["enabled"] = False
    assert decide(operation_id="job-1", approved=True) == "approved"
    assert calls == ["approve", "record:approved"]
    assert decide(operation_id="job-2", approved=False) == "denied"
    assert calls == ["approve", "record:approved", "deny", "record:denied"]

    controls = json.loads((tmp_path / "ledger.sqlite.composites.json").read_text())
    assert controls["mycelium:job-1"]["manifest"]["path"] == "input:approved:then"
    assert controls["mycelium:job-2"]["manifest"]["path"] == "input:approved:else"


def test_composite_branch_requires_immutable_boolean_argument(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        return "ok"

    @composite(
        storage,
        operation_id_from=lambda args, kwargs: args[0] if args else kwargs["operation_id"],
    )
    def decide(operation_id: str, enabled: bool) -> str:
        if not enabled:
            result = effect(idempotency_key="disabled")
        else:
            result = effect(idempotency_key="enabled")
        return result

    with pytest.raises(CompositeUnsupportedError, match="must be a bool"):
        decide(operation_id="job-1", enabled="yes")  # type: ignore[arg-type]
    assert decide("job-1", False) == "ok"

    with pytest.raises(CompositeUnsupportedError, match="cannot be reassigned"):

        @composite(storage)
        def mutable(operation_id: str, enabled: bool) -> str:
            enabled = not enabled
            if enabled:
                result = effect(idempotency_key="a")
            else:
                result = effect(idempotency_key="b")
            return result


def test_child_result_branch_persists_choice_and_replays(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []
    crash = {"enabled": True}

    @ledger_sync(storage=storage, transition_binding=_binding())
    def check(idempotency_key: str) -> bool:
        with side_effect():
            calls.append("check")
        return True

    @ledger_sync(storage=storage, transition_binding=_binding())
    def approve(idempotency_key: str) -> str:
        with side_effect():
            calls.append("approve")
        return "approved"

    @ledger_sync(storage=storage, transition_binding=_binding())
    def deny(idempotency_key: str) -> str:
        with side_effect():
            calls.append("deny")
        return "denied"

    @ledger_sync(storage=storage, transition_binding=_binding())
    def record(idempotency_key: str, result: str) -> str:
        with side_effect():
            calls.append("record")
        return result

    def crash_after_branch() -> None:
        if crash["enabled"]:
            raise RuntimeError("simulated crash")

    register_composite_helper(crash_after_branch)

    @composite(storage)
    def decide(operation_id: str) -> str:
        allowed = check(idempotency_key="check")
        if composite_choice(allowed):
            result = approve(idempotency_key="approve")
        else:
            result = deny(idempotency_key="deny")
        crash_after_branch()
        return record(idempotency_key="record", result=result)

    with pytest.raises(RuntimeError, match="simulated crash"):
        decide(operation_id="job-1")
    assert calls == ["check", "approve"]
    controls = json.loads((tmp_path / "ledger.sqlite.composites.json").read_text())
    assert controls["mycelium:job-1"]["selected_manifest"]["path"] == "result:allowed:then"

    store = _ControlStore(storage)
    key = "mycelium:job-1"
    contender = store.acquire(key, "replay-owner", 10)
    prefix_step = decide._mycelium_composite_manifest.steps[0]
    prefix_ids = (prefix_step.step_id,)
    binding = dict(store.load(key)["children"][prefix_step.step_id])
    binding.pop("outcome")
    store.admit(key, "replay-owner", contender["fence"], prefix_step, binding, 10)
    store.resolve_child(key, "replay-owner", contender["fence"], prefix_step)
    with pytest.raises(CompositeDefinitionDriftError, match="choice changed"):
        store.pin_result_path(
            key, "replay-owner", contender["fence"],
            decide._mycelium_composite_manifests[False], prefix_ids,
        )
    store.release(key, "replay-owner", contender["fence"])

    crash["enabled"] = False
    assert decide(operation_id="job-1") == "approved"
    assert calls == ["check", "approve", "record"]
    assert decide(operation_id="job-1") == "approved"
    assert calls == ["check", "approve", "record"]


def test_child_result_branch_rejects_non_boolean_and_unbound_selector(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def check(idempotency_key: str) -> str:
        with side_effect():
            calls.append("check")
        return "yes"

    @ledger_sync(storage=storage, transition_binding=_binding())
    def act(idempotency_key: str) -> str:
        with side_effect():
            calls.append("act")
        return "done"

    @composite(storage)
    def decide(operation_id: str) -> str:
        allowed = check(idempotency_key="check")
        if composite_choice(allowed):
            result = act(idempotency_key="yes")
        else:
            result = act(idempotency_key="no")
        return result

    with pytest.raises(CompositeUnsupportedError, match="bool child result"):
        decide(operation_id="job-1")
    assert calls == ["check"]

    with pytest.raises(CompositeUnsupportedError, match="preceding child result"):

        @composite(storage)
        def unbound(operation_id: str, allowed: bool) -> str:
            if composite_choice(allowed):
                result = act(idempotency_key="yes")
            else:
                result = act(idempotency_key="no")
            return result


def test_child_result_choice_survives_crash_before_branch_effect(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []
    crash = {"enabled": True}

    @ledger_sync(storage=storage, transition_binding=_binding())
    def check(idempotency_key: str) -> bool:
        with side_effect():
            calls.append("check")
        return False

    @ledger_sync(storage=storage, transition_binding=_binding())
    def act(idempotency_key: str) -> str:
        with side_effect():
            calls.append(idempotency_key)
        return idempotency_key

    def pause() -> None:
        if crash["enabled"]:
            raise RuntimeError("after choice")

    register_composite_helper(pause)

    @composite(storage)
    def decide(operation_id: str) -> str:
        allowed = check(idempotency_key="check")
        if composite_choice(allowed):
            result = act(idempotency_key="approved")
        else:
            pause()
            result = act(idempotency_key="denied")
        return result

    with pytest.raises(RuntimeError, match="after choice"):
        decide(operation_id="job-1")
    assert calls == ["check"]
    controls = json.loads((tmp_path / "ledger.sqlite.composites.json").read_text())
    assert controls["mycelium:job-1"]["selected_manifest"]["path"] == "result:allowed:else"

    crash["enabled"] = False
    assert decide(operation_id="job-1") == "denied"
    assert calls == ["check", "denied"]


def test_result_branch_definitions_are_pinned_before_first_child(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def check(idempotency_key: str) -> bool:
        with side_effect():
            calls.append("check")
        return True

    @ledger_sync(storage=storage, transition_binding=_binding())
    def first(idempotency_key: str) -> str:
        with side_effect():
            calls.append("first")
        return "first"

    @ledger_sync(storage=storage, transition_binding=_binding())
    def second(idempotency_key: str) -> str:
        with side_effect():
            calls.append("second")
        return "second"

    @composite(storage, definition="pinned")
    def workflow(operation_id: str) -> str:
        allowed = check(idempotency_key="check")
        if composite_choice(allowed):
            result = first(idempotency_key="first")
        else:
            result = second(idempotency_key="second")
        return result

    _ControlStore(storage).create_or_load(
        "mycelium:job-1", workflow._mycelium_composite_manifest, "mycelium"
    )

    @composite(storage, definition="pinned")
    def workflow(operation_id: str) -> str:
        allowed = check(idempotency_key="check")
        if composite_choice(allowed):
            result = second(idempotency_key="changed")
        else:
            result = first(idempotency_key="changed")
        return result

    with pytest.raises(CompositeDefinitionDriftError, match="manifest drift"):
        workflow(operation_id="job-1")
    assert calls == []


def test_async_child_result_branch_uses_stored_boolean(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger(storage=storage, transition_binding=_binding())
    async def check(idempotency_key: str) -> bool:
        async with side_effect_async():
            calls.append("check")
        return False

    @ledger(storage=storage, transition_binding=_binding())
    async def act(idempotency_key: str) -> str:
        async with side_effect_async():
            calls.append(idempotency_key)
        return idempotency_key

    @composite(storage)
    async def decide(operation_id: str) -> str:
        allowed = await check(idempotency_key="check")
        if not composite_choice(allowed):
            result = await act(idempotency_key="denied")
        else:
            result = await act(idempotency_key="approved")
        return result

    assert asyncio.run(decide(operation_id="async-branch")) == "denied"
    assert asyncio.run(decide(operation_id="async-branch")) == "denied"
    assert calls == ["check", "denied"]


def test_composite_rejects_expression_control_flow_before_any_effect(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        with side_effect():
            calls.append(idempotency_key)
        return "effect-result"

    enabled = True
    with pytest.raises(CompositeUnsupportedError, match="conditional expressions"):

        @composite(storage)
        def conditional(operation_id: str) -> str:
            return effect(idempotency_key="conditional") if enabled else "skipped"

    with pytest.raises(CompositeUnsupportedError, match="short-circuit"):

        @composite(storage)
        def short_circuit(operation_id: str) -> str:
            return enabled and effect(idempotency_key="short-circuit")

    with pytest.raises(CompositeUnsupportedError, match="comprehensions"):

        @composite(storage)
        def comprehension(operation_id: str) -> list[str]:
            return [effect(idempotency_key="comprehension") for _ in (0,)]

    with pytest.raises(CompositeUnsupportedError, match="nested or embedded calls"):

        @composite(storage)
        def nested(operation_id: str) -> str:
            return str(effect(idempotency_key="nested"))

    with pytest.raises(CompositeUnsupportedError, match="early returns"):

        @composite(storage)
        def early_return(operation_id: str) -> str:
            return "skipped"
            effect(idempotency_key="early")

    assert calls == []


def test_composite_finish_rejects_admission_without_resolution(tmp_path) -> None:
    from mycelium.composite import CompositeInvocation

    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        return "ok"

    @composite(storage)
    def publish(operation_id: str) -> str:
        return effect(idempotency_key="guard")

    invocation = CompositeInvocation(
        storage,
        "admission-only",
        publish._mycelium_composite_manifest,
        namespace="mycelium",
        lease_ttl=10,
    )
    invocation.__enter__()
    binding = getattr(effect, "_mycelium_transition_binding")
    invocation.prepare_child("effect", (), {"idempotency_key": "guard"}, binding)
    with pytest.raises(CompositeDefinitionDriftError, match="without resolving"):
        invocation.__exit__(None, None, None)
    invocation.store.release(invocation.key, invocation.owner, invocation.fence)


def test_replays_all_completed_children_after_parent_completion_window(
    tmp_path, monkeypatch
) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def first(idempotency_key: str) -> str:
        with side_effect():
            calls.append("first")
        return "first"

    @ledger_sync(storage=storage, transition_binding=_binding())
    def second(idempotency_key: str) -> str:
        with side_effect():
            calls.append("second")
        return "second"

    @composite(storage)
    def publish(operation_id: str) -> str:
        first(idempotency_key="first")
        return second(idempotency_key="second")

    original_resolve = CompositeInvocation.resolve_child
    fail_once = True

    def fail_parent_finish_window(self, step_id: str) -> None:
        nonlocal fail_once
        original_resolve(self, step_id)
        if fail_once and step_id == self.manifest.steps[-1].step_id:
            fail_once = False
            raise RuntimeError("simulated parent completion crash")

    monkeypatch.setattr(CompositeInvocation, "resolve_child", fail_parent_finish_window)
    with pytest.raises(RuntimeError, match="parent completion crash"):
        publish(operation_id="parent-window")

    monkeypatch.setattr(CompositeInvocation, "resolve_child", original_resolve)
    assert publish(operation_id="parent-window") == "second"
    assert calls == ["first", "second"]


def test_composite_rejects_opaque_calls_instead_of_skipping_them(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    def uninstrumented_provider() -> str:
        return "external"

    with pytest.raises(CompositeUnsupportedError, match="unresolvable call"):

        @composite(storage)
        def unsupported(operation_id: str) -> str:
            return uninstrumented_provider()


def test_composite_definition_is_pinned(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        with side_effect():
            return "ok"

    @composite(storage)
    def publish(operation_id: str) -> str:
        return effect(idempotency_key="x")

    assert publish(operation_id="job-1") == "ok"

    # A new decorator definition cannot silently reuse the old invocation.
    @composite(storage, definition="changed-definition")
    def changed(operation_id: str) -> str:
        return effect(idempotency_key="x")

    with pytest.raises(CompositeDefinitionDriftError):
        changed(operation_id="job-1")


def test_file_backend_survives_subprocess_termination_and_restart(tmp_path) -> None:
    sdk_path = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "PYTHONPATH": str(sdk_path),
        "MYCELIUM_COMPOSITE_FIXTURE_DIR": str(tmp_path),
        "MYCELIUM_COMPOSITE_CRASH": "1",
    }
    first = subprocess.run(
        [sys.executable, "tests/fixtures/composite_worker.py"],
        cwd=sdk_path,
        env=env,
        check=False,
    )
    assert first.returncode == 17

    time.sleep(0.6)
    env["MYCELIUM_COMPOSITE_CRASH"] = "0"
    second = subprocess.run(
        [sys.executable, "tests/fixtures/composite_worker.py"],
        cwd=sdk_path,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )
    assert second.stdout.strip() == "push-1"
    assert json.loads((tmp_path / "provider-counts.json").read_text()) == {
        "A": 1,
        "B": 1,
        "C": 1,
    }


def test_provider_success_before_result_persistence_fails_closed(tmp_path) -> None:
    sdk_path = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "PYTHONPATH": str(sdk_path),
        "MYCELIUM_COMPOSITE_FIXTURE_DIR": str(tmp_path),
        "MYCELIUM_COMPOSITE_CRASH": "0",
        "MYCELIUM_COMPOSITE_CRASH_WINDOW": "1",
    }
    first = subprocess.run(
        [sys.executable, "tests/fixtures/composite_worker.py"],
        cwd=sdk_path,
        env=env,
        check=False,
    )
    assert first.returncode == 19

    time.sleep(0.6)
    env["MYCELIUM_COMPOSITE_CRASH_WINDOW"] = "0"
    second = subprocess.run(
        [sys.executable, "tests/fixtures/composite_worker.py"],
        cwd=sdk_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    assert second.returncode != 0
    assert json.loads((tmp_path / "provider-counts.json").read_text()) == {
        "A": 1,
        "B": 1,
    }


def test_same_process_invocations_do_not_share_parent_authority(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    started = threading.Event()
    release = threading.Event()

    @ledger_sync(storage=storage, transition_binding=_binding())
    def slow(idempotency_key: str) -> str:
        started.set()
        release.wait(timeout=2)
        with side_effect():
            return "done"

    @composite(storage, lease_ttl=2)
    def publish(operation_id: str) -> str:
        return slow(idempotency_key="slow-1")

    worker = threading.Thread(target=lambda: publish(operation_id="same"))
    worker.start()
    assert started.wait(timeout=2)
    with pytest.raises(CompositeBusyError, match="live worker"):
        publish(operation_id="same")
    release.set()
    worker.join(timeout=2)
    assert not worker.is_alive()


def test_parent_lease_renews_during_long_child(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger_sync(storage=storage, transition_binding=_binding())
    def slow(idempotency_key: str) -> str:
        time.sleep(0.16)
        with side_effect():
            return "done"

    @composite(storage, lease_ttl=0.05)
    def publish(operation_id: str) -> str:
        return slow(idempotency_key="slow-1")

    assert publish(operation_id="renewed") == "done"


def test_renewal_failure_blocks_the_next_child(tmp_path, monkeypatch) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    pause_started = threading.Event()
    renewal_failed = threading.Event()
    release_pause = threading.Event()
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def first(idempotency_key: str) -> str:
        with side_effect():
            calls.append("first")
        return "first"

    @ledger_sync(storage=storage, transition_binding=_binding())
    def second(idempotency_key: str) -> str:
        with side_effect():
            calls.append("second")
        return "second"

    def pause() -> None:
        pause_started.set()
        release_pause.wait(timeout=2)

    register_composite_helper(pause)
    original_renew = _ControlStore.renew
    renewals = 0

    def fail_after_initial_renew(self, key, owner, fence, lease_ttl):
        nonlocal renewals
        renewals += 1
        if renewals >= 2:
            renewal_failed.set()
            raise CompositeAuthorityError("controlled renewal failure")
        return original_renew(self, key, owner, fence, lease_ttl)

    monkeypatch.setattr(_ControlStore, "renew", fail_after_initial_renew)

    @composite(storage, lease_ttl=0.2, renewal_interval=0.01)
    def publish(operation_id: str) -> str:
        first(idempotency_key="first")
        pause()
        return second(idempotency_key="second")

    errors: list[BaseException] = []

    def run() -> None:
        try:
            publish(operation_id="renewal-failure")
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=run)
    worker.start()
    assert pause_started.wait(timeout=2)
    assert renewal_failed.wait(timeout=2)
    release_pause.set()
    worker.join(timeout=2)
    assert not worker.is_alive()
    assert any(isinstance(error, CompositeAuthorityError) for error in errors)
    assert calls == ["first"]


def test_stale_worker_cannot_admit_after_parent_reclaim(tmp_path, monkeypatch) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
    started = threading.Event()
    release_stale = threading.Event()
    calls: list[str] = []

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        with side_effect():
            calls.append("effect")
        return "ok"

    def pause_for_reclaim() -> None:
        if threading.current_thread().name == "stale-worker":
            started.set()
            release_stale.wait(timeout=2)

    register_composite_helper(pause_for_reclaim)
    original_start_renewal = CompositeInvocation._start_renewal

    def start_renewal_except_stale(invocation: CompositeInvocation) -> None:
        if threading.current_thread().name != "stale-worker":
            original_start_renewal(invocation)

    monkeypatch.setattr(CompositeInvocation, "_start_renewal", start_renewal_except_stale)

    @composite(storage, lease_ttl=0.05)
    def publish(operation_id: str) -> str:
        pause_for_reclaim()
        return effect(idempotency_key="reclaimed")

    errors: list[BaseException] = []

    def stale_worker() -> None:
        try:
            publish(operation_id="reclaim")
        except BaseException as exc:
            errors.append(exc)

    worker = threading.Thread(target=stale_worker, name="stale-worker")
    worker.start()
    try:
        assert started.wait(timeout=2)
        controls = json.loads((tmp_path / "ledger.sqlite.composites.json").read_text())
        lease_until = controls["mycelium:reclaim"]["lease_until"]
        time.sleep(max(0, lease_until - time.time()) + 0.02)
        assert publish(operation_id="reclaim") == "ok"
    finally:
        release_stale.set()
        worker.join(timeout=2)
    assert not worker.is_alive()
    assert any(isinstance(error, CompositeAuthorityError) for error in errors)
    assert calls == ["effect"]


def test_reclaimed_fence_rejects_stale_renew_and_finish(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger_sync(storage=storage, transition_binding=_binding())
    def effect(idempotency_key: str) -> str:
        return "ok"

    @composite(storage)
    def publish(operation_id: str) -> str:
        return effect(idempotency_key="fence")

    first = CompositeInvocation(
        storage,
        "stale-fence",
        publish._mycelium_composite_manifest,
        namespace="mycelium",
        lease_ttl=0.05,
    )
    time.sleep(0.06)
    second = CompositeInvocation(
        storage,
        "stale-fence",
        publish._mycelium_composite_manifest,
        namespace="mycelium",
        lease_ttl=1,
    )
    with pytest.raises(CompositeAuthorityError):
        first.renew()
    expected = tuple(step.step_id for step in publish._mycelium_composite_manifest.steps)
    with pytest.raises(CompositeAuthorityError):
        first.store.finish(first.key, first.owner, first.fence, expected, (), frozenset())
    second.store.release(second.key, second.owner, second.fence)


def test_child_result_must_be_faithfully_reconstructable(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger_sync(storage=storage, transition_binding=_binding())
    def tuple_result(idempotency_key: str) -> tuple[str, ...]:
        with side_effect():
            return ("not", "json-faithful")

    @composite(storage)
    def publish(operation_id: str) -> tuple[str, ...]:
        return tuple_result(idempotency_key="tuple-1")

    with pytest.raises(CompositeUnsupportedError, match="faithfully"):
        publish(operation_id="serialization")


def test_async_composite_uses_the_same_child_protocol(tmp_path) -> None:
    storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")

    @ledger(storage=storage, transition_binding=_binding())
    async def effect(idempotency_key: str) -> dict[str, str]:
        async with side_effect_async():
            return {"value": "ok"}

    @composite(storage)
    async def publish(operation_id: str) -> dict[str, str]:
        result = await effect(idempotency_key="async-1")
        return {"copied": result["value"]}

    assert asyncio.run(publish(operation_id="async")) == {"copied": "ok"}


def test_async_cancellation_releases_parent_without_leaking_progress(tmp_path) -> None:
    async def scenario() -> None:
        storage = SqliteLedgerStorage(tmp_path / "ledger.sqlite")
        started = asyncio.Event()
        release = asyncio.Event()

        @ledger(storage=storage, transition_binding=_binding())
        async def effect(idempotency_key: str) -> str:
            async with side_effect_async():
                return "ok"

        async def pause() -> None:
            started.set()
            await release.wait()

        register_composite_helper(pause)

        @composite(storage)
        async def publish(operation_id: str) -> str:
            await pause()
            return await effect(idempotency_key="cancelled")

        task = asyncio.create_task(publish(operation_id="cancelled"))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        release.set()
        assert await publish(operation_id="cancelled") == "ok"

    asyncio.run(scenario())

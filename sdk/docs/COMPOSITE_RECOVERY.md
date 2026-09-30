# Durable composite recovery

Mycelium supports a bounded composite protocol for unchanged, sequential
Python functions whose consequential calls already pass through Mycelium's
`ledger`/`ledger_sync` wrappers. A non-effect helper may be explicitly marked
with `register_composite_helper`; an unresolved call is otherwise a setup
error, never an ignored manifest entry.

```python
from mycelium import composite

@composite(
    storage=storage,
    operation_id_from=lambda _args, kwargs: kwargs["job_id"],
)
def publish_change(job_id: str):
    commit = create_commit(idempotency_key=f"{job_id}:commit")
    pushed = push_branch(idempotency_key=f"{job_id}:push")
    return update_tracking_record(
        idempotency_key=f"{job_id}:tracking", pushed=pushed
    )
```

The child wrappers remain ordinary ledger transitions. The outer decorator
builds a manifest before execution, then stores it with a digest in a durable
composite-control record. That record owns the logical operation ID,
definition, lease, monotonically increasing parent fence, lifecycle, and child
bindings. The child identity extends the existing transition preimage with
the composite namespace, pinned definition, and a semantic call-site step ID
that is independent of the checkout or deployment path; it does not replace
scope, dispatch, tool, arguments, destination,
side-effect, agent, policy, or identity-schema fields.

On replay the unchanged function runs from the beginning. A completed child
returns its durably serialized and faithfully reconstructable result, while an unattempted child is admitted
under the current parent fence and executes through its existing ledger
protocol. Ambiguous children still reconcile or hard-block. Parent authority
is checked at admission and immediately before the child body and
`mark_maybe_crossed()` boundary. This protects progression but cannot cancel
an external request already sent during a check-to-send race.

The Python decorator supports straight-line assignments, expression
statements, and one final return, with each supported call at a statement
boundary. It also supports one top-level `if`/`else` whose condition is a
boolean function argument (`if enabled:` or `if not enabled:`). The argument
must be an actual `bool` and cannot be reassigned. Both possible paths are
checked at decoration time; the chosen path is pinned in the durable manifest
before the first child effect. A retry with a different choice is rejected.
Each path must contain at least one supported child boundary. For example:

```python
@composite(storage)
def publish_or_hold(operation_id: str, publish: bool):
    if publish:
        decision = publish_change(idempotency_key=f"{operation_id}:publish")
    else:
        decision = record_hold(idempotency_key=f"{operation_id}:hold")
    return record_decision(idempotency_key=f"{operation_id}:record", decision=decision)
```

The branch may instead depend on a `bool` returned by the immediately
preceding consequential child. Wrap that value with `composite_choice()` in
the `if` condition:

```python
from mycelium import composite, composite_choice

@composite(storage)
def publish_after_check(operation_id: str):
    allowed = check_release(idempotency_key=f"{operation_id}:check")
    if composite_choice(allowed):
        result = publish_change(idempotency_key=f"{operation_id}:publish")
    else:
        result = record_hold(idempotency_key=f"{operation_id}:hold")
    return result
```

The check is an ordinary ledgered child with a durable, faithfully replayed
boolean result. Both possible path definitions are pinned before the check
runs, including when the host supplies an explicit workflow version. Mycelium
stores the selected path under the parent fence
after that child resolves and before any branch child executes. A crash after
selection replays the check's stored result, verifies the same path, and
continues with stored child results. A changed choice blocks. The check must
be assigned immediately before the `if`; direct reads and mutable external
facts cannot determine a replayable branch.

Multiple or nested conditions, short-circuit or conditional expressions,
loops, comprehensions, generators, nested calls such as `outer(inner())`,
early returns, nested definitions, recursion, dynamic dispatch, nested
composites, and unsupported/opaque boundaries are rejected before execution.
This avoids inferring order by sorting every AST call by source location.
Static analysis is preflight assistance, not proof that arbitrary hidden
effects were found. Deterministic local computation may rerun, but time,
randomness, mutable globals, fresh external reads, and other nondeterministic
inputs must not change supported calls or their arguments.

Parent completion requires the current replay to observe every manifest step in
order and to record each child as resolved under the current parent fence.
Admission alone is not completion evidence. A completed child can still be
replayed through its wrapper after a crash before parent completion.

The parent lease renews automatically while the body is running and renewal
failure blocks the next boundary or completion. Use a stable host-owned
operation ID. A new operation ID means a genuinely new invocation; a new
worker, retry attempt, or parent fence does not. The durable
record pins an invocation to its original manifest and definition. Changing
order, adding/removing steps, changing bindings, arguments, or destinations
blocks recovery rather than minting fresh child identities.

SQLite, file, Redis, PostgreSQL, and in-process storage provide the
composite-control capability. Redis and PostgreSQL persist parent records in
their respective atomic state stores with revision-checked updates. Workers
must use the same Redis key prefix or PostgreSQL ledger table and database to
share parent authority. In-memory storage is useful for unit tests only. The
Python decorator remains the only API that statically inspects a function
body. The sidecar also offers an experimental `composite-v1` extension for
explicitly declared straight-line manifests on file or shared PostgreSQL
storage.

## Language-neutral sidecar extension

The extension is advertised in `GET /v1/capabilities` under
`extensions.composite-v1`; it does not change the frozen `v1alpha1` effect
routes. Claim a parent at `POST /extensions/composite-v1/composites/claim` with
`operation_id`, a stable `definition`, and an ordered list of unique
`{step_id, tool_id}` pairs. The sidecar pins the manifest and issues a parent
owner and fence. A changed definition, step order, or tool blocks replay.

For each step, call `.../steps/{step_id}/claim` with the parent owner/fence and
the complete child identity and decision. Only `EXECUTE` permits the provider
call. Before the provider boundary, call `.../boundary`; after success, call
`.../complete`. These commands validate both the parent fence and the child
effect fence. The sidecar derives each child effect identity from its ordinary
identity fields plus the parent operation, definition, and step. Reusing the
same tool input in another parent creates a distinct child effect.

On resume, claim the same parent manifest. A completed child returns
`RETURN_STORED_RESULT`; call `.../resolve` to acknowledge its evidence in the
new replay, then continue with the next step. Finish the parent only after all
children are committed and resolved. `.../renew` extends a long-running parent
lease; `.../release` relinquishes it after a controlled interruption. An
unexpected worker death leaves the lease for a later fenced reclaim. `UNKNOWN`
and unresolved children remain blocked.

The host must declare the full sequence before claiming and call only those
steps in order. The sidecar cannot inspect TypeScript or Go program control
flow and does not make arbitrary workflows transactional. Its explicit
manifests use a separate namespace and cannot resume a Python `@composite`
decorator invocation. A parent check just before a provider call cannot
cancel an external request already sent.

## Decision record

The implementation chooses a lightweight durable parent-control record plus
ordinary child `LedgerEntry` rows. `handoff_scope()` remains audit causation
only. The parent is not an atomic transaction and never aggregates away child
ambiguity. General workflow scheduling and arbitrary conditions based on
prior child outcomes remain deferred.

"""`run_id` derivation — contracts §Submit, pinned by golden values.

Any second implementation (a replay tool, another consumer) must reproduce
these exact ids, or its deliveries could not re-enter a claim ours left.
"""

from uuid import UUID

from src.models.submit import RUN_ID_NAMESPACE, derive_run_id

AUTOMATION_ID = "3f2b8c1e-4d6a-4b2f-9e77-0a1b2c3d4e5f"


def test_namespace_is_the_contract_literal():
    assert RUN_ID_NAMESPACE == UUID("4c6f4bd9-3da3-4046-9b16-75097fd337aa")


def test_golden_run_id():
    assert (
        derive_run_id(AUTOMATION_ID, "evt-789")
        == "fca67579-0aa4-5464-9b3b-3be62ccf1c00"
    )


def test_history_id_is_utf8_and_may_contain_the_separator():
    assert (
        derive_run_id(AUTOMATION_ID, "évt:ü/1")
        == "3d4b9c2a-a8a0-561c-b84b-cd659efcb245"
    )


def test_one_event_one_run_id_and_distinct_events_differ():
    run_id = derive_run_id(AUTOMATION_ID, "evt-789")

    assert derive_run_id(AUTOMATION_ID, "evt-789") == run_id
    assert derive_run_id(AUTOMATION_ID, "evt-790") != run_id
    assert (
        derive_run_id("0b8f6a6e-2c1d-4b8e-9f3a-5d6c7e8f9a0b", "evt-789") != run_id
    )
    assert UUID(run_id).version == 5

from __future__ import annotations

import pytest

from meeting_action_orchestrator.api import openapi


def test_problem_responses_inline_references_without_shared_components() -> None:
    responses = openapi.problem_responses((404, 503))

    assert responses[404]["description"] == "Not Found"
    assert responses[503]["description"] == "Service Unavailable"
    for response in responses.values():
        schema = response["content"]["application/problem+json"]["schema"]
        assert "$defs" not in schema
        assert "$ref" not in str(schema)


def test_inline_schema_resolves_nested_lists_and_reference_siblings() -> None:
    schema = {
        "$defs": {"Item": {"type": "object", "required": ["value"]}},
        "allOf": [[{"$ref": "#/$defs/Item", "description": "safe"}]],
    }

    result = openapi._inline_schema(schema)

    assert result == {"allOf": [[{"type": "object", "required": ["value"], "description": "safe"}]]}


@pytest.mark.parametrize(
    "schema",
    [
        {"$defs": []},
        {"$defs": {"Root": "invalid"}, "$ref": "#/$defs/Root"},
        {"$defs": {}, "$ref": "#/$defs/Missing"},
    ],
)
def test_inline_schema_rejects_malformed_definition_graphs(schema: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="schema"):
        openapi._inline_schema(schema)

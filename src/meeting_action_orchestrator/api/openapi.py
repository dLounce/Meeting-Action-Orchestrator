from __future__ import annotations

from collections.abc import Iterable, Mapping
from http import HTTPStatus
from typing import Any

from meeting_action_orchestrator.api.problems import ProblemDetail


def problem_responses(statuses: Iterable[int]) -> dict[int | str, dict[str, Any]]:
    schema = _inline_schema(ProblemDetail.model_json_schema(by_alias=True))
    return {
        status: {
            "description": HTTPStatus(status).phrase,
            "content": {"application/problem+json": {"schema": schema}},
        }
        for status in statuses
    }


def _inline_schema(schema: dict[str, Any]) -> dict[str, Any]:
    definitions = schema.pop("$defs", {})
    if not isinstance(definitions, Mapping):
        raise ValueError("Model schema definitions are invalid")
    resolved = _resolve_schema(schema, definitions)
    if not isinstance(resolved, dict):
        raise ValueError("Model schema is invalid")
    return resolved


def _resolve_schema(value: Any, definitions: Mapping[str, Any]) -> Any:
    if isinstance(value, list):
        return [_resolve_schema(item, definitions) for item in value]
    if not isinstance(value, dict):
        return value
    reference = value.get("$ref")
    if isinstance(reference, str) and reference.startswith("#/$defs/"):
        name = reference.removeprefix("#/$defs/")
        target = definitions.get(name)
        if not isinstance(target, dict):
            raise ValueError("Model schema reference is invalid")
        siblings = {key: item for key, item in value.items() if key != "$ref"}
        return _resolve_schema(target | siblings, definitions)
    return {key: _resolve_schema(item, definitions) for key, item in value.items()}

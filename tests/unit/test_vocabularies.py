"""Vocabulary constants of the embedded runtime match the enumerations of the profile schema."""

import pytest

from tabledossier.deep import DEEP_OPERATION_KINDS
from tabledossier.keys import KEY_ORIGINS, KEY_OUTCOMES
from tabledossier.metrics import METRIC_ACCURACIES, METRIC_SCOPES, METRIC_SOURCES, METRIC_STATUSES
from tabledossier.paths import SEGMENT_KINDS


def _enum(schemas, *path):
    node = schemas["profile"]["$defs"]
    for key in path:
        node = node[key]
    return [value for value in node["enum"] if value is not None]


@pytest.mark.parametrize(
    ("constant", "path"),
    [
        (METRIC_SCOPES, ("metric", "properties", "scope")),
        (METRIC_ACCURACIES, ("metric", "properties", "accuracy")),
        (METRIC_SOURCES, ("metric", "properties", "source")),
        (METRIC_STATUSES, ("metric", "properties", "status")),
        (SEGMENT_KINDS, ("segment", "properties", "kind")),
        (KEY_ORIGINS, ("uniquenessKey", "properties", "origins", "items")),
        (KEY_OUTCOMES, ("uniquenessKey", "properties", "outcome")),
    ],
)
def test_constants_list_exactly_the_schema_values(schemas, constant, path):
    assert sorted(constant) == sorted(_enum(schemas, *path))


def test_deep_operation_kinds_are_operation_kinds(schemas):
    kinds = _enum(schemas, "operations", "properties", "planned", "items", "properties", "kind")
    assert set(DEEP_OPERATION_KINDS) <= set(kinds)

# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import pytest
from jsonschema import ValidationError, validate

from digitalhub.factory.entity import entity_factory

ROOT = Path("test/instances")
ENTITIES_PATH = ROOT / "entities"
SCHEMAS_PATH = ROOT / "schemas"


class SchemaRegistry:
    """Registry for JSON schemas with lazy loading."""

    def __init__(self, schemas_path: Path):
        self._schemas_path = schemas_path
        self._schema_paths = {path.stem: path for path in schemas_path.rglob("*.json")}

    @lru_cache(maxsize=None)
    def get_schema(self, kind: str) -> dict[str, Any]:
        """Load and cache schema by kind."""
        if kind not in self._schema_paths:
            raise ValueError(f"Schema not found for kind: {kind}")
        return json.loads(self._schema_paths[kind].read_text())

    @property
    def available_kinds(self) -> set[str]:
        """Return all available schema kinds."""
        return set(self._schema_paths.keys())


class EntityValidator:
    """Validator for entity JSON files."""

    def __init__(self, schema_registry: SchemaRegistry):
        self.schema_registry = schema_registry

    def load_entity(self, path: Path) -> dict[str, Any]:
        """Load entity from JSON file."""
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {path}: {e}") from e

    def build_from_file(self, path: Path) -> tuple[dict[str, Any], str]:
        """Build entity object from JSON file using factory."""
        entity = self.load_entity(path)

        if "kind" not in entity:
            raise ValueError(f"Missing 'kind' field in {path}")
        if "spec" not in entity:
            raise ValueError(f"Missing 'spec' field in {path}")

        kind = entity["kind"]
        built = entity_factory.build_spec(kind, **entity["spec"])
        return built.to_dict(), kind

    def validate(self, built: dict[str, Any], kind: str) -> None:
        """Validate built object against its schema."""
        schema = self.schema_registry.get_schema(kind)
        try:
            validate(instance=built, schema=schema)
        except ValidationError as e:
            raise AssertionError(f"Validation failed for kind '{kind}': {e.message}") from e

    def check_schema_completeness(self, built: dict[str, Any], kind: str) -> list[str]:
        """Check if rebuilt object includes all schema properties."""
        schema = self.schema_registry.get_schema(kind)
        missing_fields = []

        if "properties" in schema:
            schema_fields = set(schema["properties"].keys())
            built_fields = set(built.keys())
            missing_fields = sorted(schema_fields - built_fields)

        return missing_fields


# Discover entity files with collision-resistant naming
def discover_entities(entities_path: Path) -> dict[str, Path]:
    """Discover all entity JSON files, handling name collisions gracefully."""
    entities = {}
    for path in entities_path.rglob("*.json"):
        # Use relative path from entities_path for uniqueness
        key = str(path.relative_to(entities_path))
        entities[key] = path
    return entities


# Initialize components
schema_registry = SchemaRegistry(SCHEMAS_PATH)
validator = EntityValidator(schema_registry)
entity_paths = discover_entities(ENTITIES_PATH)


class TestValidate:
    """Test entity JSON files build correctly and validate against schemas."""

    @pytest.mark.parametrize("entity_file", sorted(entity_paths.keys()))
    def test_entity_validation(self, entity_file: str):
        """Test that entity builds from JSON and validates against its schema."""
        path = entity_paths[entity_file]
        built, kind = validator.build_from_file(path)
        validator.validate(built, kind)

    @pytest.mark.parametrize("entity_file", sorted(entity_paths.keys()))
    def test_schema_field_completeness(self, entity_file: str):
        """Test that rebuilt object includes all fields defined in schema (required and optional)."""
        path = entity_paths[entity_file]
        built, kind = validator.build_from_file(path)

        missing = validator.check_schema_completeness(built, kind)
        if missing:
            schema = validator.schema_registry.get_schema(kind)
            required = schema.get("required", [])
            missing_required = [f for f in missing if f in required]
            missing_optional = [f for f in missing if f not in required]

            msg_parts = []
            if missing_required:
                msg_parts.append(f"Missing REQUIRED fields: {missing_required}")
            if missing_optional:
                msg_parts.append(f"Missing optional fields: {missing_optional}")

            pytest.fail(f"Schema completeness check failed for '{kind}': {'; '.join(msg_parts)}")

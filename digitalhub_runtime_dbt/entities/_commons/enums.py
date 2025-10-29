# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from enum import Enum


class EntityKinds(Enum):
    """
    Entity kinds.
    """

    FUNCTION_DBT = "dbt"
    TASK_DBT_TRANSFORM = "dbt+transform"
    RUN_DBT_TRANSFORM = "dbt+transform:run"


class Actions(Enum):
    """
    Task actions.
    """

    TRANSFORM = "transform"

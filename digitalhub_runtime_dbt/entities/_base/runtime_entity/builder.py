# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from digitalhub.entities._base.runtime_entity.builder import RuntimeEntityBuilder
from digitalhub.entities._commons.utils import map_actions

from digitalhub_runtime_dbt.entities._commons.enums import Actions, EntityKinds


class RuntimeEntityBuilderDbt(RuntimeEntityBuilder):
    EXECUTABLE_KIND = EntityKinds.FUNCTION_DBT.value
    TASKS_KINDS = map_actions(
        [
            (
                EntityKinds.TASK_DBT_TRANSFORM.value,
                Actions.TRANSFORM.value,
            )
        ]
    )
    RUN_KINDS = map_actions(
        [
            (
                EntityKinds.RUN_DBT_TRANSFORM.value,
                Actions.TRANSFORM.value,
            )
        ]
    )

# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0
from digitalhub_runtime_dbt.entities._commons.enums import EntityKinds
from digitalhub_runtime_dbt.entities.function.dbt.builder import FunctionDbtBuilder
from digitalhub_runtime_dbt.entities.run.transform.builder import RunDbtRunBuilder
from digitalhub_runtime_dbt.entities.task.transform.builder import TaskDbtTransformBuilder

entity_builders = (
    (EntityKinds.FUNCTION_DBT.value, FunctionDbtBuilder),
    (EntityKinds.TASK_DBT_TRANSFORM.value, TaskDbtTransformBuilder),
    (EntityKinds.RUN_DBT_TRANSFORM.value, RunDbtRunBuilder),
)

try:
    from digitalhub_runtime_dbt.runtimes.builder import RuntimeDbtBuilder

    runtime_builders = ((kind, RuntimeDbtBuilder) for kind in [e.value for e in EntityKinds])
except ImportError:
    runtime_builders = tuple()

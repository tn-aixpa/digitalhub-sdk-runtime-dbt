# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing
from typing import Callable

from digitalhub.context.api import get_context
from digitalhub.entities._constructors.uuid import build_uuid
from digitalhub.factory.entity import entity_factory
from digitalhub.runtimes._base import Runtime
from digitalhub.utils.logger import LOGGER

from digitalhub_runtime_dbt.entities._commons.enums import Actions
from digitalhub_runtime_dbt.utils.configuration import (
    CredsConfigurator,
    cleanup,
    generate_dbt_profile_yml,
    generate_dbt_project_yml,
    generate_inputs_conf,
    generate_outputs_conf,
    get_output_table_name,
    save_function_source,
)
from digitalhub_runtime_dbt.utils.functions import transform
from digitalhub_runtime_dbt.utils.inputs import materialize_dataitem
from digitalhub_runtime_dbt.utils.outputs import build_status, create_dataitem_, parse_results

if typing.TYPE_CHECKING:
    from dbt.contracts.results import RunResult
    from digitalhub.entities.dataitem._base.entity import Dataitem


class RuntimeDbt(Runtime):
    """
    Runtime Dbt class.
    """

    def __init__(self, project: str) -> None:
        super().__init__(project)

        ctx = get_context(self.project)
        self.runtime_dir = ctx.root / "runtime_dbt"
        self.tmp_dir = ctx.root / "tmp"

        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        self.tmp_dir.mkdir(parents=True, exist_ok=True)

        # UUID for output dataitem and dbt execution
        self.uuid = build_uuid()

        # Registries
        self._input_dataitems: list[dict[str, str]] = []
        self._versioned_tables: list[str] = []

    def build(self, function: dict, task: dict, run: dict) -> dict:
        """
        Build run spec.

        Parameters
        ----------
        function : dict
            The function.
        task : dict
            The task.
        run : dict
            The run.

        Returns
        -------
        dict
            The run spec.
        """
        return {
            **function.get("spec", {}),
            **task.get("spec", {}),
            **run.get("spec", {}),
        }

    def run(self, run: dict) -> dict:
        """
        Run function.

        Parameters
        ----------
        run : dict
            The run.

        Returns
        -------
        dict
            Status of the executed run.
        """
        LOGGER.info("Validating task.")
        action = self._validate_task(run)
        executable = self._get_executable(action)

        LOGGER.info("Starting task.")
        spec = run.get("spec")
        project = run.get("project")
        run_key = run.get("key")

        LOGGER.info("Initializing environment.")
        configurator = CredsConfigurator()
        _ = configurator.get_creds()

        LOGGER.info("Collecting inputs.")
        self._collect_inputs(spec, configurator)

        LOGGER.info("Configure execution.")
        output_table = self._configure_execution(spec, project, configurator)

        LOGGER.info("Executing run.")
        results = self._execute(executable, output_table, self.runtime_dir)

        LOGGER.info("Collecting outputs.")
        output = self._collect_outputs(results, output_table, project, run_key, configurator)
        status = build_status(output, results, output_table)

        LOGGER.info("Clean up environment.")
        self._cleanup(configurator)

        LOGGER.info("Task completed, returning run status.")
        return status

    @staticmethod
    def _get_executable(action: str) -> Callable:
        """
        Select function according to action.

        Parameters
        ----------
        action : str
            Action to execute.

        Returns
        -------
        Callable
            Function to execute.
        """
        if action == Actions.TRANSFORM.value:
            return transform
        raise NotImplementedError

    ##############################
    # Inputs
    ##############################

    def _collect_inputs(self, spec: dict, configurator: CredsConfigurator) -> None:
        """
        Parse inputs from run spec and materialize dataitems in postgres.

        Parameters
        ----------
        spec : dict
            Run spec dict.
        configurator : CredsConfigurator
            Creds configurator.
        """
        # Collect input dataitems
        for param, di in spec.get("inputs", {}).items():
            di = entity_factory.build_entity_from_dict(di)

            # Register dataitem in a dict to be used for inputs confs generation
            self._input_dataitems.append({"name": param, "id": di.id})

            # Materialize dataitem in postgres
            table = materialize_dataitem(di, param, configurator)

            # Save versioned table name to be used for cleanup
            self._versioned_tables.append(table)

    ##############################
    # Configuration
    ##############################

    def _configure_execution(
        self,
        spec: dict,
        project: str,
        configurator: CredsConfigurator,
    ) -> str:
        """
        Initialize a dbt project with a model and a schema definition.

        Parameters
        ----------
        spec : str
            Run spec dict.
        project : str
            The project name.
        configurator : CredsConfigurator
            Creds configurator.

        Returns
        -------
        str
            Output table name.
        """
        model_dir = self.runtime_dir / "models"
        model_dir.mkdir(parents=True, exist_ok=True)

        output_table = get_output_table_name(spec.get("outputs", []))
        query = save_function_source(self.tmp_dir, spec.get("source", {}))

        # Generate profile yaml file
        generate_dbt_profile_yml(self.runtime_dir, configurator)

        # Generate project yaml file
        generate_dbt_project_yml(self.runtime_dir, model_dir, project.replace("-", "_"))

        # Generate outputs confs
        generate_outputs_conf(model_dir, query, output_table, self.uuid)

        # Generate inputs confs for every dataitem
        for di in self._input_dataitems:
            generate_inputs_conf(model_dir, di["name"], di["id"])

        return output_table

    ##############################
    # Outputs
    ##############################

    def _collect_outputs(
        self,
        results: RunResult,
        output_table: str,
        project: str,
        run_key: str,
        configurator: CredsConfigurator,
    ) -> Dataitem:
        """
        Collect outputs.

        Parameters
        ----------
        results : dict
            The dbt run results.
        output_table : str
            Output table name.
        project : str
            The project name.
        run_key : str
            The run key.
        configurator : CredsConfigurator
            Creds configurator.

        Returns
        -------
        Dataitem
            The output dataitem table.
        """
        parsed_result = parse_results(results, output_table, project)
        return create_dataitem_(parsed_result, project, self.uuid, run_key, configurator)

    ##############################
    # Cleanup
    ##############################

    def _cleanup(self, configurator: CredsConfigurator) -> None:
        """
        Cleanup environment.

        Parameters
        ----------
        configurator : CredsConfigurator
            Creds configurator.
        """
        cleanup(
            self._versioned_tables,
            self.tmp_dir,
            configurator,
        )

# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing

from digitalhub.entities._commons.enums import Relationship
from digitalhub.entities._commons.utils import get_entity_type_from_key
from digitalhub.entities.run._base.entity import Run

from digitalhub_runtime_dbt.entities.run.transform.utils import get_getter_for_material

if typing.TYPE_CHECKING:
    from digitalhub.entities._base.entity.metadata import Metadata
    from digitalhub.entities._base.material.entity import MaterialEntity

    from digitalhub_runtime_dbt.entities.run.transform.spec import RunSpecDbtRun
    from digitalhub_runtime_dbt.entities.run.transform.status import RunStatusDbtRun


class RunDbtRun(Run):
    """
    RunDbtRun class.
    """

    def __init__(
        self,
        project: str,
        uuid: str,
        kind: str,
        metadata: Metadata,
        spec: RunSpecDbtRun,
        status: RunStatusDbtRun,
        user: str | None = None,
    ) -> None:
        super().__init__(project, uuid, kind, metadata, spec, status, user)

        self.spec: RunSpecDbtRun
        self.status: RunStatusDbtRun

    def _setup_execution(self) -> None:
        """
        Setup run execution.
        """
        self.refresh()
        inputs = self.inputs(as_dict=True)
        if self.spec.local_execution:
            for _, v in inputs.items():
                self.add_relationship(
                    relation=Relationship.CONSUMES.value,
                    dest=v.get("key"),
                )
        self.save(update=True)
        self.spec.inputs = inputs

    def inputs(self, as_dict: bool = False) -> dict:
        """
        Get inputs passed in spec as objects or as dictionaries.

        Parameters
        ----------
        as_dict : bool
            If True, return inputs as dictionaries.

        Returns
        -------
        dict
            Inputs.
        """
        inputs = {}
        if self.spec.inputs is None:
            return inputs

        for parameter, key in self.spec.inputs.items():
            entity_type = get_entity_type_from_key(key)
            entity = get_getter_for_material(entity_type)(key)
            if as_dict:
                entity = entity.to_dict()
            inputs[parameter] = entity

        return inputs

    def output(
        self,
        output_name: str,
        as_key: bool = False,
        as_dict: bool = False,
    ) -> MaterialEntity | dict | str | None:
        """
        Get run's output by name.

        Parameters
        ----------
        output_name : str
            Key of the result.
        as_key : bool
            If True, return result as key.
        as_dict : bool
            If True, return result as dictionary.

        Returns
        -------
        Entity | dict | str | None
            Result.
        """
        return self.outputs(as_key=as_key, as_dict=as_dict).get(output_name)

    def outputs(
        self,
        as_key: bool = False,
        as_dict: bool = False,
    ) -> MaterialEntity | dict | str | None:
        """
        Get run's outputs.

        Parameters
        ----------
        as_key : bool
            If True, return results as keys.
        as_dict : bool
            If True, return results as dictionaries.

        Returns
        -------
        dict
            List of output objects.
        """
        outputs = {}
        if self.status.outputs is None:
            return outputs

        for parameter, key in self.status.outputs.items():
            entity_type = get_entity_type_from_key(key)
            entity = get_getter_for_material(entity_type)(key)
            if as_key:
                entity = entity.key
            if as_dict:
                entity = entity.to_dict()
            outputs[parameter] = entity

        return outputs

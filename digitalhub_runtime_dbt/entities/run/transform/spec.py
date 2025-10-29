# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from digitalhub.entities.run._base.spec import RunSpec, RunValidator


class RunSpecDbtRun(RunSpec):
    """RunSpecDbtRun specifications."""

    def __init__(
        self,
        task: str,
        local_execution: bool = False,
        function: str | None = None,
        workflow: str | None = None,
        node_selector: list[dict] | None = None,
        volumes: list[dict] | None = None,
        resources: dict | None = None,
        affinity: dict | None = None,
        tolerations: list[dict] | None = None,
        envs: list[dict] | None = None,
        secrets: list[str] | None = None,
        profile: str | None = None,
        runtime_class: str | None = None,
        priority_class: str | None = None,
        source: dict | None = None,
        inputs: dict | None = None,
        outputs: dict | None = None,
        parameters: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            task,
            local_execution,
            function,
            workflow,
            node_selector,
            volumes,
            resources,
            affinity,
            tolerations,
            envs,
            secrets,
            profile,
            runtime_class,
            priority_class,
            **kwargs,
        )
        self.source = source
        self.inputs = inputs
        self.outputs = outputs
        self.parameters = parameters


class RunValidatorDbtRun(RunValidator):
    """RunValidatorDbtRun validator."""

    # Function parameters
    source: dict = None

    # Run parameters
    inputs: dict = None
    """Run inputs."""

    outputs: dict = None
    """Run outputs."""

    parameters: dict = None
    """Run parameters."""

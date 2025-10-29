# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing
from dataclasses import dataclass

import psycopg2
from dbt.cli.main import dbtRunnerResult
from digitalhub.entities._commons.enums import EntityKinds, Relationship, State
from digitalhub.entities.dataitem.table.utils import check_preview_size, finalize_preview, prepare_data, prepare_preview
from digitalhub.factory.entity import entity_factory
from digitalhub.utils.logger import LOGGER
from psycopg2 import sql

from digitalhub_runtime_dbt.utils.configuration import get_connection

if typing.TYPE_CHECKING:
    from dbt.contracts.results import RunResult
    from digitalhub.entities.dataitem.table.entity import DataitemTable

    from digitalhub_runtime_dbt.utils.configuration import CredsConfigurator


# Postgres type mapper to frictionless types.
TYPE_MAPPER = {
    16: "boolean",  # bool
    18: "string",  # char
    20: "integer",  # int8
    21: "integer",  # int2
    23: "integer",  # int4
    25: "string",  # text
    114: "object",  # json
    142: "str",  # xml
    650: "str",  # cidr
    700: "number",  # float4
    701: "number",  # float8
    774: "str",  # macaddr8
    829: "str",  # macaddr
    869: "str",  # inet
    1043: "string",  # varchar
    1082: "date",  # date
    1083: "time",  # time
    1114: "datetime",  # timestamp
    1184: "datetime",  # timestamptz
    1266: "time",  # timetz
    1700: "number",  # numeric
    2950: "str",  # uuid
}


@dataclass
class ParsedResults:
    """
    Parsed results class.
    """

    name: str
    path: str


def parse_results(run_result: dbtRunnerResult, output: str, project: str) -> ParsedResults:
    """
    Parse dbt results.

    Parameters
    ----------
    run_result : dbtRunnerResult
        The dbt result.
    output : str
        The output table name.
    project : str
        The project name.

    Returns
    -------
    ParsedResults
        Parsed results.
    """
    result: RunResult = validate_results(run_result, output, project)
    try:
        path = get_path(result)
        name = result.node.name
        return ParsedResults(name, path)
    except Exception as e:
        msg = f"Something got wrong during results parsing. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e


def validate_results(run_result: dbtRunnerResult, output: str, project: str) -> RunResult:
    """
    Parse dbt results.

    Parameters
    ----------
    run_result : dbtRunnerResult
        The dbt result.
    output : str
        The output table name.
    project : str
        The project name.

    Returns
    -------
    RunResult
        Run result.

    Raises
    ------
    RuntimeError
        If something got wrong during function execution.
    """
    try:
        # Take last result, final result of the query
        result: RunResult = run_result.result[-1]
    except IndexError as e:
        msg = f"No results found. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.error(msg)
        raise RuntimeError(msg)

    if not result.status.value == "success":
        msg = f"Function execution failed: {result.status.value}."
        LOGGER.error(msg)
        raise RuntimeError(msg)

    if not result.node.package_name == project.replace("-", "_"):
        msg = f"Wrong project name. Got {result.node.package_name}, expected {project.replace('-', '_')}."
        LOGGER.error(msg)
        raise RuntimeError(msg)

    if not result.node.name == output:
        msg = f"Wrong output name. Got {result.node.name}, expected {output}."
        LOGGER.error(msg)
        raise RuntimeError(msg)

    return result


def get_path(result: RunResult) -> str:
    """
    Get path from dbt result (sql://database/schema/table).

    Parameters
    ----------
    result : RunResult
        The dbt result.

    Returns
    -------
    str
        SQL path.
    """
    try:
        components = result.node.relation_name.replace('"', "")
        components = "/".join(components.split("."))
        return f"sql://{components}"
    except Exception as e:
        msg = f"Something got wrong during path parsing. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e


def create_dataitem_(
    result: ParsedResults,
    project: str,
    uuid: str,
    run_key: str,
    configurator: CredsConfigurator,
) -> DataitemTable:
    """
    Create new dataitem.

    Parameters
    ----------
    result : ParsedResults
        The parsed results.
    project : str
        The project name.
    uuid : str
        The uuid of the model for outputs versioning.
    run_key : str
        The run key.
    configurator : CredsConfigurator
        Creds configurator.

    Returns
    -------
    list[dict]
        The output dataitem infos.

    Raises
    ------
    RuntimeError
        If something got wrong during dataitem creation.
    """
    try:
        # Get columns and data sample from dbt results
        conn = get_connection(configurator)
        columns, data, rows_count = get_data_sample(result.name, uuid, conn)

        # Prepare dataitem kwargs
        kwargs = {}
        kwargs["project"] = project
        kwargs["name"] = result.name
        kwargs["kind"] = EntityKinds.DATAITEM_TABLE.value
        kwargs["path"] = result.path
        kwargs["uuid"] = uuid
        kwargs["schema"] = get_schema(columns)

        # Create dataitem
        dataitem = entity_factory.build_entity_from_params(**kwargs)

        # Update dataitem relationships with run key
        dest = run_key + ":" + run_key.split("/")[-1]
        dataitem.add_relationship(relation=Relationship.PRODUCEDBY.value, dest=dest)

        # Update dataitem status with preview
        dataitem.status.preview = _get_data_preview(columns, data, rows_count)

        # Save dataitem in core and return it
        dataitem.save()
        return dataitem

    except Exception as e:
        msg = f"Something got wrong during dataitem creation. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e


def get_data_sample(
    table_name: str,
    uuid: str,
    connection: psycopg2.extensions.connection,
) -> None:
    """
    Get columns and data sample from dbt results.

    Parameters
    ----------
    table_name : str
        The output table name.
    uuid : str
        The uuid of the model for outputs versioning.
    conn : connection
        The connection to postgres.
    """
    LOGGER.info("Getting columns and data sample from dbt results.")
    try:
        query_sample = sql.SQL("SELECT * FROM {table} LIMIT 10;").format(table=sql.Identifier(f"{table_name}_v{uuid}"))
        query_count = sql.SQL("SELECT count(*) FROM {table};").format(table=sql.Identifier(f"{table_name}_v{uuid}"))
        with connection:
            with connection.cursor() as cursor:
                cursor.execute(query_sample)
                columns = cursor.description
                data = cursor.fetchall()
                cursor.execute(query_count)
                rows_count = cursor.fetchone()[0]
        return columns, data, rows_count
    except Exception as e:
        msg = f"Something got wrong during data fetching. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e
    finally:
        LOGGER.info("Closing connection to postgres.")
        connection.close()


def get_schema(columns: tuple) -> list[dict]:
    """
    Get schema from dbt result.

    Parameters
    ----------
    columns : tuple
        The columns.

    Returns
    -------
    list
        A list of dictionaries containing schema.
    """
    try:
        return {"fields": [{"name": c.name, "type": TYPE_MAPPER.get(c.type_code, "any")} for c in columns]}
    except Exception as e:
        msg = f"Something got wrong during schema parsing. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e


def _get_data_preview(columns: tuple, data: list[tuple], rows_count: int) -> list[dict]:
    """
    Get data preview from dbt result.

    Parameters
    ----------
    columns : tuple
        The columns.
    data : list[tuple]
        The data.
    rows_count : int
        The number of rows.

    Returns
    -------
    list
        A list of dictionaries containing data.
    """
    try:
        columns = [i.name for i in columns]
        prepared_data = prepare_data(data)
        preview = prepare_preview(columns, prepared_data)
        finalized = finalize_preview(preview, rows_count)
        return check_preview_size(finalized)
    except Exception as e:
        msg = f"Something got wrong during data preview creation. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e


def build_status(dataitem: DataitemTable, results: dbtRunnerResult, output_table: str) -> dict:
    """
    Build status.

    Parameters
    ----------
    dataitem : DataitemTable
        The dataitem output.
    results : dbtRunnerResult
        The dbt results.
    output_table : str
        The output table name.

    Returns
    -------
    dict
        The status.
    """
    try:
        return {
            "state": State.COMPLETED.value,
            "outputs": {output_table: dataitem.key},
            "results": {"dbt_result": results.result[-1].to_dict()},
        }
    except Exception as e:
        msg = f"Something got wrong during status creation. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e

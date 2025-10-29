# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import shutil
import typing
from pathlib import Path

import psycopg2
from digitalhub.stores.credentials.enums import CredsEnvVar
from digitalhub.stores.data.api import get_store
from digitalhub.utils.exceptions import ConfigError
from digitalhub.utils.generic_utils import decode_base64_string, extract_archive, requests_chunk_download
from digitalhub.utils.git_utils import clone_repository
from digitalhub.utils.logger import LOGGER
from digitalhub.utils.uri_utils import (
    get_filename_from_uri,
    has_git_scheme,
    has_remote_scheme,
    has_s3_scheme,
    has_zip_scheme,
)
from psycopg2 import sql

if typing.TYPE_CHECKING:
    from digitalhub.stores.data.sql.configurator import SqlStoreConfigurator

##############################
# Templates
##############################

PROJECT_TEMPLATE = """
name: "{}"
version: "1.0.0"
config-version: 2
profile: "postgres"
model-paths: ["{}"]
models:
""".lstrip("\n")

MODEL_TEMPLATE_VERSION = """
models:
  - name: {}
    latest_version: {}
    versions:
        - v: {}
          config:
            materialized: table
""".lstrip("\n")

PROFILE_TEMPLATE = """
postgres:
    outputs:
        dev:
            type: postgres
            host: "{}"
            user: "{}"
            pass: "{}"
            port: {}
            dbname: "{}"
            schema: "public"
    target: dev
""".lstrip("\n")


def generate_dbt_profile_yml(
    root: Path,
    configurator: CredsConfigurator,
) -> None:
    """
    Generate dbt profiles.yml configuration file.

    Creates a dbt profiles.yml file with PostgreSQL connection configuration
    using credentials from the provided configurator. The file is written
    to the specified root directory.

    Parameters
    ----------
    root : Path
        The root directory path where the profiles.yml file will be created.
    configurator : CredsConfigurator
        The credential configurator instance containing database
        connection parameters.
    """
    profiles_path = root / "profiles.yml"
    host, port, user, password, db = configurator.get_creds()
    profiles_path.write_text(PROFILE_TEMPLATE.format(host, user, password, int(port), db))


def generate_dbt_project_yml(root: Path, model_dir: Path, project: str) -> None:
    """
    Generate dbt_project.yml configuration file.

    Creates a dbt_project.yml file with project configuration including
    name, version, and model paths. Uses the provided model directory
    name in the configuration.

    Parameters
    ----------
    root : Path
        The root directory path where the dbt_project.yml file will be created.
    model_dir : Path
        The model directory path whose name will be used in the configuration.
    project : str
        The name of the dbt project to be configured.
    """
    project_path = root / "dbt_project.yml"
    project_path.write_text(PROJECT_TEMPLATE.format(project, model_dir.name))


def generate_outputs_conf(model_dir: Path, sql: str, output: str, uuid: str) -> None:
    """
    Generate output configuration files for dbt models.

    Creates both SQL model file and YAML configuration file for dbt outputs.
    The SQL file contains the model code, while the YAML file contains
    versioning information for the output table.

    Parameters
    ----------
    model_dir : Path
        The directory path where the output files will be created.
    sql : str
        The SQL code content for the dbt model.
    output : str
        The name of the output table/model.
    uuid : str
        The unique identifier used for model versioning.
    """
    sql_path = model_dir / f"{output}.sql"
    sql_path.write_text(sql)

    output_path = model_dir / f"{output}.yml"
    output_path.write_text(MODEL_TEMPLATE_VERSION.format(output, uuid, uuid))


def generate_inputs_conf(model_dir: Path, name: str, uuid: str) -> None:
    """
    Generate input configuration files for dbt model dependencies.

    Creates both YAML configuration file for input versioning and
    SQL select file for the input schema. These files define the
    dependencies that the dbt model will use.

    Parameters
    ----------
    model_dir : Path
        The directory path where the input configuration files will be created.
    name : str
        The name of the input dataitem/table.
    uuid : str
        The unique identifier used for input versioning.
    """
    # write schema and version detail for inputs versioning
    input_path = model_dir / f"{name}.yml"
    input_path.write_text(MODEL_TEMPLATE_VERSION.format(name, uuid, uuid))

    # write also sql select for the schema
    sql_path = model_dir / f"{name}_v{uuid}.sql"
    sql_path.write_text(f'SELECT * FROM "{name}_v{uuid}"')


##############################
# Utils
##############################


def get_output_table_name(outputs: list[dict]) -> str:
    """
    Extract output table name from run specification.

    Retrieves the output table name from the outputs dictionary.
    Validates that the required 'output_table' key is present.

    Parameters
    ----------
    outputs : list[dict]
        The outputs specification containing table information.
        Must contain an 'output_table' key.

    Returns
    -------
    str
        The name of the output dataitem/table.

    Raises
    ------
    RuntimeError
        If outputs structure is invalid or 'output_table' key is missing.
    """
    try:
        return outputs["output_table"]
    except IndexError as e:
        msg = f"Outputs must be a list of one dataitem. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e
    except KeyError as e:
        msg = f"Must pass reference to 'output_table'. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e


##############################
# Source
##############################


def save_function_source(path: Path, source_spec: dict) -> str:
    """
    Download and save function source code from various sources.

    Handles multiple source types including inline code, base64 encoded content,
    Git repositories, HTTP/HTTPS URLs, and S3 paths. Automatically extracts
    archives and retrieves handler files when specified.

    Parameters
    ----------
    path : Path
        The local directory path where the source will be saved.
    source_spec : dict
        The source specification dictionary containing source information.
        May include keys: 'code', 'base64', 'source', 'handler'.

    Returns
    -------
    str
        The function source code content. Returns either inline code,
        decoded base64 content, or content from the handler file.

    Raises
    ------
    RuntimeError
        If no valid source is found in the specification or if
        the source scheme is unsupported.
    """
    # Get relevant information
    code = source_spec.get("code")
    base64 = source_spec.get("base64")
    source = source_spec.get("source")
    handler: str = source_spec.get("handler")

    if code is not None:
        return code

    if base64 is not None:
        return decode_base64_string(base64)

    if source is None:
        raise RuntimeError("Function source not found in spec.")

    # Git repo
    if has_git_scheme(source):
        clone_repository(path, source)

    # Http(s) or s3 presigned urls
    elif has_remote_scheme(source):
        filename = path / get_filename_from_uri(source)
        if has_zip_scheme(source):
            requests_chunk_download(source.removeprefix("zip+"), filename)
            extract_archive(path, filename)
            filename.unlink()
        else:
            requests_chunk_download(source, filename)

    # S3 path
    elif has_s3_scheme(source):
        if not has_zip_scheme(source):
            raise RuntimeError("S3 source must be a zip file with scheme zip+s3://.")
        filename = path / get_filename_from_uri(source)
        store = get_store(source.removeprefix("zip+"))
        store.get_s3_source(source, filename)
        extract_archive(path, filename)
        filename.unlink()

    if handler is not None:
        return (path / handler).read_text()

    # Unsupported scheme
    raise RuntimeError("Unable to collect source.")


##############################
# Creds configurator
##############################


class CredsConfigurator:
    """
    Database credentials configurator for dbt operations.

    Manages database connection credentials and provides validated
    connection parameters for PostgreSQL databases. Includes connection
    testing and credential caching functionality.

    Attributes
    ----------
    cfg : SqlStoreConfigurator
        The SQL store configurator instance for credential management.
    _valid_creds : tuple or None
        Cached valid database credentials to avoid repeated validation.
    """

    def __init__(self) -> None:
        self.cfg: SqlStoreConfigurator = get_store("sql://")._configurator
        self._valid_creds = None  # cache of valid creds

    def _test_connection(self, creds: tuple) -> tuple[bool, Exception | None]:
        host, port, user, password, db = creds
        try:
            conn = psycopg2.connect(
                host=host,
                port=port,
                database=db,
                user=user,
                password=password,
            )
            conn.close()
            return True, None
        except Exception as e:
            return False, e

    def get_creds(self, retry: bool = True) -> tuple:
        """
        Retrieve and validate database credentials.

        Gets database connection credentials from the configurator,
        tests the connection, and caches valid credentials for reuse.
        Supports retry mechanism with alternative credential sources.

        Parameters
        ----------
        retry : bool, default True
            Whether to attempt retry with alternative credential source
            if the initial connection fails.

        Returns
        -------
        tuple
            Database credentials tuple containing (host, port, user,
            password, database) in that order.

        Raises
        ------
        RuntimeError
            If no valid database connection can be established with
            available credentials.
        """
        if self._valid_creds is not None:
            return self._valid_creds

        creds_dict = self.cfg.get_sql_credentials()
        creds = (
            creds_dict[CredsEnvVar.DB_HOST.value],
            creds_dict[CredsEnvVar.DB_PORT.value],
            creds_dict[CredsEnvVar.DB_USERNAME.value],
            creds_dict[CredsEnvVar.DB_PASSWORD.value],
            creds_dict[CredsEnvVar.DB_DATABASE.value],
        )

        valid_conn, err = self._test_connection(creds)
        if valid_conn:
            self._valid_creds = creds
            return creds

        if retry:
            try:
                self.cfg.eval_change_origin()
            except ConfigError:
                raise RuntimeError(f"Error while connecting to database. {err}")
            return self.get_creds(retry=False)

        raise RuntimeError(f"Error while connecting to database. {err}")

    def get_database(self) -> str:
        """
        Get the database name from validated credentials.

        Retrieves and returns the database name component from
        the validated credential set.

        Returns
        -------
        str
            The name of the database from the credentials.
        """
        creds = self.get_creds()
        return creds[4]


##############################
# Engine
##############################


def get_connection(
    configurator: CredsConfigurator,
) -> psycopg2.extensions.connection:
    """
    Create a PostgreSQL database connection with autocommit enabled.

    Establishes a connection to PostgreSQL using validated credentials
    from the configurator. The connection is configured for immediate
    commit of all operations.

    Parameters
    ----------
    configurator : CredsConfigurator
        The credential configurator instance containing validated
        database connection parameters.

    Returns
    -------
    psycopg2.extensions.connection
        An active PostgreSQL connection with autocommit enabled.

    Raises
    ------
    RuntimeError
        If the connection cannot be established despite having
        validated credentials.
    """
    host, port, user, password, db = configurator.get_creds()
    try:
        return psycopg2.connect(
            host=host,
            port=port,
            database=db,
            user=user,
            password=password,
        )
    except Exception as e:
        msg = f"Unable to connect to postgres with validated credentials. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e


##############################
# Cleanup
##############################


def cleanup(
    tables: list[str],
    tmp_dir: Path,
    configurator: CredsConfigurator,
) -> None:
    """
    Clean up database tables and temporary directories.

    Removes specified database tables and deletes temporary directories
    created during dbt operations. Ensures proper cleanup of resources
    even if some operations fail.

    Parameters
    ----------
    tables : list[str]
        List of table names to drop from the database.
    tmp_dir : Path
        The temporary directory path to remove.
    configurator : CredsConfigurator
        The credential configurator for database connection.


    Raises
    ------
    RuntimeError
        If database operations fail during cleanup. The temporary
        directory removal will still be attempted.
    """
    try:
        connection = get_connection(configurator)
        with connection:
            with connection.cursor() as cursor:
                for table in tables:
                    LOGGER.info(f"Dropping table '{table}'.")
                    query = sql.SQL("DROP TABLE {table}").format(table=sql.Identifier(table))
                    cursor.execute(query)
    except Exception as e:
        msg = f"Something got wrong during environment cleanup. Exception: {e.__class__}. Error: {e.args}"
        LOGGER.exception(msg)
        raise RuntimeError(msg) from e
    finally:
        LOGGER.info("Closing connection to postgres.")
        connection.close()

    LOGGER.info("Removing temporary directory.")
    shutil.rmtree(tmp_dir, ignore_errors=True)

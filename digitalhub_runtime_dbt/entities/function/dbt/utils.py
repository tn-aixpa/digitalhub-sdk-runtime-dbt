# SPDX-FileCopyrightText: © 2025 DSLab - Fondazione Bruno Kessler
#
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import typing
from pathlib import Path

from digitalhub.stores.data.api import get_default_store, get_store
from digitalhub.utils.exceptions import EntityError
from digitalhub.utils.file_utils import eval_text_type, eval_zip_type
from digitalhub.utils.generic_utils import encode_string, read_source
from digitalhub.utils.uri_utils import has_local_scheme

from digitalhub_runtime_dbt.entities.function.dbt.models import Lang

if typing.TYPE_CHECKING:
    from digitalhub_runtime_dbt.entities.function.dbt.entity import FunctionDbt


def source_check(**kwargs) -> dict:
    """
    Check source code.

    Parameters
    ----------
    **kwargs
        Keyword arguments.

    Returns
    -------
    dict
        Checked source.
    """
    source: dict = kwargs.pop("source", None)
    code_src = kwargs.pop("code_src", None)
    code = kwargs.pop("code", None)
    base64 = kwargs.pop("base64", None)
    handler = kwargs.pop("handler", None)
    lang = kwargs.pop("lang", None)

    if source is not None:
        code_src = source.pop("source", None)
        code = source.pop("code", None)
        base64 = source.pop("base64", None)
        handler = source.pop("handler", None)
        lang = source.pop("lang", None)

    kwargs["source"] = _check_params(
        code_src=code_src,
        code=code,
        base64=base64,
        handler=handler,
        lang=lang,
    )
    return kwargs


def _check_params(
    code_src: str | None = None,
    code: str | None = None,
    base64: str | None = None,
    handler: str | None = None,
    lang: str | None = None,
) -> dict:
    """
    Check source code.

    Parameters
    ----------
    code_src : str
        Source code source.
    code : str
        Source code.
    base64 : str
        Source code base64.
    handler : str
        Function handler.
    lang : str
        Source code language.

    Returns
    -------
    dict
        Checked source.
    """
    source = {}

    if handler is not None:
        source["handler"] = handler

    if lang is None:
        source["lang"] = Lang.SQL.value

    if code_src is None and code is None and base64 is None:
        raise EntityError("Source must be provided.")

    if code_src is not None:
        source["source"] = code_src

    if base64 is not None:
        source["base64"] = base64

    if code is not None:
        source["base64"] = encode_string(code)

    return source


def source_post_check(exec: FunctionDbt) -> FunctionDbt:
    """
    Post check source.

    Parameters
    ----------
    exec : FunctionDbt
        Executable.

    Returns
    -------
    FunctionDbt
        Updated executable.
    """
    code_src = exec.spec.source.get("source", None)
    base64 = exec.spec.source.get("base64", None)
    if code_src is None or base64 is not None:
        return exec

    # Check local source
    if has_local_scheme(code_src):
        if not Path(code_src).is_file():
            raise EntityError(f"Source file {code_src} does not exist.")

        # Check text
        if eval_text_type(code_src):
            exec.spec.source["base64"] = read_source(code_src)

        # Check zip
        elif eval_zip_type(code_src):
            filename = Path(code_src).name
            dst = f"zip+{get_default_store(exec.project)}/{exec.project}/{exec.ENTITY_TYPE}/{exec.name}/{exec.id}/{filename}"
            get_store(dst).upload(code_src, dst)
            exec.spec.source["source"] = dst
            if exec.spec.source.get("handler") is None:
                raise EntityError("Handler must be provided.")

    return exec

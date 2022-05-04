# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import os
import sys
import typing
from typing import Dict, Literal, Tuple, Type

from sqlalchemy.types import BLOB, FLOAT, INTEGER, TEXT

if not hasattr(sys, "frozen"):
    KIARA_PLUGIN_TABULAR_BASE_FOLDER = os.path.dirname(__file__)
    """Marker to indicate the base folder for the kiara network module package."""
else:
    KIARA_PLUGIN_TABULAR_BASE_FOLDER = os.path.join(sys._MEIPASS, os.path.join("kiara_modules", "network_analysis"))  # type: ignore
    """Marker to indicate the base folder for the kiara network module package."""

KIARA_PLUGIN_TABULAR_RESOURCES_FOLDER = os.path.join(
    KIARA_PLUGIN_TABULAR_BASE_FOLDER, "resources"
)
"""Default resources folder for this package."""

TEMPLATES_FOLDER = os.path.join(KIARA_PLUGIN_TABULAR_RESOURCES_FOLDER, "templates")

DEFAULT_TABULAR_DATA_CHUNK_SIZE = 1024

SqliteDataType = Literal["NULL", "INTEGER", "REAL", "TEXT", "BLOB"]
SQLITE_DATA_TYPE: Tuple[SqliteDataType, ...] = typing.get_args(SqliteDataType)

SQLITE_SQLALCHEMY_TYPE_MAP: Dict[SqliteDataType, Type] = {
    "INTEGER": INTEGER,
    "REAL": FLOAT,
    "TEXT": TEXT,
    "BLOB": BLOB,
}

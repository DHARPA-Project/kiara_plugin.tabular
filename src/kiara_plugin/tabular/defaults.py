# -*- coding: utf-8 -*-
#  Copyright (c) 2022, Markus Binsteiner
#
#  Mozilla Public License, version 2.0 (see LICENSE or https://www.mozilla.org/en-US/MPL/2.0/)
import typing
from typing import Dict, Literal, Tuple, Type

from sqlalchemy.types import BLOB, FLOAT, INTEGER, TEXT, VARCHAR

# if not hasattr(sys, "frozen"):
#     KIARA_PLUGIN_TABULAR_BASE_FOLDER = os.path.dirname(__file__)
#     """Marker to indicate the base folder for the kiara network module package."""
# else:
#     KIARA_PLUGIN_TABULAR_BASE_FOLDER = os.path.join(sys._MEIPASS, os.path.join("kiara_modules", "network_analysis"))  # type: ignore
#     """Marker to indicate the base folder for the kiara network module package."""

# KIARA_PLUGIN_TABULAR_RESOURCES_FOLDER = os.path.join(
#     KIARA_PLUGIN_TABULAR_BASE_FOLDER, "resources"
# )
# """Default resources folder for this package."""

# TEMPLATES_FOLDER = os.path.join(KIARA_PLUGIN_TABULAR_RESOURCES_FOLDER, "templates")

DEFAULT_TABULAR_DATA_CHUNK_SIZE = 1024

SqliteDataType = Literal["NULL", "INTEGER", "REAL", "TEXT", "BLOB", "FLOAT"]
SQLITE_DATA_TYPE: Tuple[SqliteDataType, ...] = typing.get_args(SqliteDataType)

SQLITE_SQLALCHEMY_TYPE_MAP: Dict[SqliteDataType, Type] = {
    "INTEGER": INTEGER,
    "FLOAT": FLOAT,
    "REAL": FLOAT,
    "TEXT": TEXT,
    "BLOB": BLOB,
}

SQLALCHEMY_SQLITE_TYPE_MAP: Dict[Type, SqliteDataType] = {
    INTEGER: "INTEGER",
    FLOAT: "REAL",
    TEXT: "TEXT",
    BLOB: "BLOB",
    VARCHAR: "TEXT",
}

RESERVED_SQL_KEYWORDS = [
    "ALL",
    "AND",
    "ARRAY",
    "AS",
    "BETWEEN",
    "BOTH",
    "CASE",
    "CHECK",
    "CONSTRAINT",
    "CROSS",
    "CURRENT",
    "CURRENT",
    "CURRENT",
    "CURRENT",
    "CURRENT",
    "CURRENT",
    "DISTINCT",
    "EXCEPT",
    "EXISTS",
    "FALSE",
    "FETCH",
    "FILTER",
    "FOR",
    "FOREIGN",
    "FROM",
    "FULL",
    "GROUP",
    "GROUPS",
    "HAVING",
    "IF",
    "ILIKE",
    "IN",
    "INNER",
    "INTERSECT",
    "INTERSECTS",
    "INTERVAL",
    "IS",
    "JOIN",
    "LEADING",
    "LEFT",
    "LIKE",
    "LIMIT",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "MINUS",
    "NATURAL",
    "NOT",
    "NULL",
    "OFFSET",
    "ON",
    "OR",
    "ORDER",
    "OVER",
    "PARTITION",
    "PRIMARY",
    "QUALIFY",
    "RANGE",
    "REGEXP",
    "RIGHT",
    "ROW",
    "_ROWID",
    "ROWNUM",
    "ROWS",
    "SELECT",
    "SYSDATE",
    "SYSTIME",
    "SYSTIMESTAMP",
    "TABLE",
    "TODAY",
    "TOP",
    "TRAILING",
    "TRUE",
    "UNION",
    "UNIQUE",
    "UNKNOWN",
    "USING",
    "VALUES",
    "WHERE",
    "WINDOW",
    "WITH",
]

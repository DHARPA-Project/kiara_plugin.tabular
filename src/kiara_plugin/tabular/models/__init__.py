# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.tabular`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
from typing import Any, Dict, List, Union

from kiara.models import KiaraModel
from pydantic import BaseModel, Field


class ColumnSchema(BaseModel):
    """Describes properties of a single column of the 'table' data type."""

    type_name: str = Field(
        description="The type name of the column (backend-specific)."
    )
    metadata: Dict[str, Any] = Field(
        description="Other metadata for the column.", default_factory=dict
    )


class TableMetadata(KiaraModel):
    """Describes properties for the 'table' data type."""

    column_names: List[str] = Field(description="The name of the columns of the table.")
    column_schema: Dict[str, ColumnSchema] = Field(
        description="The schema description of the table."
    )
    rows: int = Field(description="The number of rows the table contains.")
    size: Union[int, None] = Field(
        description="The tables size in bytes.", default=None
    )

    def _retrieve_data_to_hash(self) -> Any:

        return {
            "column_schemas": {k: v.dict() for k, v in self.column_schema.items()},
            "rows": self.rows,
            "size": self.size,
        }

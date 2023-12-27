# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.tabular`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
from typing import TYPE_CHECKING, Any, Dict, List, Union

from pydantic import BaseModel, Field

from kiara.models import KiaraModel

if TYPE_CHECKING:
    from kiara_plugin.tabular.models.table import KiaraTable


class StorageBackend(BaseModel):
    """Describes the storage backend type that is used, and (optionally) some backend-specific properties."""

    name: str = Field(description="The name of the storage backend.")
    properties: Dict[str, Any] = Field(
        description="Backend-specific properties.", default_factory=dict
    )


class ColumnSchema(BaseModel):
    """Describes properties of a single column of the 'table' data type."""

    type_name: str = Field(
        description="The type name of the column (backend-specific)."
    )
    metadata: Dict[str, Dict[str, Any]] = Field(
        description="Other metadata for the column.", default_factory=dict
    )

    def _retrieve_data_to_hash(self) -> Any:
        return self.model_dump()


class TableMetadata(KiaraModel):
    """Describes properties for the 'table' data type."""

    @classmethod
    def create_from_table(cls, table: "KiaraTable") -> "TableMetadata":

        arrow_table = table.arrow_table
        table_schema: Dict[str, Any] = {}

        backend_properties: Dict[str, Any] = {"column_types": {}}

        for name in arrow_table.schema.names:
            field = arrow_table.schema.field(name)
            _md = table.get_column_metadata(column_name=name)
            md = {}
            for _col, _col_md in _md.items():
                md[_col] = _col_md.model_dump()
            _type = field.type
            backend_properties["column_types"][name] = {
                "type_id": _type.id,
                "size": arrow_table[name].nbytes,
            }
            _d = {
                "type_name": str(_type),
                "metadata": md,
            }
            table_schema[name] = _d

        backend = StorageBackend(name="arrow", properties=backend_properties)
        schema = {
            "column_names": table.column_names,
            "column_schema": table_schema,
            "backend": backend,
            "rows": table.num_rows,
            "size": arrow_table.nbytes,
        }
        result = TableMetadata(**schema)
        return result

    column_names: List[str] = Field(description="The name of the columns of the table.")
    column_schema: Dict[str, ColumnSchema] = Field(
        description="The schema description of the table."
    )
    backend: StorageBackend = Field(
        description="The storage backend that is used, and backend-specific properties."
    )
    rows: int = Field(description="The number of rows the table contains.")
    size: Union[int, None] = Field(
        description="The tables size in bytes.", default=None
    )

    def _retrieve_data_to_hash(self) -> Any:

        return {
            "column_schemas": {
                k: v._retrieve_data_to_hash() for k, v in self.column_schema.items()
            },
            "rows": self.rows,
            "size": self.size,
        }

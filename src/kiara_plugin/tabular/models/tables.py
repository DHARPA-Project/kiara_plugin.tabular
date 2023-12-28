# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Iterable, List, Mapping, Union

import pyarrow as pa
from pydantic import Field

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.tabular.defaults import DEFAULT_TABLE_NAME
from kiara_plugin.tabular.models import TableMetadata
from kiara_plugin.tabular.models.table import KiaraTable

try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self  # type: ignore

if TYPE_CHECKING:
    from kiara.models.values.value import Value


class KiaraTables(KiaraModel):
    """A wrapper class, containing multiple tables."""

    @classmethod
    def create_tables(cls, data: Any) -> Self:

        if isinstance(data, KiaraTables):
            return data

        table_obj: Union[None, Dict[str, KiaraTable]] = None
        if isinstance(data, Mapping):
            temp = {}
            for k, v in data.items():
                temp[k] = KiaraTable.create_table(v)
            table_obj = temp

        elif isinstance(data, (pa.Table)):
            table_obj = {DEFAULT_TABLE_NAME: data}

        if table_obj is None:
            if isinstance(data, (str)):
                raise Exception(
                    f"Can't create tables, invalid source data type 'string', check if maybe invalid alias: {data}."
                )
            raise Exception(
                f"Can't create tables, invalid source data type: {type(data)}."
            )

        obj = cls(tables=table_obj)
        return obj

    tables: Dict[str, KiaraTable] = Field(
        description="A dictionary of tables, with the table names as keys."
    )

    @property
    def table_names(self) -> List[str]:
        return list(self.tables.keys())

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    def get_table(self, table_name: str) -> KiaraTable:

        if table_name not in self.tables:
            raise KiaraException(
                f"Table '{table_name}' not found. Available: {', '.join(self.tables.keys())}"
            )

        return self.tables[table_name]


class KiaraTablesMetadata(ValueMetadata):
    """File stats."""

    _metadata_key: ClassVar[str] = "tables"

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["tables"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "KiaraTablesMetadata":

        kiara_tables: KiaraTables = value.data

        tables = {}
        for table_name, table in kiara_tables.tables.items():

            md = TableMetadata.create_from_table(table)
            tables[table_name] = md

        return KiaraTablesMetadata(tables=tables)

    tables: Dict[str, TableMetadata] = Field(description="The table schema.")

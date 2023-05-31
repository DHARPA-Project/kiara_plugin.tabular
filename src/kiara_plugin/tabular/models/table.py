# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Iterable, Union

import pyarrow as pa
from pydantic import Field, PrivateAttr

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.tabular.models import TableMetadata

if TYPE_CHECKING:
    import polars as pl


class KiaraTable(KiaraModel):
    """A wrapper class to manage tabular data in a memory efficient way."""

    @classmethod
    def create_table(cls, data: Any) -> "KiaraTable":
        """Create a `KiaraTable` instance from an Apache Arrow Table, or dict of lists."""

        if isinstance(data, KiaraTable):
            return data
        elif isinstance(data, Value):
            if data.data_type_name != "table":
                raise KiaraException(
                    f"Invalid data type '{data.data_type_name}', need 'table'."
                )
            return data.data  # type: ignore

        table_obj = None
        if isinstance(data, (pa.Table)):
            table_obj = data
        else:
            try:
                table_obj = pa.table(data)
            except Exception:
                pass

        if table_obj is None:
            raise Exception(
                f"Can't create table, invalid source data type: {type(data)}."
            )

        obj = KiaraTable()
        obj._table_obj = table_obj
        return obj

    data_path: Union[None, str] = Field(
        description="The path to the (feather) file backing this array.", default=None
    )
    """The path where the table object is store (for internal or read-only use)."""
    _table_obj: pa.Table = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    @property
    def arrow_table(self) -> pa.Table:
        """Return the data as an Apache Arrow Table instance."""

        if self._table_obj is not None:
            return self._table_obj

        if not self.data_path:
            raise Exception("Can't retrieve table data, object not initialized (yet).")

        with pa.memory_map(self.data_path, "r") as source:
            table: pa.Table = pa.ipc.open_file(source).read_all()

        self._table_obj = table
        return self._table_obj

    @property
    def polars_dataframe(self) -> "pl.DataFrame":
        """Return the data as a Polars dataframe."""

        import polars as pl

        return pl.from_arrow(self.arrow_table)  # type: ignore

    @property
    def column_names(self) -> Iterable[str]:
        """Retrieve the names of all the columns of this table."""
        return self.arrow_table.column_names

    @property
    def num_rows(self) -> int:
        """Return the number of rows in this table."""
        return self.arrow_table.num_rows

    def to_pydict(self):
        """Convert and return the table data as a dictionary of lists.

        This will load all data into memory, so you might or might not want to do that.
        """
        return self.arrow_table.to_pydict()

    def to_pylist(self):
        """Convert and return the table data as a list of rows/dictionaries.

        This will load all data into memory, so you might or might not want to do that.
        """

        return self.arrow_table.to_pylist()

    def to_pandas(self):
        """Convert and return the table data to a Pandas dataframe.

        This will load all data into memory, so you might or might not want to do that.
        """
        return self.arrow_table.to_pandas()


class KiaraTableMetadata(ValueMetadata):
    """File stats."""

    _metadata_key = "table"

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["table"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "KiaraTableMetadata":

        kiara_table: KiaraTable = value.data

        table: pa.Table = kiara_table.arrow_table

        table_schema = {}
        for name in table.schema.names:
            field = table.schema.field(name)
            md = field.metadata
            _type = field.type
            if not md:
                md = {
                    "arrow_type_id": _type.id,
                }
            _d = {
                "type_name": str(_type),
                "metadata": md,
            }
            table_schema[name] = _d

        schema = {
            "column_names": table.column_names,
            "column_schema": table_schema,
            "rows": table.num_rows,
            "size": table.nbytes,
        }

        md = TableMetadata.construct(**schema)
        return KiaraTableMetadata.construct(table=md)

    table: TableMetadata = Field(description="The table schema.")

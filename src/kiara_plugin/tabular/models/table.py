# -*- coding: utf-8 -*-
from typing import Any, Iterable, List, Optional

import pyarrow as pa
from kiara.models import KiaraModel
from kiara.models.render_value import (
    RenderInstruction,
    RenderMetadata,
    RenderValueResult,
)
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara.utils.output import ArrowTabularWrap
from pydantic import Field, PrivateAttr

from kiara_plugin.tabular.models import TableMetadata


class KiaraTable(KiaraModel):
    """A wrapper class to manage tabular data in a memory efficient way."""

    @classmethod
    def create_table(cls, data: Any) -> "KiaraTable":
        """Create a `KiaraTable` instance from an Apache Arrow Table, or dict of lists."""

        table_obj = None
        if isinstance(data, KiaraTable):
            return data

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

    data_path: Optional[str] = Field(
        description="The path to the (feather) file backing this array."
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


class RenderTableInstruction(RenderInstruction):
    @classmethod
    def retrieve_source_type(cls) -> str:
        return "table"

    _kiara_model_id = "instance.render_instruction.table"
    number_of_rows: int = Field(description="How many rows to display.", default=20)
    row_offset: int = Field(description="From which row to start.", default=0)
    columns: Optional[List[str]] = Field(
        description="Which rows do display.", default=None
    )

    def render_as__terminal_renderable(self, value: Value):

        import duckdb

        table: KiaraTable = value.data

        columnns: Iterable[str] = self.columns  # type: ignore
        if not columnns:
            columnns = table.column_names

        assert columnns

        query = f"""SELECT {', '.join(columnns)} FROM data ORDER by {', '.join(columnns)} LIMIT {self.number_of_rows} OFFSET {self.row_offset}"""

        rel_from_arrow = duckdb.arrow(table.arrow_table)
        query_result: duckdb.DuckDBPyResult = rel_from_arrow.query("data", query)

        result_table = query_result.fetch_arrow_table()

        wrap = ArrowTabularWrap(table=result_table)
        pretty = wrap.pretty_print()

        related_instructions = {}

        related_instructions["first"] = RenderTableInstruction.construct(
            **{"row_offset": 0, "columns": self.columns}
        )

        if self.row_offset > 0:
            p_offset = self.row_offset - self.number_of_rows
            if p_offset < 0:
                p_offset = 0
            previous = {"row_offset": p_offset, "columns": self.columns}
            related_instructions["previous"] = RenderTableInstruction.construct(
                **previous
            )

        n_offset = self.row_offset + self.number_of_rows
        if n_offset < table.num_rows:
            next = {"row_offset": n_offset, "columns": self.columns}
            related_instructions["next"] = RenderTableInstruction.construct(**next)

        row_offset = table.num_rows - self.number_of_rows
        if row_offset < 0:
            row_offset = 0
        related_instructions["last"] = RenderTableInstruction.construct(
            **{"row_offset": row_offset, "columns": columnns}
        )

        render_metadata = RenderMetadata(related_instructions=related_instructions)

        return RenderValueResult(rendered=pretty, metadata=render_metadata)

# -*- coding: utf-8 -*-
from typing import Any, Iterable, Union

import pyarrow as pa
from kiara.models import KiaraModel
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
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


# class BaseRenderTableScene(RenderScene):
#
#     _kiara_model_id = None
#
#     render_metadata: bool = Field(
#         description="Render the table value metadata.", default=False
#     )
#     number_of_rows: int = Field(description="How many rows to display.", default=20)
#     row_offset: int = Field(description="From which row to start.", default=0)
#
#     def preprocess_table(self, value: Value, inputs: Mapping[str, Any]):
#
#         render_metadata = inputs.get("render_metadata", False)
#         number_of_rows = inputs.get("number_of_rows", 20)
#         row_offset = inputs.get("row_offset", 0)
#
#         import duckdb
#
#         if value.data_type_name == "array":
#             array: KiaraArray = value.data
#             arrow_table = pa.table(data=[array.arrow_array], names=["array"])
#             column_names: Iterable[str] = ["array"]
#         else:
#             table: KiaraTable = value.data
#             arrow_table = table.arrow_table
#             column_names = table.column_names
#
#         columnns = [f'"{x}"' if not x.startswith('"') else x for x in column_names]
#
#         # query = f"""SELECT {', '.join(columnns)} FROM data ORDER by {', '.join(columnns)} LIMIT {self.number_of_rows} OFFSET {self.row_offset}"""
#         query = f"""SELECT {', '.join(columnns)} FROM data LIMIT {self.number_of_rows} OFFSET {self.row_offset}"""
#
#         rel_from_arrow = duckdb.arrow(arrow_table)
#         query_result: duckdb.DuckDBPyResult = rel_from_arrow.query("data", query)
#
#         result_table = query_result.fetch_arrow_table()
#         wrap = ArrowTabularWrap(table=result_table)
#
#         related_scenes: Dict[str, Union[None, RenderScene]] = {}
#
#         row_offset = arrow_table.num_rows - self.number_of_rows
#
#         if row_offset > 0:
#
#             if self.row_offset > 0:
#                 related_scenes["first"] = self.__class__(
#                     **{"row_offset": 0, "number_of_rows": self.number_of_rows}  # type: ignore
#                 )
#
#                 p_offset = self.row_offset - self.number_of_rows
#                 if p_offset < 0:
#                     p_offset = 0
#                 previous = {
#                     "row_offset": p_offset,
#                     "number_of_rows": self.number_of_rows,
#                 }
#                 related_scenes["previous"] = self.__class__(**previous)  # type: ignore
#             else:
#                 related_scenes["first"] = None
#                 related_scenes["previous"] = None
#
#             n_offset = self.row_offset + self.number_of_rows
#             if n_offset < arrow_table.num_rows:
#                 next = {"row_offset": n_offset, "number_of_rows": self.number_of_rows}
#                 related_scenes["next"] = self.__class__(**next)  # type: ignore
#             else:
#                 related_scenes["next"] = None
#
#             last_page = int(arrow_table.num_rows / self.number_of_rows)
#             current_start = last_page * self.number_of_rows
#             if (self.row_offset + self.number_of_rows) > arrow_table.num_rows:
#                 related_scenes["last"] = None
#             else:
#                 related_scenes["last"] = self.__class__(
#                     **{  # type: ignore
#                         "row_offset": current_start,  # type: ignore
#                         "number_of_rows": self.number_of_rows,  # type: ignore
#                     }
#                 )
#         else:
#             related_scenes["first"] = None
#             related_scenes["previous"] = None
#             related_scenes["next"] = None
#             related_scenes["last"] = None
#
#         return wrap, related_scenes


# class RenderTableScene(BaseRenderTableScene):
#
#     _kiara_model_id = "instance.render_scene.terminal_table"
#
#     def render_as__terminal_renderable(self, value: Value):
#
#         render_config = {
#             "show_pedigree": False,
#             "show_serialized": False,
#             "show_data_preview": False,
#             "show_properties": True,
#             "show_destinies": True,
#             "show_destiny_backlinks": True,
#             "show_lineage": True,
#             "show_environment_hashes": False,
#             "show_environment_data": False,
#         }
#
#         if self.render_metadata:
#             pretty = value.create_info().create_renderable(**render_config)
#             related_scenes = {"data": RenderTableScene(title="data", description="Display the table data.", manifest_hash=self.manifest_hash, inputs={"render_metadata": False})}
#         else:
#             wrap, related_scenes = self.preprocess_table(value=value)
#             related_scenes["metadata"] = RenderTableScene(title="metadata", description="Display the table metadata.", manifest_hash=self.manifest_hash, inputs={"render_metadata": True})
#             pretty = wrap.as_terminal_renderable(max_row_height=1)
#
#         return RenderSceneResult(rendered=pretty, related_scenes=related_scenes)

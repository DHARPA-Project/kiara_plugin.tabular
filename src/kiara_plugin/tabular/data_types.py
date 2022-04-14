# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_plugin.tabular`` package.
"""
import uuid
from typing import Any, Iterable, Mapping, Optional, Type

import pyarrow as pa
from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType
from kiara.defaults import (
    ARRAY_MODEL_CATEOGORY_ID,
    DEFAULT_PRETTY_PRINT_CONFIG,
    KIARA_HASH_FUNCTION,
    TABLE_MODEL_CATEOGORY_ID,
)
from kiara.models import KiaraModel
from kiara.models.values.value import Value
from kiara.utils.output import ArrowTabularWrap
from pydantic import Field, PrivateAttr


class KiaraArray(KiaraModel):

    # @classmethod
    # def create_in_temp_dir(cls, ):
    #
    #     temp_f = tempfile.mkdtemp()
    #     file_path = os.path.join(temp_f, "array.feather")
    #
    #     def cleanup():
    #         shutil.rmtree(file_path, ignore_errors=True)
    #
    #     atexit.register(cleanup)
    #
    #     array_obj = cls(feather_path=file_path)
    #     return array_obj

    @classmethod
    def create_array(cls, data: Any) -> "KiaraArray":

        array_obj = None
        if isinstance(data, (pa.Array, pa.ChunkedArray)):
            array_obj = data
        elif isinstance(data, pa.Table):
            if len(data.columns) != 1:
                raise Exception(
                    f"Invalid type, only Arrow Arrays or single-column Tables allowed. This value is a table with {len(data.columns)} columns."
                )
            array_obj = data.column(0)
        else:
            try:
                array_obj = pa.array(data)
            except Exception:
                pass

        if array_obj is None:
            raise Exception(
                f"Can't create array, invalid source data type: {type(data)}."
            )

        obj = KiaraArray()
        obj._array_obj = array_obj
        return obj

    data_path: Optional[str] = Field(
        description="The path to the (feather) file backing this array."
    )

    _array_obj: pa.Array = PrivateAttr()

    def _retrieve_id(self) -> str:
        return str(uuid.uuid4())

    def _retrieve_category_id(self) -> str:
        return ARRAY_MODEL_CATEOGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    @property
    def arrow_array(self) -> pa.Array:

        if self._array_obj is not None:
            return self._array_obj

        if not self.data_path:
            raise Exception("Can't retrieve array data, object not initialized (yet).")

        with pa.memory_map(self.data_path, "r") as source:
            table: pa.Table = pa.ipc.open_file(source).read_all()

        if len(table.columns) != 1:
            raise Exception(
                f"Invalid serialized array data, only a single-column Table is allowed. This value is a table with {len(table.columns)} columns."
            )

        self._array_obj = table.column[0]
        return self._array_obj

    def to_pylist(self):
        return self.arrow_array.to_pylist()

    def to_pandas(self):
        return self.arrow_array.to_pandas()


class ArrayType(AnyType[KiaraArray, DataTypeConfig]):
    """An array, in most cases used as a column within a table.

    Internally, this type uses the [Apache Arrow](https://arrow.apache.org) [Array](https://arrow.apache.org/docs/python/generated/pyarrow.Array.html#pyarrow.Array) to store the data in memory (and on disk).
    """

    _data_type_name = "array"

    @classmethod
    def python_class(cls) -> Type:
        return KiaraArray

    def calculate_hash(self, data: pa.Array) -> int:
        return KIARA_HASH_FUNCTION(memoryview(data))

    def calculate_size(self, data: pa.Array) -> int:
        return len(memoryview(data))

    def parse_python_obj(self, data: Any) -> KiaraArray:

        return KiaraArray.create_array(data)

    def _validate(cls, value: Any) -> None:

        if not isinstance(value, (pa.Array, pa.ChunkedArray)):
            raise Exception(
                f"Invalid type '{type(value).__name__}', must be an Apache Arrow Array type."
            )

    def render_as_terminal_renderable(
        self, value: Value, render_config: Mapping[str, Any]
    ) -> Any:

        max_rows = render_config.get("max_no_rows")
        max_row_height = render_config.get("max_row_height")
        max_cell_length = render_config.get("max_cell_length")

        half_lines: Optional[int] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        import pyarrow as pa

        array: pa.Array = value.data.arrow_array

        temp_table = pa.Table.from_arrays(arrays=[array], names=["array"])
        atw = ArrowTabularWrap(temp_table)
        result = [
            atw.pretty_print(
                rows_head=half_lines,
                rows_tail=half_lines,
                max_row_height=max_row_height,
                max_cell_length=max_cell_length,
            )
        ]
        return result


class KiaraTable(KiaraModel):
    @classmethod
    def create_table(cls, data: Any) -> "KiaraTable":

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
                f"Can't create array, invalid source data type: {type(data)}."
            )

        obj = KiaraTable()
        obj._table_obj = table_obj
        return obj

    data_path: Optional[str] = Field(
        description="The path to the (feather) file backing this array."
    )
    _table_obj: pa.Table = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(uuid.uuid4())

    def _retrieve_category_id(self) -> str:
        return TABLE_MODEL_CATEOGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    @property
    def arrow_table(self) -> pa.Table:

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
        return self.arrow_table.column_names

    @property
    def num_rows(self) -> int:
        return self.arrow_table.num_rows

    def to_pydict(self):
        return self.arrow_table.to_pydict()

    def to_pylist(self):
        return self.arrow_table.to_pylist()

    def to_pandas(self):
        return self.arrow_table.to_pandas()


class TableType(AnyType):
    """Tabular data (table, spreadsheet, data_frame, what have you).

    Internally, this is backed by the [Apache Arrow](https://arrow.apache.org) [``Table``](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) class.
    """

    _data_type_name = "table"

    @classmethod
    def python_class(cls) -> Type:
        return pa.Table

    def parse_python_obj(self, data: Any) -> KiaraTable:

        return KiaraTable.create_table(data)

    def _validate(cls, value: Any) -> None:

        pass

        if not isinstance(value, KiaraTable):
            raise Exception(
                f"invalid type '{type(value).__name__}', must be 'KiaraTable'."
            )

    def render_as__terminal_renderable(
        self, value: "Value", render_config: Mapping[str, Any]
    ) -> Any:

        max_rows = render_config.get(
            "max_no_rows", DEFAULT_PRETTY_PRINT_CONFIG["max_no_rows"]
        )
        max_row_height = render_config.get(
            "max_row_height", DEFAULT_PRETTY_PRINT_CONFIG["max_row_height"]
        )
        max_cell_length = render_config.get(
            "max_cell_length", DEFAULT_PRETTY_PRINT_CONFIG["max_cell_length"]
        )

        half_lines: Optional[int] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        atw = ArrowTabularWrap(value.data.arrow_table)
        result = atw.pretty_print(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        return result

# -*- coding: utf-8 -*-

"""This module contains the value type classes that are used in the ``kiara_plugin.tabular`` package.
"""
from typing import Any, Mapping, Optional, Type

import pyarrow as pa
from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG, KIARA_HASH_FUNCTION
from kiara.models.values.value import Value
from kiara.utils.output import ArrowTabularWrap

from kiara_plugin.tabular.models import KiaraArray, KiaraTable


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

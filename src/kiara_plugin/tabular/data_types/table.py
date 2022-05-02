# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Mapping, Optional, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.models.values.value import SerializationResult, SerializedData, Value
from kiara.utils.hashing import compute_hash
from kiara.utils.output import ArrowTabularWrap
from mmh3 import hash_from_buffer

from kiara_plugin.tabular.models.table import KiaraArray, KiaraTable
from kiara_plugin.tabular.tabular.table import EMPTY_COLUMN_NAME_MARKER

if TYPE_CHECKING:
    import pyarrow as pa


def store_array(array_obj: "pa.Array", file_name: str, column_name: "str" = "array"):
    """Utility methdo to stora an array to a file."""

    import pyarrow as pa

    schema = pa.schema([pa.field(column_name, array_obj.type)])

    # TODO: support non-single chunk columns
    with pa.OSFile(file_name, "wb") as sink:
        with pa.ipc.new_file(sink, schema=schema) as writer:
            batch = pa.record_batch(array_obj.chunks, schema=schema)
            writer.write(batch)


class ArrayType(AnyType[KiaraArray, DataTypeConfig]):
    """An array, in most cases used as a column within a table.

    Internally, this type uses the [Apache Arrow](https://arrow.apache.org) [Array](https://arrow.apache.org/docs/python/generated/pyarrow.Array.html#pyarrow.Array) to store the data in memory (and on disk).
    """

    _data_type_name = "array"

    @classmethod
    def python_class(cls) -> Type:
        return KiaraArray

    def calculate_hash(self, data: KiaraArray) -> int:
        hashes = []

        for chunk in data.arrow_array.chunks:
            for buf in chunk.buffers():
                if not buf:
                    continue
                h = hash_from_buffer(memoryview(buf))
                hashes.append(h)
        return compute_hash(hashes)
        # return KIARA_HASH_FUNCTION(memoryview(data.arrow_array))

    def calculate_size(self, data: KiaraArray) -> int:
        return len(data.arrow_array)

    def parse_python_obj(self, data: Any) -> KiaraArray:

        return KiaraArray.create_array(data)

    def _validate(cls, value: Any) -> None:

        if not isinstance(value, (KiaraArray)):
            raise Exception(
                f"Invalid type '{type(value).__name__}', must be an instance of the 'KiaraArray' class."
            )

    def serialize(self, data: KiaraArray) -> SerializedData:

        import pyarrow as pa

        # TODO: make sure temp dir is in the same partition as file store
        temp_f = tempfile.mkdtemp()

        def cleanup():
            shutil.rmtree(temp_f, ignore_errors=True)

        atexit.register(cleanup)

        column: pa.Array = data.arrow_array
        file_name = os.path.join(temp_f, "array.arrow")

        store_array(array_obj=column, file_name=file_name, column_name="array")

        chunks = {"array.arrow": {"type": "file", "codec": "raw", "file": file_name}}

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": chunks,
            "serialization_profile": "feather",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "load.array",
                        "module_config": {
                            "value_type": "array",
                            "target_profile": "python_object",
                            "serialization_profile": "feather",
                        },
                    }
                },
            },
        }

        serialized = SerializationResult(**serialized_data)
        return serialized

    def render_as__terminal_renderable(
        self, value: Value, render_config: Mapping[str, Any]
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

        import pyarrow as pa

        array: pa.Array = value.data.arrow_array

        temp_table = pa.Table.from_arrays(arrays=[array], names=["array"])
        atw = ArrowTabularWrap(temp_table)
        result = atw.pretty_print(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
            show_table_header=False,
        )

        return result


class TableType(AnyType[KiaraTable, DataTypeConfig]):
    """Tabular data (table, spreadsheet, data_frame, what have you).

    Internally, this is backed by the [Apache Arrow](https://arrow.apache.org) [``Table``](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html) class.
    """

    _data_type_name = "table"

    @classmethod
    def python_class(cls) -> Type:
        import pyarrow as pa

        return pa.Table

    def parse_python_obj(self, data: Any) -> KiaraTable:

        return KiaraTable.create_table(data)

    def calculate_hash(self, data: KiaraTable) -> int:
        hashes = []
        for column_name in data.arrow_table.column_names:
            hashes.append(column_name)
            column = data.arrow_table.column(column_name)
            for chunk in column.chunks:
                for buf in chunk.buffers():
                    if not buf:
                        continue
                    h = hash_from_buffer(memoryview(buf))
                    hashes.append(h)
        return compute_hash(hashes)
        # return KIARA_HASH_FUNCTION(memoryview(data.arrow_array))

    def calculate_size(self, data: KiaraTable) -> int:
        return len(data.arrow_table)

    def _validate(cls, value: Any) -> None:

        pass

        if not isinstance(value, KiaraTable):
            raise Exception(
                f"invalid type '{type(value).__name__}', must be 'KiaraTable'."
            )

    def serialize(self, data: KiaraTable) -> SerializedData:

        import pyarrow as pa

        chunk_map = {}

        # TODO: make sure temp dir is in the same partition as file store
        temp_f = tempfile.mkdtemp()

        def cleanup():
            shutil.rmtree(temp_f, ignore_errors=True)

        atexit.register(cleanup)

        for column_name in data.arrow_table.column_names:
            column: pa.Array = data.arrow_table.column(column_name)
            if column_name == "":
                file_name = os.path.join(temp_f, EMPTY_COLUMN_NAME_MARKER)
            else:
                file_name = os.path.join(temp_f, column_name)
            store_array(array_obj=column, file_name=file_name, column_name=column_name)
            chunk_map[column_name] = {"type": "file", "file": file_name, "codec": "raw"}

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": chunk_map,
            "serialization_profile": "feather",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "load.table",
                        "module_config": {
                            "value_type": "table",
                            "target_profile": "python_object",
                            "serialization_profile": "feather",
                        },
                    }
                },
            },
        }

        serialized = SerializationResult(**serialized_data)
        return serialized

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

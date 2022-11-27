# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import Any, Mapping, Type, Union

import pyarrow as pa
from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.models.values.value import SerializationResult, SerializedData, Value
from kiara.utils.output import ArrowTabularWrap

from kiara_plugin.tabular.models.array import KiaraArray


def store_array(array_obj: "pa.Array", file_name: str, column_name: "str" = "array"):
    """Utility methdo to stora an array to a file."""

    import pyarrow as pa
    from pyarrow import ChunkedArray

    schema = pa.schema([pa.field(column_name, array_obj.type)])

    # TODO: support non-single chunk columns
    with pa.OSFile(file_name, "wb") as sink:
        with pa.ipc.new_file(sink, schema=schema) as writer:
            if isinstance(array_obj, ChunkedArray):
                for chunk in array_obj.chunks:
                    batch = pa.record_batch([chunk], schema=schema)
                    writer.write(batch)
            else:
                raise NotImplementedError()


class ArrayType(AnyType[KiaraArray, DataTypeConfig]):
    """An array, in most cases used as a column within a table.

    Internally, this type uses the [KiaraArray][kiara_plugin.tabular.models.array.KiaraArray] wrapper class to manage array data. This wrapper class, in turn, uses an [Apache Arrow](https://arrow.apache.org) [Array](https://arrow.apache.org/docs/python/generated/pyarrow.Array.html#pyarrow.Array) to store the data in memory (and on disk).
    """

    _data_type_name = "array"

    @classmethod
    def python_class(cls) -> Type:
        return KiaraArray

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

    def pretty_print_as__terminal_renderable(
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

        half_lines: Union[int, None] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        import pyarrow as pa

        array: pa.Array = value.data.arrow_array

        temp_table = pa.Table.from_arrays(arrays=[array], names=["array"])
        atw = ArrowTabularWrap(temp_table)
        result = atw.as_terminal_renderable(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
            show_table_header=False,
        )

        return result

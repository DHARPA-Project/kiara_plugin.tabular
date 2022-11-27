# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import Any, Mapping, Type, Union

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.models.values.value import SerializationResult, SerializedData, Value
from kiara.utils.output import ArrowTabularWrap

from kiara_plugin.tabular.data_types.array import store_array
from kiara_plugin.tabular.models.table import KiaraTable
from kiara_plugin.tabular.modules.table import EMPTY_COLUMN_NAME_MARKER


class TableType(AnyType[KiaraTable, DataTypeConfig]):
    """Tabular data (table, spreadsheet, data_frame, what have you).

    The table data is organized in sets of columns (arrays of data of the same type), with each column having a string identifier.

    *kiara* uses an instance of the [`KiaraTable`][kiara_plugin.tabular.models.table.KiaraTable]
    class to manage the table data, which let's developers access it in different formats ([Apache Arrow Table](https://arrow.apache.org/docs/python/generated/pyarrow.Table.html), [Pandas dataframe](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html), Python dict of lists, more to follow...).

    Please consult the API doc of the `KiaraTable` class for more information about how to access and query the data:

    - [`KiaraTable` API doc](https://dharpa.org/kiara_plugin.tabular/latest/reference/kiara_plugin/tabular/models/__init__/#kiara_plugin.tabular.models.table.KiaraTable)

    Internally, the data is stored in [Apache Feather format](https://arrow.apache.org/docs/python/feather.html) -- both
    in memory and on disk when saved, which enables some advanced usage to preserve memory and compute overhead.
    """

    _data_type_name = "table"

    @classmethod
    def python_class(cls) -> Type:
        return KiaraTable

    def parse_python_obj(self, data: Any) -> KiaraTable:

        return KiaraTable.create_table(data)

    # def calculate_hash(self, data: KiaraTable) -> CID:
    #     hashes = []
    #     for column_name in data.arrow_table.column_names:
    #         hashes.append(column_name)
    #         column = data.arrow_table.column(column_name)
    #         for chunk in column.chunks:
    #             for buf in chunk.buffers():
    #                 if not buf:
    #                     continue
    #                 h = hash_from_buffer(memoryview(buf))
    #                 hashes.append(h)
    #     return compute_cid(hashes)
    #     return KIARA_HASH_FUNCTION(memoryview(data.arrow_array))

    # def calculate_size(self, data: KiaraTable) -> int:
    #     return len(data.arrow_table)

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

    def pretty_print_as__terminal_renderable(
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

        half_lines: Union[int, None] = None
        if max_rows:
            half_lines = int(max_rows / 2)

        atw = ArrowTabularWrap(value.data.arrow_table)
        result = atw.as_terminal_renderable(
            rows_head=half_lines,
            rows_tail=half_lines,
            max_row_height=max_row_height,
            max_cell_length=max_cell_length,
        )
        return result

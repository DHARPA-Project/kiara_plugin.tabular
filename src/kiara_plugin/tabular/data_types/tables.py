# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, ClassVar, List, Mapping, Type, Union

from rich.console import Group

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.utils.output import ArrowTabularWrap
from kiara_plugin.tabular.data_types.array import store_array
from kiara_plugin.tabular.defaults import TABLE_COLUMN_SPLIT_MARKER
from kiara_plugin.tabular.models.tables import KiaraTables

try:
    from typing import Self  # type: ignore
except ImportError:
    from typing_extensions import Self  # type: ignore

if TYPE_CHECKING:
    from kiara.models.values.value import SerializedData, Value


class TablesType(AnyType[KiaraTables, DataTypeConfig]):
    """Multiple tabular data sets.

    The data is organized in sets of tables (which are sets of columns), each table having a string identifier.

    This is similar to the 'database' data type, the main difference being that 'database' is backed by sqlite, whereas 'tables' is backed by Apache Feather/Arrow. There is no hard rule when it's better to use which, but in general, if you need to access the datasets on a row-basis, 'database' is the better fit, for more column-based analytical queries, 'tables' is better.
    """

    _data_type_name: ClassVar[str] = "tables"

    @classmethod
    def python_class(cls) -> Type:
        return KiaraTables  # type: ignore

    def parse_python_obj(self, data: Any) -> KiaraTables:

        result: KiaraTables = KiaraTables.create_tables(data)
        return result

    def _validate(cls, value: Any) -> None:

        if not isinstance(value, KiaraTables):
            raise Exception(
                f"invalid type '{type(value).__name__}', must be 'KiaraTables'."
            )

    def serialize(self, data: Self) -> Union[None, str, "SerializedData"]:

        import pyarrow as pa

        for table_id, table in data.tables.items():
            if not table_id:
                raise Exception("table id must not be empty.")

            if TABLE_COLUMN_SPLIT_MARKER in table_id:
                raise Exception(
                    f"table id must not contain '{TABLE_COLUMN_SPLIT_MARKER}"
                )

        temp_f = tempfile.mkdtemp()

        def cleanup():
            shutil.rmtree(temp_f, ignore_errors=True)

        atexit.register(cleanup)

        chunk_map = {}

        for table_id, table in data.tables.items():
            arrow_table = table.arrow_table
            for column_name in arrow_table.column_names:
                if not column_name:
                    raise Exception(
                        f"column name for table '{table_id}' is empty. This is not allowed."
                    )

                column: pa.Array = arrow_table.column(column_name)
                file_name = os.path.join(temp_f, column_name)
                store_array(
                    array_obj=column, file_name=file_name, column_name=column_name
                )
                chunk_map[f"{table_id}{TABLE_COLUMN_SPLIT_MARKER}{column_name}"] = {
                    "type": "file",
                    "file": file_name,
                    "codec": "raw",
                }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.model_dump(),
            "data": chunk_map,
            "serialization_profile": "feather",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "load.tables",
                        "module_config": {
                            "value_type": "tables",
                            "target_profile": "python_object",
                            "serialization_profile": "feather",
                        },
                    }
                },
            },
        }

        from kiara.models.values.value import SerializationResult

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

        tables: KiaraTables = value.data

        result: List[Any] = [""]
        for table_name in tables.table_names:
            atw = ArrowTabularWrap(tables.get_table(table_name).arrow_table)

            pretty = atw.as_terminal_renderable(
                rows_head=half_lines,
                rows_tail=half_lines,
                max_row_height=max_row_height,
                max_cell_length=max_cell_length,
            )
            result.append(f"[b]Table[/b]: [i]{table_name}[/i]")
            result.append(pretty)

        return Group(*result)

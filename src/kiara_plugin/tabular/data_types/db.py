# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import Any, List, Mapping, Optional, Type

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG, KIARA_HASH_FUNCTION
from kiara.models.values.value import SerializationResult, SerializedData, Value
from kiara.utils.output import SqliteTabularWrap
from rich.console import Group

from kiara_plugin.tabular.models.db import KiaraDatabase


class DatabaseType(AnyType[KiaraDatabase, DataTypeConfig]):
    """A database, containing one or several tables.

    This is backed by a sqlite database file.
    """

    _data_type_name = "database"

    @classmethod
    def python_class(self) -> Type[KiaraDatabase]:
        return KiaraDatabase

    def calculate_size(self, data: KiaraDatabase) -> int:

        file_stats = os.stat(data.db_file_path)
        size = file_stats.st_size
        return size

    def calculate_hash(self, data: KiaraDatabase) -> int:
        return KIARA_HASH_FUNCTION(data.file_hash)

    def parse_python_obj(self, data: Any) -> KiaraDatabase:

        if isinstance(data, Path):
            data = data.as_posix()

        if isinstance(data, str):
            if not os.path.exists(data):
                raise ValueError(
                    f"Can't create database from path '{data}': path does not exist."
                )

            return KiaraDatabase(db_file_path=data)

        return data

    def _validate(cls, value: Any) -> None:

        if not isinstance(value, (KiaraDatabase)):
            raise ValueError(
                f"Invalid type '{type(value).__name__}', must be an instance of the 'KiaraDatabase' class."
            )

    def serialize(self, data: KiaraDatabase) -> SerializedData:

        chunks = {
            "db.sqlite": {"type": "file", "codec": "raw", "file": data.db_file_path}
        }

        serialized_data = {
            "data_type": self.data_type_name,
            "data_type_config": self.type_config.dict(),
            "data": chunks,
            "serialization_profile": "feather",
            "metadata": {
                "environment": {},
                "deserialize": {
                    "python_object": {
                        "module_type": "load.database",
                        "module_config": {
                            "value_type": "database",
                            "target_profile": "python_object",
                            "serialization_profile": "copy",
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

        db: KiaraDatabase = value.data

        result: List[Any] = [""]
        for table_name in db.table_names:
            atw = SqliteTabularWrap(
                engine=db.get_sqlalchemy_engine(), table_name=table_name
            )
            pretty = atw.pretty_print(
                rows_head=half_lines,
                rows_tail=half_lines,
                max_row_height=max_row_height,
                max_cell_length=max_cell_length,
            )
            result.append(f"[b]Table[/b]: [i]{table_name}[/i]")
            result.append(pretty)

        return Group(*result)

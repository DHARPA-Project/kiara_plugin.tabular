# -*- coding: utf-8 -*-
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Mapping, Type, Union

from kiara.data_types import DataTypeConfig
from kiara.data_types.included_core_types import AnyType
from kiara.defaults import DEFAULT_PRETTY_PRINT_CONFIG
from kiara.models.values.value import SerializationResult, SerializedData, Value
from kiara.utils.output import DictTabularWrap, TabularWrap
from rich.console import Group

from kiara_plugin.tabular.models.db import KiaraDatabase

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


class SqliteTabularWrap(TabularWrap):
    def __init__(self, engine: "Engine", table_name: str):
        self._engine: Engine = engine
        self._table_name: str = table_name
        super().__init__()

    def retrieve_number_of_rows(self) -> int:

        from sqlalchemy import text

        with self._engine.connect() as con:
            result = con.execute(text(f"SELECT count(*) from {self._table_name}"))
            num_rows = result.fetchone()[0]

        return num_rows

    def retrieve_column_names(self) -> Iterable[str]:

        from sqlalchemy import inspect

        engine = self._engine
        inspector = inspect(engine)
        columns = inspector.get_columns(self._table_name)
        result = [column["name"] for column in columns]
        return result

    def slice(self, offset: int = 0, length: Union[int, None] = None) -> "TabularWrap":

        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"
        if length:
            query = f"{query} LIMIT {length}"
        else:
            query = f"{query} LIMIT {self.num_rows}"
        if offset > 0:
            query = f"{query} OFFSET {offset}"
        with self._engine.connect() as con:
            result = con.execute(text(query))
            result_dict: Dict[str, List[Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return DictTabularWrap(result_dict)

    def to_pydict(self) -> Mapping:

        from sqlalchemy import text

        query = f"SELECT * FROM {self._table_name}"

        with self._engine.connect() as con:
            result = con.execute(text(query))
            result_dict: Dict[str, List[Any]] = {}
            for cn in self.column_names:
                result_dict[cn] = []
            for r in result:
                for i, cn in enumerate(self.column_names):
                    result_dict[cn].append(r[i])

        return result_dict


class DatabaseType(AnyType[KiaraDatabase, DataTypeConfig]):
    """A database, containing one or several tables.

    This is backed by the [KiaraDatabase][kiara_plugin.tabular.models.db.KiaraDatabase] class to manage
    the stored data.
    """

    _data_type_name = "database"

    @classmethod
    def python_class(self) -> Type[KiaraDatabase]:
        return KiaraDatabase

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
                            "value_type": self.data_type_name,
                            "target_profile": "python_object",
                            "serialization_profile": "copy",
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

        db: KiaraDatabase = value.data

        result: List[Any] = [""]
        for table_name in db.table_names:
            atw = SqliteTabularWrap(
                engine=db.get_sqlalchemy_engine(), table_name=table_name
            )
            pretty = atw.as_terminal_renderable(
                rows_head=half_lines,
                rows_tail=half_lines,
                max_row_height=max_row_height,
                max_cell_length=max_cell_length,
            )
            result.append(f"[b]Table[/b]: [i]{table_name}[/i]")
            result.append(pretty)

        return Group(*result)

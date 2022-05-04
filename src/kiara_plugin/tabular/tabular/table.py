# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Type

from kiara import KiaraModule
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import (
    FILE_BUNDLE_IMPORT_AVAILABLE_COLUMNS,
    FileBundle,
    FileModel,
)
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import SerializedData, Value, ValueMap
from kiara.modules import ValueSetSchema
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from pydantic import Field

from kiara_plugin.tabular.defaults import RESERVED_SQL_KEYWORDS
from kiara_plugin.tabular.models.table import KiaraArray, KiaraTable, KiaraTableMetadata

if TYPE_CHECKING:
    pass

EMPTY_COLUMN_NAME_MARKER = "__no_column_name__"


class CreateTableModuleConfig(CreateFromModuleConfig):

    ignore_errors: bool = Field(
        description="Whether to ignore convert errors and omit the failed items.",
        default=False,
    )


class CreateTableModule(CreateFromModule):

    _module_type_name = "create.table"
    _config_cls = CreateTableModuleConfig

    def create__table__from__csv_file(self, source_value: Value) -> Any:

        from pyarrow import csv

        input_file: FileModel = source_value.data
        imported_data = csv.read_csv(input_file.path)
        return imported_data

    def create__table__from__csv_file_bundle(self, source_value: Value) -> Any:

        import pyarrow as pa

        bundle: FileBundle = source_value.data

        columns = FILE_BUNDLE_IMPORT_AVAILABLE_COLUMNS

        ignore_errors = self.get_config_value("ignore_errors")
        file_dict = bundle.read_text_file_contents(ignore_errors=ignore_errors)

        # TODO: use chunks to save on memory
        tabular: Dict[str, List[Any]] = {}
        for column in columns:
            for index, rel_path in enumerate(sorted(file_dict.keys())):

                if column == "content":
                    _value: Any = file_dict[rel_path]
                elif column == "id":
                    _value = index
                elif column == "rel_path":
                    _value = rel_path
                else:
                    file_model = bundle.included_files[rel_path]
                    _value = getattr(file_model, column)

                tabular.setdefault(column, []).append(_value)

        table = pa.Table.from_pydict(tabular)
        return KiaraTable.create_table(table)


class DeserializeArrayModule(DeserializeValueModule):

    _module_type_name = "load.array"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": KiaraArray}

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "array"

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "feather"

    def to__python_object(self, data: SerializedData, **config: Any):

        assert "array.arrow" in data.get_keys() and len(list(data.get_keys())) == 1

        chunks = data.get_serialized_data("array.arrow")

        # TODO: support multiple chunks
        assert chunks.get_number_of_chunks() == 1
        files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
        assert len(files) == 1

        array_file = files[0]

        array = KiaraArray(data_path=array_file)
        return array


class DeserializeTableModule(DeserializeValueModule):

    _module_type_name = "load.table"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": KiaraTable}

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "table"

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "feather"

    def to__python_object(self, data: SerializedData, **config: Any):

        import pyarrow as pa

        columns = {}

        for column_name in data.get_keys():

            chunks = data.get_serialized_data(column_name)

            # TODO: support multiple chunks
            assert chunks.get_number_of_chunks() == 1
            files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
            assert len(files) == 1

            file = files[0]
            with pa.memory_map(file, "r") as column_chunk:
                loaded_arrays: pa.Table = pa.ipc.open_file(column_chunk).read_all()
                column = loaded_arrays.column(column_name)
                if column_name == EMPTY_COLUMN_NAME_MARKER:
                    columns[""] = column
                else:
                    columns[column_name] = column

        arrow_table = pa.table(columns)

        table = KiaraTable.create_table(arrow_table)
        return table


class CutColumnModule(KiaraModule):
    """Cut off one column from a table, returning an array."""

    _module_type_name = "table.cut_column"

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        inputs: Mapping[str, Any] = {
            "table": {"type": "table", "doc": "A table."},
            "column_name": {
                "type": "string",
                "doc": "The name of the column to extract.",
            },
        }
        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        outputs: Mapping[str, Any] = {"array": {"type": "array", "doc": "The column."}}
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        import pyarrow as pa

        column_name: str = inputs.get_value_data("column_name")

        table_value: Value = inputs.get_value_obj("table")
        table_metadata: KiaraTableMetadata = table_value.get_property_data(
            "metadata.table"
        )

        available = table_metadata.table.column_names

        if column_name not in available:
            raise KiaraProcessingException(
                f"Invalid column name '{column_name}'. Available column names: {', '.join(available)}"
            )

        table: pa.Table = table_value.data.arrow_table
        column = table.column(column_name)

        outputs.set_value("array", column)


class QueryTableSQLModuleConfig(KiaraModuleConfig):

    query: Optional[str] = Field(
        description="The query to execute. If not specified, the user will be able to provide their own.",
        default=None,
    )
    relation_name: Optional[str] = Field(
        description="The name the table is referred to in the sql query. If not specified, the user will be able to provide their own.",
        default="data",
    )


class QueryTableSQL(KiaraModule):
    """Execute a sql query against an (Arrow) table."""

    _module_type_name = "query.table"
    _config_cls = QueryTableSQLModuleConfig

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        inputs = {
            "table": {
                "type": "table",
                "doc": "The table to query",
            }
        }

        if self.get_config_value("query") is None:
            inputs["query"] = {"type": "string", "doc": "The query."}
            inputs["relation_name"] = {
                "type": "string",
                "doc": "The name the table is referred to in the sql query.",
                "default": "data",
            }

        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        return {"query_result": {"type": "table", "doc": "The query result."}}

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        import duckdb

        if self.get_config_value("query") is None:
            _query: str = inputs.get_value_data("query")
            _relation_name: str = inputs.get_value_data("relation_name")
        else:
            _query = self.get_config_value("query")
            _relation_name = self.get_config_value("relation_name")

        if _relation_name.upper() in RESERVED_SQL_KEYWORDS:
            raise KiaraProcessingException(
                f"Invalid relation name '{_relation_name}': this is a reserved sql keyword, please select a different name."
            )

        _table: KiaraTable = inputs.get_value_data("table")
        rel_from_arrow = duckdb.arrow(_table.arrow_table)
        result: duckdb.DuckDBPyResult = rel_from_arrow.query(_relation_name, _query)

        outputs.set_value("query_result", result.fetch_arrow_table())

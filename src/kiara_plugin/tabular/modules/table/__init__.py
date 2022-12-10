# -*- coding: utf-8 -*-
import os
from typing import Any, Dict, Iterable, List, Mapping, Type, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import (
    FILE_BUNDLE_IMPORT_AVAILABLE_COLUMNS,
    FileBundle,
    FileModel,
)
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.jobs import JobLog
from kiara.models.rendering import RenderScene, RenderValueResult
from kiara.models.values.value import SerializedData, Value, ValueMap
from kiara.models.values.value_schema import ValueSchema
from kiara.modules import KiaraModule, ValueMapSchema
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara.modules.included_core_modules.render_value import RenderValueModule
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from kiara.utils.output import ArrowTabularWrap
from pydantic import Field

from kiara_plugin.tabular.defaults import RESERVED_SQL_KEYWORDS
from kiara_plugin.tabular.models.array import KiaraArray
from kiara_plugin.tabular.models.table import KiaraTable, KiaraTableMetadata

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
        """Create a table from a csv_file value."""

        from pyarrow import csv

        input_file: FileModel = source_value.data
        imported_data = csv.read_csv(input_file.path)

        # import pandas as pd
        # df = pd.read_csv(input_file.path)
        # imported_data = pa.Table.from_pandas(df)

        return KiaraTable.create_table(imported_data)

    def create__table__from__text_file_bundle(self, source_value: Value) -> Any:
        """Create a table value from a text file_bundle.

        The resulting table will have (at a minimum) the following collumns:
        - id: an auto-assigned index
        - rel_path: the relative path of the file (from the provided base path)
        - content: the text file content
        """

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


class CutColumnModuleConfig(KiaraModuleConfig):
    """Configuration for the 'table.cut_column' kiara module.

    Technically this is not necessary, because we could just use the 'constants' field to
    set the 'column_name'. But this module is used in the documentation as example, as it's easy enough to understand,
    and I wanted to show how implement kiara module configuration.
    """

    column_name: Union[str, None] = Field(
        description="A hardcoded column name to cut.", default=None
    )


class CutColumnModule(KiaraModule):
    """Cut off one column from a table, returning an array."""

    _module_type_name = "table.cut_column"
    _config_cls = CutColumnModuleConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        inputs: Dict[str, Any] = {"table": {"type": "table", "doc": "A table."}}
        column_name = self.get_config_value("column_name")
        if not column_name:
            inputs["column_name"] = {
                "type": "string",
                "doc": "The name of the column to extract.",
            }

        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        outputs: Mapping[str, Any] = {"array": {"type": "array", "doc": "The column."}}
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        import pyarrow as pa

        column_name: Union[str, None] = self.get_config_value("column_name")
        if not column_name:
            column_name = inputs.get_value_data("column_name")

        if not column_name:
            raise KiaraProcessingException(
                "Could not cut column from table: column_name not provided or empty string."
            )

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


class MergeTableConfig(KiaraModuleConfig):

    inputs_schema: Dict[str, ValueSchema] = Field(
        description="A dict describing the inputs for this merge process."
    )
    column_map: Dict[str, str] = Field(
        description="A map describing", default_factory=dict
    )


class MergeTableModule(KiaraModule):
    """Create a table from other tables and/or arrays.

    This module needs configuration to be set (for now). It's currently not possible to merge an arbitrary
    number of tables/arrays, all tables to be merged must be specified in the module configuration.

    Column names of the resulting table can be controlled by the 'column_map' configuration, which takes the
    desired column name as key, and a field-name in the following format as value:
    - '[inputs_schema key]' for inputs of type 'array'
    - '[inputs_schema_key].orig_column_name' for inputs of type 'table'
    """

    _module_type_name = "table.merge"
    _config_cls = MergeTableConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        input_schema_dict = self.get_config_value("inputs_schema")
        return input_schema_dict

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        outputs = {
            "table": {
                "type": "table",
                "doc": "The merged table, including all source tables and columns.",
            }
        }
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap, job_log: JobLog) -> None:

        import pyarrow as pa

        inputs_schema: Dict[str, Any] = self.get_config_value("inputs_schema")
        column_map: Dict[str, str] = self.get_config_value("column_map")

        sources = {}
        for field_name in inputs_schema.keys():
            sources[field_name] = inputs.get_value_data(field_name)

        len_dict = {}
        arrays = {}

        column_map_final = dict(column_map)

        for source_key, table_or_array in sources.items():

            if isinstance(table_or_array, KiaraTable):
                rows = table_or_array.num_rows
                for name in table_or_array.column_names:
                    array_name = f"{source_key}.{name}"
                    if column_map and array_name not in column_map.values():
                        job_log.add_log(
                            f"Ignoring column '{name}' of input table '{source_key}': not listed in column_map."
                        )
                        continue

                    column = table_or_array.arrow_table.column(name)
                    arrays[array_name] = column
                    if not column_map:
                        if name in column_map_final:
                            raise Exception(
                                f"Can't merge table, duplicate column name: {name}."
                            )
                        column_map_final[name] = array_name

            elif isinstance(table_or_array, KiaraArray):

                if column_map and source_key not in column_map.values():
                    job_log.add_log(
                        f"Ignoring array '{source_key}': not listed in column_map."
                    )
                    continue

                rows = len(table_or_array)
                arrays[source_key] = table_or_array.arrow_array

                if not column_map:
                    if source_key in column_map_final.keys():
                        raise Exception(
                            f"Can't merge table, duplicate column name: {source_key}."
                        )
                    column_map_final[source_key] = source_key

            else:
                raise KiaraProcessingException(
                    f"Can't merge table: invalid type '{type(table_or_array)}' for source '{source_key}'."
                )

            len_dict[source_key] = rows

        all_rows = None
        for source_key, rows in len_dict.items():
            if all_rows is None:
                all_rows = rows
            else:
                if all_rows != rows:
                    all_rows = None
                    break

        if all_rows is None:
            len_str = ""
            for name, rows in len_dict.items():
                len_str = f" {name} ({rows})"

            raise KiaraProcessingException(
                f"Can't merge table, sources have different lengths: {len_str}"
            )

        column_names = []
        columns = []
        for column_name, ref in column_map_final.items():
            column_names.append(column_name)
            column = arrays[ref]
            columns.append(column)

        table = pa.Table.from_arrays(arrays=columns, names=column_names)

        outputs.set_value("table", table)


class QueryTableSQLModuleConfig(KiaraModuleConfig):

    query: Union[str, None] = Field(
        description="The query to execute. If not specified, the user will be able to provide their own.",
        default=None,
    )
    relation_name: Union[str, None] = Field(
        description="The name the table is referred to in the sql query. If not specified, the user will be able to provide their own.",
        default="data",
    )


class QueryTableSQL(KiaraModule):
    """Execute a sql query against an (Arrow) table.

    The default relation name for the sql query is 'data', but can be modified by the 'relation_name' config option/input.

    If the 'query' module config option is not set, users can provide their own query, otherwise the pre-set
    one will be used.
    """

    _module_type_name = "query.table"
    _config_cls = QueryTableSQLModuleConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

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
    ) -> ValueMapSchema:

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
        result: duckdb.DuckDBPyRelation = rel_from_arrow.query(_relation_name, _query)

        outputs.set_value("query_result", result.arrow())


class ExportTableModule(DataExportModule):
    """Export table data items."""

    _module_type_name = "export.table"

    def export__table__as__csv_file(self, value: KiaraTable, base_path: str, name: str):
        """Export a table as csv file."""

        import pyarrow.csv as csv

        target_path = os.path.join(base_path, f"{name}.csv")

        csv.write_csv(value.arrow_table, target_path)

        return {"files": target_path}

    # def export__table__as__sqlite_db(
    #     self, value: KiaraTable, base_path: str, name: str
    # ):
    #
    #     target_path = os.path.abspath(os.path.join(base_path, f"{name}.sqlite"))
    #
    #     raise NotImplementedError()
    #     # shutil.copy2(value.db_file_path, target_path)
    #
    #     return {"files": target_path}


class RenderTableModuleBase(RenderValueModule):

    _module_type_name: str = None  # type: ignore

    def preprocess_table(
        self, value: Value, input_number_of_rows: int, input_row_offset: int
    ):

        import duckdb
        import pyarrow as pa

        if value.data_type_name == "array":
            array: KiaraArray = value.data
            arrow_table = pa.table(data=[array.arrow_array], names=["array"])
            column_names: Iterable[str] = ["array"]
        else:
            table: KiaraTable = value.data
            arrow_table = table.arrow_table
            column_names = table.column_names

        columnns = [f'"{x}"' if not x.startswith('"') else x for x in column_names]

        query = f"""SELECT {', '.join(columnns)} FROM data LIMIT {input_number_of_rows} OFFSET {input_row_offset}"""

        rel_from_arrow = duckdb.arrow(arrow_table)
        query_result: duckdb.DuckDBPyRelation = rel_from_arrow.query("data", query)

        result_table = query_result.arrow()
        wrap = ArrowTabularWrap(table=result_table)

        related_scenes: Dict[str, Union[None, RenderScene]] = {}

        row_offset = arrow_table.num_rows - input_number_of_rows

        if row_offset > 0:

            if input_row_offset > 0:
                related_scenes["first"] = RenderScene.construct(
                    title="first",
                    description=f"Display the first {input_number_of_rows} rows of this table.",
                    manifest_hash=self.manifest.manifest_hash,
                    render_config={
                        "row_offset": 0,
                        "number_of_rows": input_number_of_rows,
                    },
                )

                p_offset = input_row_offset - input_number_of_rows
                if p_offset < 0:
                    p_offset = 0
                previous = {
                    "row_offset": p_offset,
                    "number_of_rows": input_number_of_rows,
                }
                related_scenes["previous"] = RenderScene.construct(title="previous", description=f"Display the previous {input_number_of_rows} rows of this table.", manifest_hash=self.manifest.manifest_hash, render_config=previous)  # type: ignore
            else:
                related_scenes["first"] = None
                related_scenes["previous"] = None

            n_offset = input_row_offset + input_number_of_rows
            if n_offset < arrow_table.num_rows:
                next = {"row_offset": n_offset, "number_of_rows": input_number_of_rows}
                related_scenes["next"] = RenderScene.construct(title="next", description=f"Display the next {input_number_of_rows} rows of this table.", manifest_hash=self.manifest.manifest_hash, render_config=next)  # type: ignore
            else:
                related_scenes["next"] = None

            last_page = int(arrow_table.num_rows / input_number_of_rows)
            current_start = last_page * input_number_of_rows
            if (input_row_offset + input_number_of_rows) > arrow_table.num_rows:
                related_scenes["last"] = None
            else:
                related_scenes["last"] = RenderScene.construct(
                    title="last",
                    description="Display the final rows of this table.",
                    manifest_hash=self.manifest.manifest_hash,
                    render_config={
                        "row_offset": current_start,  # type: ignore
                        "number_of_rows": input_number_of_rows,  # type: ignore
                    },
                )
        else:
            related_scenes["first"] = None
            related_scenes["previous"] = None
            related_scenes["next"] = None
            related_scenes["last"] = None

        return wrap, related_scenes


class RenderTableModule(RenderTableModuleBase):
    _module_type_name = "render.table"

    def render__table__as__string(self, value: Value, render_config: Mapping[str, Any]):

        input_number_of_rows = render_config.get("number_of_rows", 20)
        input_row_offset = render_config.get("row_offset", 0)

        wrap, data_related_scenes = self.preprocess_table(
            value=value,
            input_number_of_rows=input_number_of_rows,
            input_row_offset=input_row_offset,
        )
        pretty = wrap.as_string(max_row_height=1)

        return RenderValueResult(
            value_id=value.value_id,
            render_config=render_config,
            render_manifest=self.manifest.manifest_hash,
            rendered=pretty,
            related_scenes=data_related_scenes,
        )

    def render__table__as__terminal_renderable(
        self, value: Value, render_config: Mapping[str, Any]
    ):

        input_number_of_rows = render_config.get("number_of_rows", 20)
        input_row_offset = render_config.get("row_offset", 0)

        wrap, data_related_scenes = self.preprocess_table(
            value=value,
            input_number_of_rows=input_number_of_rows,
            input_row_offset=input_row_offset,
        )
        pretty = wrap.as_terminal_renderable(max_row_height=1)

        return RenderValueResult(
            value_id=value.value_id,
            render_config=render_config,
            render_manifest=self.manifest.manifest_hash,
            rendered=pretty,
            related_scenes=data_related_scenes,
        )

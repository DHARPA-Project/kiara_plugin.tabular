# -*- coding: utf-8 -*-
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generator,
    Iterable,
    List,
    Mapping,
    Type,
    Union,
)

from pydantic import BaseModel, Field

from kiara.exceptions import KiaraException, KiaraProcessingException
from kiara.models.filesystem import (
    FILE_BUNDLE_IMPORT_AVAILABLE_COLUMNS,
    KiaraFile,
    KiaraFileBundle,
)
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.jobs import JobLog
from kiara.models.rendering import RenderScene, RenderValueResult
from kiara.models.values.value import SerializedData, Value, ValueMap
from kiara.modules import KiaraModule, ValueMapSchema
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.modules.included_core_modules.export_as import DataExportModule
from kiara.modules.included_core_modules.render_value import RenderValueModule
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from kiara.utils import log_message
from kiara.utils.output import ArrowTabularWrap
from kiara_plugin.tabular.defaults import (
    RESERVED_SQL_KEYWORDS,
    TABLE_SCHEMA_CHUNKS_NAME,
)
from kiara_plugin.tabular.models.array import KiaraArray
from kiara_plugin.tabular.models.table import KiaraTable

if TYPE_CHECKING:
    from multiformats.varint import BytesLike

EMPTY_COLUMN_NAME_MARKER = "__no_column_name__"


class CreateTableModuleConfig(CreateFromModuleConfig):
    ignore_errors: bool = Field(
        description="Whether to ignore convert errors and omit the failed items.",
        default=False,
    )


class CreateTableModule(CreateFromModule):
    """Create a table from supported source input types."""

    _module_type_name = "create.table"
    _config_cls = CreateTableModuleConfig

    def create_optional_inputs(
        self, source_type: str, target_type
    ) -> Union[Mapping[str, Mapping[str, Any]], None]:
        if source_type == "file":
            return {
                "first_row_is_header": {
                    "type": "boolean",
                    "optional": True,
                    "doc": "Whether the first row of a (csv) file is a header row. If not provided, kiara will try to auto-determine. Ignored if not a csv file.",
                },
                "delimiter": {
                    "type": "string",
                    "optional": True,
                    "doc": "The delimiter that is used in the csv file. If not provided, kiara will try to auto-determine. Ignored if not a csv file.",
                },
            }

        return None

    def create__table__from__file(self, source_value: Value, optional: ValueMap) -> Any:
        """Create a table from a file, trying to auto-determine the format of said file.

        Currently supported input file types:

        - csv
        - parquet
        """

        input_file: KiaraFile = source_value.data

        if input_file.file_name.endswith(".csv"):
            return self.import_csv_file(source_value, optional)
        elif input_file.file_name.endswith(".parquet"):
            return self.import_parquet_file(source_value, optional)

    def import_parquet_file(
        self, source_value: Value, optional: ValueMap
    ) -> KiaraTable:
        """Create a table from a parquet file value."""

        import pyarrow.parquet as pq

        # TODO: use memory mapping to optimize memory usage?

        input_file: KiaraFile = source_value.data
        imported_data = None
        errors = []

        try:
            imported_data = pq.read_table(input_file.path)
        except Exception as e:
            errors.append(e)

        if imported_data is None:
            raise KiaraProcessingException(
                f"Failed to import parquet file '{input_file.path}'."
            )

        return KiaraTable.create_table(imported_data)

    def import_csv_file(self, source_value: Value, optional: ValueMap) -> KiaraTable:
        """Create a table from a csv file value."""
        import csv as py_csv

        from pyarrow import csv

        input_file: KiaraFile = source_value.data
        imported_data = None
        errors = []

        has_header = optional.get_value_data("first_row_is_header")
        delimiter = optional.get_value_data("delimiter")

        if has_header is None:
            try:
                has_header = True

                with open(input_file.path, "rt") as csvfile:
                    sniffer = py_csv.Sniffer()
                    has_header = sniffer.has_header(csvfile.read(2048))
                    csvfile.seek(0)
            except Exception as e:
                # TODO: add this to the procss log
                log_message(
                    "csv_sniffer.error",
                    file=input_file.path,
                    error=str(e),
                    details="assuming csv file has header",
                )

        if delimiter is None:
            try:
                delimiter = ","
                with open(input_file.path, "rt") as csvfile:
                    sniffer = py_csv.Sniffer()
                    dialect = sniffer.sniff(csvfile.read(2048))
                    delimiter = dialect.delimiter
                    csvfile.seek(0)
            except Exception as e:
                # TODO: add this to the procss log
                log_message(
                    "csv_sniffer.error",
                    file=input_file.path,
                    error=str(e),
                    details="assuming csv file delimiter is ','",
                )

        try:
            parse_options = csv.ParseOptions(delimiter=delimiter)
            if has_header:
                imported_data = csv.read_csv(
                    input_file.path, parse_options=parse_options
                )
            else:
                read_options = csv.ReadOptions(autogenerate_column_names=True)
                imported_data = csv.read_csv(
                    input_file.path,
                    read_options=read_options,
                    parse_options=parse_options,
                )
        except Exception as e:
            errors.append(e)

        if imported_data is None:
            msg = ""
            for err in errors:
                msg += f"{err}\n"
            raise KiaraProcessingException(f"Failed to import csv file: {msg}'.")

        # import pandas as pd
        # df = pd.read_csv(input_file.path)
        # imported_data = pa.Table.from_pandas(df)

        return KiaraTable.create_table(imported_data)

    # def create__table__from__csv_file(self, source_value: Value) -> Any:
    #     """Create a table from a csv_file value."""
    #
    #     from pyarrow import csv
    #
    #     input_file: FileModel = source_value.data
    #     imported_data = csv.read_csv(input_file.path)
    #
    #     # import pandas as pd
    #     # df = pd.read_csv(input_file.path)
    #     # imported_data = pa.Table.from_pandas(df)
    #
    #     return KiaraTable.create_table(imported_data)

    def create__table__from__file_bundle(self, source_value: Value) -> Any:
        """Create a table value from a text file_bundle.

        The resulting table will have (at a minimum) the following columns:
        - id: an auto-assigned index
        - rel_path: the relative path of the file (from the provided base path)
        - content: the text file content
        """

        import pyarrow as pa

        bundle: KiaraFileBundle = source_value.data

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

        table_schema_chunks = data.get_serialized_data(TABLE_SCHEMA_CHUNKS_NAME)
        chunks_generator: Generator["BytesLike", None, None] = (
            table_schema_chunks.get_chunks(as_files=False)  # type: ignore
        )  # type: ignore
        schema_chunk = next(chunks_generator)
        schema = pa.ipc.read_schema(pa.py_buffer(schema_chunk))

        for column_name in data.get_keys():
            if column_name == TABLE_SCHEMA_CHUNKS_NAME:
                continue

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

        arrow_table = pa.table(columns, schema=schema)

        table = KiaraTable.create_table(arrow_table)
        return table


class PickColumnModuleConfig(KiaraModuleConfig):
    """Configuration for the 'table.cut_column' kiara module.

    Technically this is not necessary, because we could just use the 'constants' field to
    set the 'column_name'. But this module is used in the documentation as example, as it's easy enough to understand,
    and I wanted to show how implement kiara module configuration.
    """

    column_name: Union[str, None] = Field(
        description="A hardcoded column name to cut.", default=None
    )


class PickColumnModule(KiaraModule):
    """Pick one column from a table, returning an array."""

    _module_type_name = "table.pick.column"
    _config_cls = PickColumnModuleConfig

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

        table_instance: KiaraTable = table_value.data

        if column_name not in table_instance.column_names:
            raise KiaraProcessingException(
                f"Invalid column name '{column_name}'. Available column names: {', '.join(table_instance.column_names)}"
            )

        table: pa.Table = table_value.data.arrow_table
        column = table.column(column_name)

        outputs.set_value("array", column)


class ValueSchemaInput(BaseModel):
    """
    The schema of a value.

    This is s simplified version of the default [ValueSchema][kiara.models.values.value_schema.ValueSchema] model,
    """

    type: str = Field(description="The type of the value.")
    type_config: Dict[str, Any] = Field(
        description="Configuration for the type, in case it's complex.",
        default_factory=dict,
    )
    default: Any = Field(description="A default value.", default=None)
    doc: str = Field(description="A description for the value of this input field.")

    # optional: bool = Field(
    #     description="Whether this value is required (True), or whether 'None' value is allowed (False).",
    #     default=False,
    # )


class MergeTableConfig(KiaraModuleConfig):
    inputs_schema: Dict[str, ValueSchemaInput] = Field(
        description="A dict describing the inputs for this merge process."
    )
    column_map: Dict[str, str] = Field(
        description="A dict describing how to map column names from inputs to the output table.",
        default_factory=dict,
    )
    # ask_column_map: bool = Field(
    #     description="Whether to ask the user for a column map.", default=False
    # )


class MergeTableModule(KiaraModule):
    """Create a table from other tables and/or arrays.

    This module is a general purpose module to merge arrays and tables into a table data item. It does not have a no-config operation, meaning it needs to be configured before it can be used.

    It does include one module config by default thought, which results in the operation `table.add_column', which takes a table and a single array, and adds the specified array as new column to the (also) provided input table.

    Otherwise it is not possible to merge an arbitrary number of tables/arrays, the number of tables or arrays to be merged must always be specified in the module configuration.

    Note: this module might still be re-worked a bit, because it's not straight-forward how to use tables here, since it's not possible to forsee the column names user-provided input tables will have. For now, the 'table.add_column' is the only use-case for this module, but I'd like it to be more generally useful in the future, for all kinds of table assembly tasks.

    ## Module configuration options

    ### `inputs_schema`

    This is a dict describing which tables/arrays need to be provided by the user. The format is the same as
    what is used in the `create_inputs_schema` method of a `KiaraModule` (apart from 'optional', which is not allowed here), e.g.:

    ```
        inputs_schema: {
          "table1": {
             "type": "table",
             "doc": "The first table to merge.",
          },
          "table2": {
            "type": "table",
            "doc": "The second table to merge.",
          },
          "array": {
            "type": "array",
            "doc": "An array to add as column.",
          }
    }
    ```

    ### `column_map`

    Column names of the resulting table can be controlled by the 'column_map' configuration, which takes the
    desired column name as value, and a field-name in the following format as key:
    - '[inputs_schema key]' for inputs of type 'array'
    - '[inputs_schema_key].orig_column_name' for inputs of type 'table'

    Additionally, the 'column_map' can also contain the special value 'input:[inputs_field_name]', which will prompt the resulting operation to have an additional input field with the name '[inputs_field_name]',  which will be used as the new column name of the referenced input field key.

    Example:

    ```
        column_map: {
            "array": "input:array_column_name"
        }
    ```

    This would result in a module with 4 inputs ('table1', 'table2', "array", and 'array_column_name'), and the 'array_column_name' input would be used as the column name for the 'array' input, and the column names of the 2 tables would be used as-is, with an exception thrown if there is a duplicate column name.

    For known column names in a table, a key/value pair might look like:

    ```
        column_map: {
           "table1.first_name": "first_name"
        }
    ```

    """

    @classmethod
    def retrieve_included_operations(cls):
        return {
            "table.add_column": {
                "doc": """Add a column to a table.

This module takes a table and an array, and adds the array as a new column to the end table. The table and array must have the same number of rows for this to work.""",
                "module_config": {
                    "inputs_schema": {
                        "table": {
                            "type": "table",
                            "doc": "The table to add the column to.",
                        },
                        "array": {
                            "type": "array",
                            "doc": "The column to add.",
                        },
                    },
                    "column_map": {"array": "input:column_name"},
                },
            }
        }

    _module_type_name = "table.merge"
    _config_cls = MergeTableConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:
        input_schema_models = self.get_config_value("inputs_schema")

        input_schema_dict = {}
        for k, v in input_schema_models.items():
            if k == "column_map":
                raise KiaraException(
                    "Invalid input schema for 'table.merge' module: 'column_map' is a reserved keyword."
                )
            input_schema_dict[k] = v.model_dump()

        # if self.get_config_value("ask_column_map"):
        #     input_schema_dict["column_map"] = {
        #         "type": "dict",
        #         "doc": "A dict describing how to map column names from inputs to the output table.",
        #     }

        for k, v in self.get_config_value("column_map").items():
            if v.startswith("input:"):
                input_name = v[6:]
                if input_name in input_schema_dict.keys():
                    raise KiaraException(
                        "Can't use input field with name '{input_name}': already used for table or array input."
                    )
                input_schema_dict[input_name] = {
                    "type": "string",
                    "doc": f"The column-name for the input '{input_name}'.",
                }

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

        # first we need to assemble the final column map, in case there was user input
        final_column_map = {}
        config_column_map = self.get_config_value("column_map")
        for k, v in config_column_map.items():
            if v.startswith("input:"):
                input_name = v[6:]
                final_column_map[k] = inputs.get_value_data(input_name)
            else:
                final_column_map[k] = v

        inputs_schema: Dict[str, ValueSchemaInput] = self.get_config_value(
            "inputs_schema"
        )

        sources: Dict[str, pa.Array] = {}
        lengths_dict: Dict[int, List[str]] = {}
        column_map_sources = {}
        column_order = []

        for field_name, schema in inputs_schema.items():
            if schema.type == "array":
                kiara_array_data: KiaraArray = inputs.get_value_data(field_name)
                array_data = kiara_array_data.arrow_array
                no_rows = array_data.length()
                sources[field_name] = array_data
                lengths_dict.setdefault(no_rows, []).append(field_name)
                column_map_sources[field_name] = field_name
                column_order.append(field_name)
            elif schema.type == "table":
                table_data: KiaraTable = inputs.get_value_data(field_name)
                arrow_table = table_data.arrow_table
                for column_name in arrow_table.column_names:
                    arrow_array = arrow_table.column(column_name)
                    name = f"{field_name}.{column_name}"
                    no_rows = arrow_array.length()
                    sources[name] = arrow_array
                    lengths_dict.setdefault(no_rows, []).append(name)
                    column_map_sources[name] = column_name
                    column_order.append(name)

        if len(lengths_dict) > 1:
            lengths_str = ", ".join((str(x) for x in lengths_dict.keys()))
            raise KiaraProcessingException(
                f"Can't merge table, sources have different lengths: {lengths_str}"
            )

        column_names = []
        columns = []

        for k in final_column_map.keys():
            if k not in column_map_sources.keys():
                raise KiaraProcessingException(
                    f"Invalid column map key '{k}': not in sources."
                )

        for column_ref, column_name in column_map_sources.items():
            if column_ref in final_column_map.keys():
                continue

            final_column_map[column_ref] = column_name

        for column_ref in column_order:
            column_name = final_column_map[column_ref]
            if column_name in column_names:
                raise KiaraProcessingException(
                    f"Can't merge table: duplicate column name '{column_name}'."
                )
            column_names.append(column_name)
            column = sources[column_ref]
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
            inputs["query"] = {
                "type": "string",
                "doc": "The query, use the value of the 'relation_name' input as table, e.g. 'select * from data'.",
            }
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

        from pyarrow import csv

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

        query = f"""SELECT {", ".join(columnns)} FROM data LIMIT {input_number_of_rows} OFFSET {input_row_offset}"""

        rel_from_arrow = duckdb.arrow(arrow_table)
        query_result: duckdb.DuckDBPyRelation = rel_from_arrow.query("data", query)

        result_table = query_result.arrow()
        wrap = ArrowTabularWrap(table=result_table)

        related_scenes: Dict[str, Union[None, RenderScene]] = {}

        row_offset = arrow_table.num_rows - input_number_of_rows

        if row_offset > 0:
            if input_row_offset > 0:
                related_scenes["first"] = RenderScene.model_construct(
                    title="first",
                    description=f"Display the first {input_number_of_rows} rows of this table.",
                    manifest_hash=self.manifest.manifest_hash,
                    render_config={
                        "row_offset": 0,
                        "number_of_rows": input_number_of_rows,
                    },
                )

                p_offset = input_row_offset - input_number_of_rows

                p_offset = max(p_offset, 0)

                previous = {
                    "row_offset": p_offset,
                    "number_of_rows": input_number_of_rows,
                }
                related_scenes["previous"] = RenderScene.model_construct(
                    title="previous",
                    description=f"Display the previous {input_number_of_rows} rows of this table.",
                    manifest_hash=self.manifest.manifest_hash,
                    render_config=previous,
                )  # type: ignore
            else:
                related_scenes["first"] = None
                related_scenes["previous"] = None

            n_offset = input_row_offset + input_number_of_rows
            if n_offset < arrow_table.num_rows:
                next = {"row_offset": n_offset, "number_of_rows": input_number_of_rows}
                related_scenes["next"] = RenderScene.model_construct(
                    title="next",
                    description=f"Display the next {input_number_of_rows} rows of this table.",
                    manifest_hash=self.manifest.manifest_hash,
                    render_config=next,
                )  # type: ignore
            else:
                related_scenes["next"] = None

            last_page = int(arrow_table.num_rows / input_number_of_rows)
            current_start = last_page * input_number_of_rows
            if (input_row_offset + input_number_of_rows) > arrow_table.num_rows:
                related_scenes["last"] = None
            else:
                related_scenes["last"] = RenderScene.model_construct(
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

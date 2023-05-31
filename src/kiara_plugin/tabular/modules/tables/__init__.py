# -*- coding: utf-8 -*-
import functools
import logging
from typing import Any, Dict, List, Mapping, Tuple, Type, Union

from pydantic import Field

from kiara.exceptions import KiaraException, KiaraProcessingException
from kiara.models.filesystem import FileBundle
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.jobs import JobLog
from kiara.models.values.value import SerializedData, Value, ValueMap
from kiara.modules import KiaraModule, ValueMapSchema
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from kiara.utils import find_free_id, log_message
from kiara_plugin.tabular.defaults import TABLE_COLUMN_SPLIT_MARKER
from kiara_plugin.tabular.models.tables import KiaraTables
from kiara_plugin.tabular.utils import create_table_from_file_bundle


class DeserializeTableModule(DeserializeValueModule):

    _module_type_name = "load.tables"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": KiaraTables}

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "tables"

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "feather"

    def to__python_object(self, data: SerializedData, **config: Any):

        import pyarrow as pa

        tables: Dict[str, Any] = {}

        for column_id in data.get_keys():

            if TABLE_COLUMN_SPLIT_MARKER not in column_id:
                raise KiaraException(
                    f"Invalid serialized 'tables' data, key must contain '{TABLE_COLUMN_SPLIT_MARKER}': {column_id}"
                )
            table_id, column_name = column_id.split(
                TABLE_COLUMN_SPLIT_MARKER, maxsplit=1
            )

            chunks = data.get_serialized_data(column_id)

            # TODO: support multiple chunks
            assert chunks.get_number_of_chunks() == 1
            files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
            assert len(files) == 1

            file = files[0]
            with pa.memory_map(file, "r") as column_chunk:
                loaded_arrays: pa.Table = pa.ipc.open_file(column_chunk).read_all()
                column = loaded_arrays.column(column_name)
                tables.setdefault(table_id, {})[column_name] = column

        table = KiaraTables.create_tables(tables)
        return table


class CreateTablesModuleConfig(CreateFromModuleConfig):

    ignore_errors: bool = Field(
        description="Whether to ignore convert errors and omit the failed items.",
        default=False,
    )
    # merge_into_single_table: bool = Field(
    #     description="Whether to merge all csv files into a single table.", default=False
    # )
    include_source_metadata: Union[bool, None] = Field(
        description="Whether to include a table with metadata about the source files.",
        default=None,
    )
    include_source_file_content: bool = Field(
        description="When including source metadata, whether to also include the original raw (string) content.",
        default=False,
    )


class CreateDatabaseModule(CreateFromModule):

    _module_type_name = "create.tables"
    _config_cls = CreateTablesModuleConfig

    def create__tables__from__file_bundle(
        self, source_value: Value, job_log: JobLog
    ) -> Any:
        """Create a database from a file_bundle value.

        Currently, only csv files are supported, files in the source file_bundle that have different extensions will be ignored.

        Unless 'merge_into_single_table' is set to 'True' in the module configuration, each csv file will create one table
        in the resulting database. If this option is set, only a single table with all the values of all
        csv files will be created. For this to work, all csv files should follow the same schema.

        """

        from pyarrow import csv as pa_csv

        include_raw_content_in_file_info: Union[bool, None] = self.get_config_value(
            "include_source_metadata"
        )

        tables = {}

        bundle: FileBundle = source_value.data

        table_names: List[str] = []
        included_files: Dict[str, bool] = {}
        errors: Dict[str, Union[None, str]] = {}
        for rel_path in sorted(bundle.included_files.keys()):

            if not rel_path.endswith(".csv"):
                job_log.add_log(
                    f"Ignoring file (not csv): {rel_path}", log_level=logging.INFO
                )
                included_files[rel_path] = False
                errors[rel_path] = "Not a csv file."
                continue

            file_item = bundle.included_files[rel_path]
            table_name = find_free_id(
                stem=file_item.file_name_without_extension, current_ids=table_names
            )
            try:
                table_names.append(table_name)
                table = pa_csv.read_csv(file_item.path)
                tables[table_name] = table
                included_files[rel_path] = True
            except Exception as e:
                included_files[rel_path] = False
                errors[rel_path] = KiaraException.get_root_details(e)

                if self.get_config_value("ignore_errors") is True or True:
                    log_message("ignore.import_file", file=rel_path, reason=str(e))
                    continue

                raise KiaraProcessingException(e)

        if include_raw_content_in_file_info in [None, True]:
            include_content: bool = self.get_config_value("include_source_file_content")

            if "file_items" in tables:
                raise KiaraProcessingException(
                    "Can't create table: 'file_items' columns already exists."
                )

            table = create_table_from_file_bundle(
                file_bundle=source_value.data,
                include_content=include_content,
                included_files=included_files,
                errors=errors,
            )
            tables["file_items"] = table

        return tables


class AssembleTablesConfig(KiaraModuleConfig):
    """Configuration for the 'assemble.tables' module."""

    number_of_tables: Union[int, None] = Field(
        description="How many tables should be merged. If 'table_names' is empty, this defaults to '2', otherwise the length of the 'table_names' input.",
        default=None,
    )
    table_names: Union[List[str], None] = Field(
        description="A pre-defined list of table names. If not defined, users will be asked for the table name(s).",
        default=None,
    )


class AssembleTablesModule(KiaraModule):
    """Assemble a 'tables' value from multiple tables.

    Depending on the module configuration, 2 or more tables can be merged into a single 'tables' value.

    """

    _module_type_name = "assemble.tables"
    _config_cls = AssembleTablesConfig

    @functools.cached_property
    def _table_details(self) -> Tuple[int, Union[List[str], None]]:

        number_tables: Union[int, None] = self.get_config_value("number_of_tables")
        table_names: Union[None, List[str]] = self.get_config_value("table_names")

        if not table_names:
            if not number_tables:
                number_tables = 2
        else:
            if not number_tables:
                number_tables = len(table_names)
            else:
                if not number_tables == len(table_names):
                    raise KiaraException(
                        "The 'number_of_tables' and length of 'table_names' config option must match."
                    )

        if number_tables < 2:
            raise KiaraException("The 'number_of_tables' must be at least 2.")

        return number_tables, table_names

    @property
    def number_of_tables(self) -> int:
        number_tables, _ = self._table_details
        return number_tables

    @property
    def table_names(self) -> Union[List[str], None]:
        _, table_names = self._table_details
        return table_names

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        number_tables = self.number_of_tables
        table_names = self.table_names

        if not table_names:
            if not number_tables:
                number_tables = 2
        else:
            if not number_tables:
                number_tables = len(table_names)
            else:
                if not number_tables == len(table_names):
                    raise KiaraException(
                        "The 'number_of_tables' and length of 'table_names' config option must match."
                    )

        if number_tables < 2:
            raise KiaraException("The 'number_of_tables' must be at least 2.")

        inputs_schema = {}
        if not table_names:
            for i in range(1, number_tables + 1):
                inputs_schema[f"table_name_{i}"] = {
                    "type": "string",
                    "doc": f"The alias for table #{i}.",
                }
                inputs_schema[f"table_{i}"] = {
                    "type": "table",
                    "doc": f"The table to merge (#{i}).",
                }
        else:
            for table_name in table_names:
                inputs_schema[f"table_{table_name}"] = {
                    "type": "table",
                    "doc": f"The table to merge for alias '{table_name}'.",
                }

        return inputs_schema

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

        outputs = {
            "tables": {
                "type": "tables",
                "doc": "The assembled tables instance.",
            }
        }
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap, job_log: JobLog) -> None:

        number_tables = self.number_of_tables
        table_names = self.table_names

        tables: Dict[str, Any] = {}
        if not table_names:
            for i in range(1, number_tables + 1):
                table_name = inputs.get_value_data(f"table_name_{i}")
                table = inputs.get_value_obj(f"table_{i}")
                if table_name in tables.keys():
                    raise KiaraException(f"Duplicate table name: '{table_name}'")
                tables[table_name] = table
        else:
            for table_name in table_names:
                table = inputs.get_value_obj(f"table_{table_name}")
                tables[table_name] = table

        outputs.set_value("tables", tables)

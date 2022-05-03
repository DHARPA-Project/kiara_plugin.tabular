# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Type

from kiara import KiaraModule
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import (
    FILE_BUNDLE_IMPORT_AVAILABLE_COLUMNS,
    FileBundle,
    FileModel,
)
from kiara.models.values.value import SerializedData, Value, ValueMap
from kiara.modules import ValueSetSchema
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from pydantic import Field

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


# class LoadTableConfig(KiaraModuleConfig):
#
#     only_column: Optional[str] = Field(
#         description="Whether to only load a single column instead of the whole table.",
#         default=None,
#     )
#
#
# class LoadTableFromDiskModule(KiaraModule):
#
#     _module_type_name = "load.table"
#     _config_cls = LoadTableConfig
#
#     def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
#         return ModuleCharacteristics(is_internal=True)
#
#     def create_inputs_schema(
#         self,
#     ) -> ValueSetSchema:
#
#         inputs = {"bytes_structure": {"type": "any", "doc": "The bytes."}}
#         return inputs
#
#     def create_outputs_schema(
#         self,
#     ) -> ValueSetSchema:
#
#         if not self.get_config_value("only_column"):
#             return {"table": {"type": "table", "doc": "The table."}}
#         else:
#             return {"array": {"type": "array", "doc": "The array."}}
#
#     def process(self, inputs: ValueMap, outputs: ValueMap):
#
#         import pyarrow as pa
#
#         bytes_structure: BytesStructure = inputs.get_value_data("bytes_structure")
#
#         if not self.get_config_value("only_column"):
#             columns = {}
#
#             for column_name, chunks in bytes_structure.chunk_map.items():
#                 assert len(chunks) == 1
#                 with pa.memory_map(chunks[0], "r") as column_chunk:
#                     loaded_arrays: pa.Table = pa.ipc.open_file(column_chunk).read_all()
#                     column = loaded_arrays.column(column_name)
#                     if column_name == EMPTY_COLUMN_NAME_MARKER:
#                         columns[""] = column
#                     else:
#                         columns[column_name] = column
#
#             arrow_table = pa.table(columns)
#
#             table = KiaraTable.create_table(arrow_table)
#             outputs.set_value("table", table)
#         else:
#             chunks = bytes_structure.chunk_map["array.arrow"]
#             assert len(chunks) == 1
#
#             array_file = chunks[0]
#             # with pa.memory_map(array_file, "r") as column_chunk:
#             #     loaded_arrays = pa.ipc.open_file(column_chunk).read_all()
#             #     column = loaded_arrays.column("array")
#             #
#             # array = KiaraArray.create_array(column)
#
#             array = KiaraArray(data_path=array_file)
#             outputs.set_value("array", array)


# class SaveTableToDiskModule(PersistValueModule):
#
#     _module_type_name = "table.save_to.disk.as.feather"
#
#     def get_persistence_target_name(self) -> str:
#         return "disk"
#
#     def get_persistence_format_name(self) -> str:
#         return "arrays"
#
#     def data_type__array(self, value: Value, persistence_config: Mapping[str, Any]):
#
#         import pyarrow as pa
#
#         kiara_array: KiaraArray = value.data
#
#         chunk_map = {}
#
#         # TODO: make sure temp dir is in the same partition as file store
#         temp_f = tempfile.mkdtemp()
#
#         def cleanup():
#             shutil.rmtree(temp_f, ignore_errors=True)
#
#         atexit.register(cleanup)
#
#         column: pa.Array = kiara_array.arrow_array
#         file_name = os.path.join(temp_f, "array.arrow")
#         self._store_array(array_obj=column, file_name=file_name, column_name="array")
#         chunk_map["array.arrow"] = [file_name]
#
#         bytes_structure_data: Dict[str, Any] = {
#             "data_type": value.value_schema.type,
#             "data_type_config": value.value_schema.type_config,
#             "chunk_map": chunk_map,
#         }
#
#         bytes_structure = BytesStructure.construct(**bytes_structure_data)
#
#         load_config_data = {
#             "provisioning_strategy": ByteProvisioningStrategy.FILE_PATH_MAP,
#             "module_type": "table.load_from.disk",
#             "module_config": {"only_column": "array"},
#             "inputs": {
#                 "bytes_structure": LOAD_CONFIG_PLACEHOLDER,
#             },
#             "output_name": value.value_schema.type,
#         }
#
#         load_config = LoadConfig(**load_config_data)
#         return load_config, bytes_structure
#
#     def data_type__table(
#         self, value: Value, persistence_config: Mapping[str, Any]
#     ) -> Tuple[LoadConfig, Optional[BytesStructure]]:
#         """Store the table as Apache Arrow feather file
#
#         The table will be store with one feather file per column, to support de-duplicated storage of re-arranged tables.
#         """
#
#         import pyarrow as pa
#
#         table: KiaraTable = value.data
#
#         chunk_map = {}
#
#         # TODO: make sure temp dir is in the same partition as file store
#         temp_f = tempfile.mkdtemp()
#
#         def cleanup():
#             shutil.rmtree(temp_f, ignore_errors=True)
#
#         atexit.register(cleanup)
#
#         for column_name in table.arrow_table.column_names:
#             column: pa.Array = table.arrow_table.column(column_name)
#             if column_name == "":
#                 file_name = os.path.join(temp_f, EMPTY_COLUMN_NAME_MARKER)
#             else:
#                 file_name = os.path.join(temp_f, column_name)
#             self._store_array(
#                 array_obj=column, file_name=file_name, column_name=column_name
#             )
#             chunk_map[column_name] = [file_name]
#
#         bytes_structure_data: Dict[str, Any] = {
#             "data_type": value.value_schema.type,
#             "data_type_config": value.value_schema.type_config,
#             "chunk_map": chunk_map,
#         }
#
#         bytes_structure = BytesStructure.construct(**bytes_structure_data)
#
#         load_config_data = {
#             "provisioning_strategy": ByteProvisioningStrategy.FILE_PATH_MAP,
#             "module_type": "table.load_from.disk",
#             "inputs": {"bytes_structure": LOAD_CONFIG_PLACEHOLDER},
#             "output_name": value.value_schema.type,
#         }
#
#         load_config = LoadConfig(**load_config_data)
#         return load_config, bytes_structure
#
#     def _store_array(
#         self, array_obj: "pa.Array", file_name: str, column_name: "str" = "array"
#     ):
#
#         import pyarrow as pa
#
#         schema = pa.schema([pa.field(column_name, array_obj.type)])
#
#         # TODO: support non-single chunk columns
#         with pa.OSFile(file_name, "wb") as sink:
#             with pa.ipc.new_file(sink, schema=schema) as writer:
#                 batch = pa.record_batch(array_obj.chunks, schema=schema)
#                 writer.write(batch)


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

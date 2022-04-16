# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, Tuple

from kiara.models.filesystem import FileModel
from kiara.models.module.persistence import (
    ByteProvisioningStrategy,
    BytesStructure,
    LoadConfig,
)
from kiara.models.values.value import Value, ValueMap
from kiara.modules import KiaraModule, KiaraModuleConfig, ValueSetSchema
from kiara.modules.included_core_modules.persistence import PersistValueModule
from pydantic import Field

from kiara_plugin.tabular.models import KiaraArray, KiaraTable

if TYPE_CHECKING:
    import pyarrow as pa


class CreateTableModuleCOnfig(KiaraModuleConfig):

    source_profile: str = Field(description="The source profile name.")
    source_type: str = Field(description="The source data type.")
    ignore_errors: bool = Field(
        description="Whether to ignore convert errors and omit the failed items.",
        default=False,
    )


class CreateTableModule(KiaraModule):

    _module_type_name = "table.import"
    _config_cls = CreateTableModuleCOnfig

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        source_type = self.get_config_value("source_type")

        inputs = {source_type: {"type": source_type, "doc": "The source data."}}

        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        outputs = {
            "table": {
                "type": "table",
                "doc": "The (new) table.",
            }
        }
        return outputs

    def process(self, inputs: ValueMap, outputs: ValueMap) -> None:

        source_type = self.get_config_value("source_type")

        source_profile_name = self.get_config_value("source_profile")

        func_name = f"create_from__{source_profile_name}__{source_type}"
        func = getattr(self, func_name)

        source_value = inputs.get_value_obj(source_type)

        result = func(source_value=source_value)

        outputs.set_value("table", KiaraTable.create_table(result))

    def create_from__csv__file(self, source_value: Value) -> Any:

        from pyarrow import csv

        input_file: FileModel = source_value.data
        imported_data = csv.read_csv(input_file.path)
        return imported_data


class LoadTableConfig(KiaraModuleConfig):

    only_column: Optional[str] = Field(
        description="Whether to only load a single column instead of the whole table.",
        default=None,
    )


class LoadTableFromDiskModule(KiaraModule):

    _module_type_name = "table.load_from.disk"
    _config_cls = LoadTableConfig

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        inputs = {"bytes_structure": {"type": "any", "doc": "The bytes."}}
        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        if not self.get_config_value("only_column"):
            return {"table": {"type": "table", "doc": "The table."}}
        else:
            return {"array": {"type": "array", "doc": "The array."}}

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import pyarrow as pa

        bytes_structure: BytesStructure = inputs.get_value_data("bytes_structure")

        if not self.get_config_value("only_column"):
            columns = {}

            for column_name, chunks in bytes_structure.chunk_map.items():
                assert len(chunks) == 1
                with pa.memory_map(chunks[0], "r") as column_chunk:
                    loaded_arrays: pa.Table = pa.ipc.open_file(column_chunk).read_all()
                    column = loaded_arrays.column(column_name)
                    columns[column_name] = column

            arrow_table = pa.table(columns)

            table = KiaraTable.create_table(arrow_table)
            outputs.set_value("table", table)
        else:
            chunks = bytes_structure.chunk_map["array.arrow"]
            assert len(chunks) == 1
            with pa.memory_map(chunks[0], "r") as column_chunk:
                loaded_arrays = pa.ipc.open_file(column_chunk).read_all()
                column = loaded_arrays.column("array")

            array = KiaraArray.create_array(column)
            outputs.set_value("array", array)


class SaveTableToDiskModule(PersistValueModule):

    _module_type_name = "table.save_to.disk.as.feather"

    def get_persistence_target_name(self) -> str:
        return "disk"

    def get_persistence_format_name(self) -> str:
        return "arrays"

    def data_type__array(self, value: Value, persistence_config: Mapping[str, Any]):

        import pyarrow as pa

        kiara_array: KiaraArray = value.data

        chunk_map = {}

        # TODO: make sure temp dir is in the same partition as file store
        temp_f = tempfile.mkdtemp()

        def cleanup():
            shutil.rmtree(temp_f, ignore_errors=True)

        atexit.register(cleanup)

        column: pa.Array = kiara_array.arrow_array
        file_name = os.path.join(temp_f, "array.arrow")
        self._store_array(array_obj=column, file_name=file_name, column_name="array")
        chunk_map["array.arrow"] = [file_name]

        bytes_structure_data: Dict[str, Any] = {
            "data_type": value.value_schema.type,
            "data_type_config": value.value_schema.type_config,
            "chunk_map": chunk_map,
        }

        bytes_structure = BytesStructure.construct(**bytes_structure_data)

        load_config_data = {
            "provisioning_strategy": ByteProvisioningStrategy.FILE_PATH_MAP,
            "module_type": "array.load_from.disk",
            "inputs": {"bytes_structure": "__dummy__"},
            "output_name": value.value_schema.type,
        }

        load_config = LoadConfig(**load_config_data)
        return load_config, bytes_structure

    def data_type__table(
        self, value: Value, persistence_config: Mapping[str, Any]
    ) -> Tuple[LoadConfig, Optional[BytesStructure]]:
        """Store the table as Apache Arrow feather file

        The table will be store with one feather file per column, to support de-duplicated storage of re-arranged tables.
        """

        import pyarrow as pa

        table: KiaraTable = value.data

        chunk_map = {}

        # TODO: make sure temp dir is in the same partition as file store
        temp_f = tempfile.mkdtemp()

        def cleanup():
            shutil.rmtree(temp_f, ignore_errors=True)

        atexit.register(cleanup)

        for column_name in table.arrow_table.column_names:
            column: pa.Array = table.arrow_table.column(column_name)
            file_name = os.path.join(temp_f, column_name)
            self._store_array(
                array_obj=column, file_name=file_name, column_name=column_name
            )
            chunk_map[column_name] = [file_name]

        bytes_structure_data: Dict[str, Any] = {
            "data_type": value.value_schema.type,
            "data_type_config": value.value_schema.type_config,
            "chunk_map": chunk_map,
        }

        bytes_structure = BytesStructure.construct(**bytes_structure_data)

        load_config_data = {
            "provisioning_strategy": ByteProvisioningStrategy.FILE_PATH_MAP,
            "module_type": "table.load_from.disk",
            "inputs": {"bytes_structure": "__dummy__"},
            "output_name": value.value_schema.type,
        }

        load_config = LoadConfig(**load_config_data)
        return load_config, bytes_structure

    def _store_array(
        self, array_obj: "pa.Array", file_name: str, column_name: "str" = "array"
    ):

        import pyarrow as pa

        schema = pa.schema([pa.field(column_name, array_obj.type)])

        # TODO: support non-single chunk columns
        with pa.OSFile(file_name, "wb") as sink:
            with pa.ipc.new_file(sink, schema=schema) as writer:
                batch = pa.record_batch(array_obj.chunks, schema=schema)
                writer.write(batch)

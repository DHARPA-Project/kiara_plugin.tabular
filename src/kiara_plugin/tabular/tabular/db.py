# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import Any, List

from kiara import KiaraModule
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import FileBundle, FileModel
from kiara.models.module.persistence import BytesStructure
from kiara.models.values.value import Value, ValueMap
from kiara.modules import ModuleCharacteristics, ValueSetSchema
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.utils import find_free_id, log_message
from pydantic import Field

from kiara_plugin.tabular.models.db import KiaraDatabase
from kiara_plugin.tabular.utils import (
    create_sqlite_table_from_tabular_file,
    insert_db_table_from_file_bundle,
)


class CreateDatabaseModuleConfig(CreateFromModuleConfig):

    ignore_errors: bool = Field(
        description="Whether to ignore convert errors and omit the failed items.",
        default=False,
    )
    merge_into_single_table: bool = Field(
        description="Whether to merge all csv files into a single table.", default=False
    )
    include_source_metadata: bool = Field(
        description="Whether to include a table with metadata about the source files.",
        default=True,
    )
    include_source_file_content: bool = Field(
        description="When including source metadata, whether to also include the original raw (string) content.",
        default=False,
    )


class CreateDatabaseModule(CreateFromModule):

    _module_type_name = "database.create"
    _config_cls = CreateDatabaseModuleConfig

    def create__database__from__csv_file(self, source_value: Value) -> Any:

        raise NotImplementedError()
        from pyarrow import csv

        input_file: FileModel = source_value.data
        imported_data = csv.read_csv(input_file.path)
        return imported_data

    def create__database__from__csv_file_bundle(self, source_value: Value) -> Any:

        merge_into_single_table = self.get_config_value("merge_into_single_table")
        if merge_into_single_table:
            raise NotImplementedError("Not supported (yet).")

        include_raw_content_in_file_info: bool = self.get_config_value(
            "include_source_metadata"
        )

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

        db = KiaraDatabase(db_file_path=db_path)
        db.create_if_not_exists()

        # TODO: check whether/how to add indexes

        bundle: FileBundle = source_value.data
        table_names: List[str] = []
        for rel_path in sorted(bundle.included_files.keys()):

            file_item = bundle.included_files[rel_path]
            table_name = find_free_id(
                stem=file_item.file_name_without_extension, current_ids=table_names
            )
            try:
                table_names.append(table_name)
                create_sqlite_table_from_tabular_file(
                    target_db_file=db_path, file_item=file_item, table_name=table_name
                )
            except Exception as e:
                if self.get_config_value("ignore_errors") is True or True:
                    log_message("ignore.import_file", file=rel_path, reason=str(e))
                    continue
                raise KiaraProcessingException(e)

        if include_raw_content_in_file_info:
            include_content: bool = self.get_config_value("include_source_file_content")
            db._unlock_db()
            insert_db_table_from_file_bundle(
                database=db,
                file_bundle=source_value.data,
                table_name="source_files_metadata",
                include_content=include_content,
            )
            db._lock_db()

        return db_path


# class SaveDatabaseModule(PersistValueModule):
#
#     _module_type_name = "database.save_to.disk"
#
#     def get_persistence_target_name(self) -> str:
#         return "disk"
#
#     def get_persistence_format_name(self) -> str:
#         return "arrays"
#
#     def data_type__database(self, value: Value, persistence_config: Mapping[str, Any]):
#
#         db: KiaraDatabase = value.data  # type: ignore
#         db._lock_db()
#
#         # TODO: assert type inherits from database?
#         chunk_map = {}
#         chunk_map["db.sqlite"] = [db.db_file_path]
#
#         bytes_structure_data: Dict[str, Any] = {
#             "data_type": value.value_schema.type,
#             "data_type_config": value.value_schema.type_config,
#             "chunk_map": chunk_map,
#         }
#         bytes_structure = BytesStructure.construct(**bytes_structure_data)
#
#         load_config_data = {
#             "provisioning_strategy": ByteProvisioningStrategy.FILE_PATH_MAP,
#             "module_type": "database.load_from.disk",
#             "inputs": {
#                 "bytes_structure": LOAD_CONFIG_PLACEHOLDER,
#             },
#             "output_name": value.value_schema.type,
#         }
#
#         load_config = LoadConfig.construct(**load_config_data)
#         return load_config, bytes_structure


class LoadDatabaseFromDiskModule(KiaraModule):

    _module_type_name = "database.load_from.disk"

    def _retrieve_module_characteristics(self) -> ModuleCharacteristics:
        return ModuleCharacteristics(is_internal=True)

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        inputs = {"bytes_structure": {"type": "any", "doc": "The bytes."}}
        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        return {"database": {"type": "database", "doc": "The database."}}

    def process(self, inputs: ValueMap, outputs: ValueMap):

        bytes_structure: BytesStructure = inputs.get_value_data("bytes_structure")

        db_file = bytes_structure.chunk_map["db.sqlite"]
        assert len(db_file) == 1
        db = KiaraDatabase(db_file_path=db_file[0])
        db._immutable = True

        outputs.set_value("database", db)

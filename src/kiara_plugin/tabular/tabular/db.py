# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import Any, List, Mapping, Type

from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import FileBundle
from kiara.models.values.value import SerializedData, Value
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
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

    _module_type_name = "create.database"
    _config_cls = CreateDatabaseModuleConfig

    def create__database__from__csv_file(self, source_value: Value) -> Any:

        raise NotImplementedError()

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


class LoadDatabaseFromDiskModule(DeserializeValueModule):

    _module_type_name = "load.database"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": KiaraDatabase}

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "database"

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "copy"

    def to__python_object(self, data: SerializedData, **config: Any):

        assert "db.sqlite" in data.get_keys() and len(list(data.get_keys())) == 1

        chunks = data.get_serialized_data("db.sqlite")

        # TODO: support multiple chunks
        assert chunks.get_number_of_chunks() == 1
        files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
        assert len(files) == 1

        db_file = files[0]

        db = KiaraDatabase(db_file_path=db_file)
        return db

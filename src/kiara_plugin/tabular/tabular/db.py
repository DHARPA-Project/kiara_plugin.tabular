# -*- coding: utf-8 -*-
import atexit
import os
import shutil
import tempfile
from typing import Any, Dict, List, Mapping, Optional, Type

from kiara import KiaraModule
from kiara.exceptions import KiaraProcessingException
from kiara.models.filesystem import FileBundle, FileModel
from kiara.models.module import KiaraModuleConfig
from kiara.models.values.value import SerializedData, Value, ValueMap
from kiara.modules import ValueSetSchema
from kiara.modules.included_core_modules.create_from import (
    CreateFromModule,
    CreateFromModuleConfig,
)
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from kiara.utils import find_free_id, log_message
from pydantic import Field
from sqlalchemy import insert, text

from kiara_plugin.tabular.defaults import DEFAULT_TABULAR_DATA_CHUNK_SIZE
from kiara_plugin.tabular.models.db import KiaraDatabase
from kiara_plugin.tabular.models.table import KiaraTable
from kiara_plugin.tabular.utils import (
    create_sqlite_schema_data_from_arrow_table,
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
    include_source_metadata: Optional[bool] = Field(
        description="Whether to include a table with metadata about the source files.",
        default=None,
    )
    include_source_file_content: bool = Field(
        description="When including source metadata, whether to also include the original raw (string) content.",
        default=False,
    )


class CreateDatabaseModule(CreateFromModule):

    _module_type_name = "create.database"
    _config_cls = CreateDatabaseModuleConfig

    def create__database__from__csv_file(self, source_value: Value) -> Any:

        temp_f = tempfile.mkdtemp()
        db_path = os.path.join(temp_f, "db.sqlite")

        def cleanup():
            shutil.rmtree(db_path, ignore_errors=True)

        atexit.register(cleanup)

        file_item: FileModel = source_value.data
        table_name = file_item.file_name_without_extension

        try:
            create_sqlite_table_from_tabular_file(
                target_db_file=db_path, file_item=file_item, table_name=table_name
            )
        except Exception as e:
            if self.get_config_value("ignore_errors") is True or True:
                log_message("ignore.import_file", file=file_item.path, reason=str(e))

            raise KiaraProcessingException(e)

        include_raw_content_in_file_info: bool = self.get_config_value(
            "include_source_metadata"
        )
        if include_raw_content_in_file_info:
            db = KiaraDatabase(db_file_path=db_path)
            db.create_if_not_exists()
            include_content: bool = self.get_config_value("include_source_file_content")
            db._unlock_db()
            included_files = {file_item.file_name: file_item}
            file_bundle = FileBundle.create_from_file_models(
                files=included_files, bundle_name=file_item.file_name
            )
            insert_db_table_from_file_bundle(
                database=db,
                file_bundle=file_bundle,
                table_name="source_files_metadata",
                include_content=include_content,
            )
            db._lock_db()

        return db_path

    def create__database__from__csv_file_bundle(self, source_value: Value) -> Any:

        merge_into_single_table = self.get_config_value("merge_into_single_table")
        if merge_into_single_table:
            raise NotImplementedError("Not supported (yet).")

        include_raw_content_in_file_info: Optional[bool] = self.get_config_value(
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

        if include_raw_content_in_file_info in [None, True]:
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

    def create_optional_inputs(
        self, source_type: str, target_type
    ) -> Optional[Mapping[str, Mapping[str, Any]]]:

        if target_type == "database" and source_type == "table":

            return {
                "table_name": {
                    "type": "string",
                    "doc": "The name of the table in the new database.",
                    "default": "imported_table",
                }
            }
        else:
            return None

    def create__database__from__table(
        self, source_value: Value, optional: ValueMap
    ) -> Any:

        table_name = optional.get_value_data("table_name")
        if not table_name:
            table_name = "imported_table"

        table: KiaraTable = source_value.data
        arrow_table = table.arrow_table

        column_map = None
        index_columns = None

        sqlite_schema = create_sqlite_schema_data_from_arrow_table(
            table=arrow_table, index_columns=index_columns, column_map=column_map
        )

        db = KiaraDatabase.create_in_temp_dir()
        db._unlock_db()
        engine = db.get_sqlalchemy_engine()

        table = sqlite_schema.create_table(table_name=table_name, engine=engine)

        with engine.connect() as conn:

            for batch in arrow_table.to_batches(
                max_chunksize=DEFAULT_TABULAR_DATA_CHUNK_SIZE
            ):
                conn.execute(insert(table), batch.to_pylist())
                conn.commit()

        db._lock_db()
        return db


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


class QueryDatabaseConfig(KiaraModuleConfig):

    query: Optional[str] = Field(description="The query.", default=None)


class QueryDatabaseModule(KiaraModule):

    _config_cls = QueryDatabaseConfig
    _module_type_name = "query.database"

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        result: Dict[str, Dict[str, Any]] = {
            "database": {"type": "database", "doc": "The database to query."}
        }

        if not self.get_config_value("query"):
            result["query"] = {"type": "string", "doc": "The query to execute."}

        return result

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        return {"query_result": {"type": "table", "doc": "The query result."}}

    def process(self, inputs: ValueMap, outputs: ValueMap):

        import pyarrow as pa

        database: KiaraDatabase = inputs.get_value_data("database")
        query = self.get_config_value("query")
        if query is None:
            query = inputs.get_value_data("query")

        # TODO: make this memory efficent

        result_columns: Dict[str, List[Any]] = {}
        with database.get_sqlalchemy_engine().connect() as con:
            result = con.execute(text(query))
            for r in result:
                for k, v in dict(r).items():
                    result_columns.setdefault(k, []).append(v)

        table = pa.Table.from_pydict(result_columns)
        outputs.set_value("query_result", table)

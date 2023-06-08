# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Dict, Union

import pyarrow as pa

from kiara_plugin.tabular.defaults import DEFAULT_TABULAR_DATA_CHUNK_SIZE
from kiara_plugin.tabular.utils import create_sqlite_schema_data_from_arrow_table

if TYPE_CHECKING:
    from kiara.models import KiaraModel
    from kiara_plugin.tabular.models.db import KiaraDatabase
    from kiara_plugin.tabular.models.tables import KiaraTables


def attach_metadata(
    table: pa.Table,
    *,
    table_metadata: Union[Dict[str, "KiaraModel"], None] = None,
    column_metadata: Union[Dict[str, Dict[str, "KiaraModel"]], None] = None,
    overwrite_existing: bool = True
) -> pa.Table:
    """Attach metadata and column_metadata to a table.

    Arguments:
        table_metadata: the (overall) metadata to attach to the table (format: <metadata_key> = <metadata_value>)
        column_metadata: the column metadata to attach to the table (format: <column_name>.<metadata_key> = <metadata_value>)
        overwrite_existing: if True, existing keys will be overwritten, otherwise they will be kept and the new values will be ignored
    """

    if column_metadata:
        new_fields = []
        for idx, column_name in enumerate(table.schema.names):
            field = table.schema.field(idx)
            assert field.name == column_name

            if table_metadata:
                raise NotImplementedError()

            models = column_metadata.get(column_name, None)
            if not models:
                new_fields.append(field)
            else:
                coL_metadata = {}
                for key, model in models.items():
                    if not overwrite_existing:
                        if field.metadata and key in field.metadata.keys():
                            continue
                    coL_metadata[key] = model.as_json_with_schema(incl_model_id=True)
                new_field = field.with_metadata(coL_metadata)
                new_fields.append(new_field)

        new_schema = pa.schema(new_fields)
    else:
        new_schema = table.schema

    new_table = pa.table(table.columns, schema=new_schema)
    return new_table


def extract_column_metadata(table: pa.Table) -> Dict[str, Dict[str, "KiaraModel"]]:

    from kiara.registries.models import ModelRegistry

    model_registry = ModelRegistry.instance()

    result: Dict[str, Dict[str, KiaraModel]] = {}
    for idx, column_name in enumerate(table.schema.names):
        field = table.schema.field(idx)
        assert field.name == column_name

        if not field.metadata:
            result[column_name] = {}
        else:
            column_metadata = {}
            for key, model_data in field.metadata.items():
                model_instance = model_registry.create_instance_from_json(model_data)
                column_metadata[key] = model_instance
            result[column_name] = column_metadata

    return result


def create_database_from_tables(tables: "KiaraTables") -> "KiaraDatabase":

    from sqlalchemy import insert

    from kiara_plugin.tabular.models.db import KiaraDatabase

    column_map = None
    index_columns = None

    db = KiaraDatabase.create_in_temp_dir()
    db._unlock_db()
    engine = db.get_sqlalchemy_engine()

    for table_name, table in tables.tables.items():
        arrow_table = table.arrow_table
        nullable_columns = []
        for column_name in arrow_table.column_names:
            column = arrow_table.column(column_name)
            if column.null_count > 0:
                nullable_columns.append(column_name)

        sqlite_schema = create_sqlite_schema_data_from_arrow_table(
            table=table.arrow_table,
            index_columns=index_columns,
            column_map=column_map,
            nullable_columns=nullable_columns,
        )

        _table = sqlite_schema.create_table(table_name=table_name, engine=engine)
        with engine.connect() as conn:
            arrow_table = table.arrow_table
            for batch in arrow_table.to_batches(
                max_chunksize=DEFAULT_TABULAR_DATA_CHUNK_SIZE
            ):
                conn.execute(insert(_table), batch.to_pylist())
                conn.commit()

    db._lock_db()
    return db

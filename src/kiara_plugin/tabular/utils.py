# -*- coding: utf-8 -*-
import typing
from typing import Dict, Iterable, Mapping, Optional

from kiara.models.filesystem import FileBundle, FileModel
from sqlite_utils.cli import insert_upsert_implementation

from kiara_plugin.tabular.defaults import SqliteDataType
from kiara_plugin.tabular.models.db import KiaraDatabase, SqliteTableSchema

if typing.TYPE_CHECKING:
    import pyarrow as pa


def insert_db_table_from_file_bundle(
    database: KiaraDatabase,
    file_bundle: FileBundle,
    table_name: str = "file_items",
    include_content: bool = True,
):

    # TODO: check if table with that name exists

    from sqlalchemy import (
        Column,
        DateTime,
        Integer,
        MetaData,
        String,
        Table,
        Text,
        insert,
    )
    from sqlalchemy.engine import Engine

    # if db_file_path is None:
    #     temp_f = tempfile.mkdtemp()
    #     db_file_path = os.path.join(temp_f, "db.sqlite")
    #
    #     def cleanup():
    #         shutil.rmtree(db_file_path, ignore_errors=True)
    #
    #     atexit.register(cleanup)

    metadata_obj = MetaData()

    file_items = Table(
        table_name,
        metadata_obj,
        Column("id", Integer, primary_key=True),
        Column("size", Integer(), nullable=False),
        Column("import_time", DateTime(), nullable=False),
        Column("mime_type", String(length=64), nullable=False),
        Column("rel_path", String(), nullable=False),
        Column("file_name", String(), nullable=False),
        Column("content", Text(), nullable=not include_content),
    )

    engine: Engine = database.get_sqlalchemy_engine()
    metadata_obj.create_all(engine)

    with engine.connect() as con:

        # TODO: commit in batches for better performance

        for index, rel_path in enumerate(sorted(file_bundle.included_files.keys())):
            f: FileModel = file_bundle.included_files[rel_path]
            if not include_content:
                content: Optional[str] = f.read_text()  # type: ignore
            else:
                content = None

            _values = {
                "id": index,
                "size": f.size,
                "import_time": f.import_time,
                "mime_type": f.mime_type,
                "rel_path": rel_path,
                "file_name": f.file_name,
                "content": content,
            }

            stmt = insert(file_items).values(**_values)
            con.execute(stmt)
        con.commit()


def convert_arrow_type_to_sqlite(data_type: str) -> SqliteDataType:

    if data_type.startswith("int") or data_type.startswith("uint"):
        return "INTEGER"

    if (
        data_type.startswith("float")
        or data_type.startswith("decimal")
        or data_type.startswith("double")
    ):
        return "REAL"

    if data_type.startswith("time") or data_type.startswith("date"):
        return "TEXT"

    if data_type == "bool":
        return "INTEGER"

    if data_type in ["string", "utf8", "large_string", "large_utf8"]:
        return "TEXT"

    if data_type in ["binary", "large_binary"]:
        return "BLOB"

    raise Exception(f"Can't convert to sqlite type: {data_type}")


def convert_arrow_column_types_to_sqlite(
    table: "pa.Table",
) -> Dict[str, SqliteDataType]:

    result: Dict[str, SqliteDataType] = {}
    for column_name in table.column_names:
        field = table.field(column_name)
        sqlite_type = convert_arrow_type_to_sqlite(str(field.type))
        result[column_name] = sqlite_type

    return result


def create_sqlite_schema_data_from_arrow_table(
    table: "pa.Table",
    column_map: Optional[Mapping[str, str]] = None,
    index_columns: Optional[Iterable[str]] = None,
    nullable_columns: Optional[Iterable[str]] = None,
    primary_key: Optional[str] = None,
) -> SqliteTableSchema:
    """Create a sql schema statement from an Arrow table object.

    Arguments:
        table: the Arrow table object
        column_map: a map that contains column names that should be changed in the new table
        index_columns: a list of column names (after mapping) to create module_indexes for
        extra_column_info: a list of extra schema instructions per column name (after mapping)
    """

    columns = convert_arrow_column_types_to_sqlite(table=table)
    if column_map is None:
        column_map = {}

    temp: Dict[str, SqliteDataType] = {}

    if index_columns is None:
        index_columns = []

    if nullable_columns is None:
        nullable_columns = []

    for cn, sqlite_data_type in columns.items():
        if cn in column_map.keys():
            new_key = column_map[cn]
        else:
            new_key = cn

        temp[new_key] = sqlite_data_type

    columns = temp
    if not columns:
        raise Exception("Resulting table schema has no columns.")
    else:
        for ic in index_columns:
            if ic not in columns.keys():
                raise Exception(
                    f"Can't create schema, requested index column name not available: {ic}"
                )

    schema = SqliteTableSchema(
        columns=columns,
        index_columns=index_columns,
        nullable_columns=nullable_columns,
        primary_key=primary_key,
    )
    return schema


def create_sqlite_table_from_tabular_file(
    target_db_file: str,
    file_item: FileModel,
    table_name: Optional[str] = None,
    is_csv: bool = True,
    is_tsv: bool = False,
    is_nl: bool = False,
    primary_key_column_names: Optional[Iterable[str]] = None,
    flatten_nested_json_objects: bool = False,
    csv_delimiter: str = None,
    quotechar: str = None,
    sniff: bool = True,
    no_headers: bool = False,
    encoding: str = "utf-8",
    batch_size: int = 100,
    detect_types: bool = True,
):

    if not table_name:
        table_name = file_item.file_name_without_extension

    with open(file_item.path, "rb") as f:

        insert_upsert_implementation(
            path=target_db_file,
            table=table_name,
            file=f,
            pk=primary_key_column_names,
            flatten=flatten_nested_json_objects,
            nl=is_nl,
            csv=is_csv,
            tsv=is_tsv,
            lines=False,
            text=False,
            convert=None,
            imports=None,
            delimiter=csv_delimiter,
            quotechar=quotechar,
            sniff=sniff,
            no_headers=no_headers,
            encoding=encoding,
            batch_size=batch_size,
            alter=False,
            upsert=False,
            ignore=False,
            replace=False,
            truncate=False,
            not_null=None,
            default=None,
            detect_types=detect_types,
            analyze=False,
            load_extension=None,
            silent=True,
            bulk_sql=None,
        )

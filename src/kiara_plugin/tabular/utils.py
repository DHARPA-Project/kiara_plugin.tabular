# -*- coding: utf-8 -*-
import csv as csv_std
import io
import itertools
import json
import typing
from typing import Dict, Iterable, Mapping, Union

import sqlite_utils
from kiara.models.filesystem import FileBundle, FileModel
from kiara.utils import log_exception
from sqlite_utils.cli import (
    _find_variables,
    _load_extensions,
    _register_functions,
    verify_is_dict,
)
from sqlite_utils.utils import (
    OperationalError,
    TypeTracker,
    _compile_code,
    chunks,
    decode_base64_values,
    file_progress,
)
from sqlite_utils.utils import flatten as _flatten

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

    from sqlalchemy import Column, Integer, MetaData, String, Table, Text, insert
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
                content: Union[str, None] = f.read_text()  # type: ignore
            else:
                content = None

            _values = {
                "id": index,
                "size": f.size,
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
    column_map: Union[Mapping[str, str], None] = None,
    index_columns: Union[Iterable[str], None] = None,
    nullable_columns: Union[Iterable[str], None] = None,
    unique_columns: Union[Iterable[str], None] = None,
    primary_key: Union[str, None] = None,
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

    if unique_columns is None:
        unique_columns = []

    for cn, sqlite_data_type in columns.items():
        if cn in column_map.keys():
            new_key = column_map[cn]
            index_columns = [
                x if x not in column_map.keys() else column_map[x]
                for x in index_columns
            ]
            unique_columns = [
                x if x not in column_map.keys() else column_map[x]
                for x in unique_columns
            ]
            nullable_columns = [
                x if x not in column_map.keys() else column_map[x]
                for x in nullable_columns
            ]
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
        unique_columns=unique_columns,
        primary_key=primary_key,
    )
    return schema


def create_sqlite_table_from_tabular_file(
    target_db_file: str,
    file_item: FileModel,
    table_name: Union[str, None] = None,
    is_csv: bool = True,
    is_tsv: bool = False,
    is_nl: bool = False,
    primary_key_column_names: Union[Iterable[str], None] = None,
    flatten_nested_json_objects: bool = False,
    csv_delimiter: Union[str, None] = None,
    quotechar: Union[str, None] = None,
    sniff: bool = True,
    no_headers: bool = False,
    encoding: str = "utf-8",
    batch_size: int = 100,
    detect_types: bool = True,
):

    if not table_name:
        table_name = file_item.file_name_without_extension

    f = open(file_item.path, "rb")

    try:
        insert_upsert_implementation_patched(
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
    except Exception as e:
        log_exception(e)
    # finally:
    #     f.close()


def insert_upsert_implementation_patched(
    path,
    table,
    file,
    pk,
    flatten,
    nl,
    csv,
    tsv,
    lines,
    text,
    convert,
    imports,
    delimiter,
    quotechar,
    sniff,
    no_headers,
    encoding,
    batch_size,
    alter,
    upsert,
    ignore=False,
    replace=False,
    truncate=False,
    not_null=None,
    default=None,
    detect_types=None,
    analyze=False,
    load_extension=None,
    silent=False,
    bulk_sql=None,
    functions=None,
):
    """Patched version of the insert/upsert implementation from the sqlite-utils package."""

    import rich_click as click

    db = sqlite_utils.Database(path)
    _load_extensions(db, load_extension)
    if functions:
        _register_functions(db, functions)
    if (delimiter or quotechar or sniff or no_headers) and not tsv:
        csv = True
    if (nl + csv + tsv) >= 2:
        raise click.ClickException("Use just one of --nl, --csv or --tsv")
    if (csv or tsv) and flatten:
        raise click.ClickException("--flatten cannot be used with --csv or --tsv")
    if encoding and not (csv or tsv):
        raise click.ClickException("--encoding must be used with --csv or --tsv")
    if pk and len(pk) == 1:
        pk = pk[0]
    encoding = encoding or "utf-8-sig"

    # The --sniff option needs us to buffer the file to peek ahead
    sniff_buffer = None
    if sniff:
        sniff_buffer = io.BufferedReader(file, buffer_size=4096)
        decoded = io.TextIOWrapper(sniff_buffer, encoding=encoding)
    else:
        decoded = io.TextIOWrapper(file, encoding=encoding)

    try:
        tracker = None
        with file_progress(decoded, silent=silent) as decoded:
            if csv or tsv:
                if sniff:
                    # Read first 2048 bytes and use that to detect
                    first_bytes = sniff_buffer.peek(2048)
                    dialect = csv_std.Sniffer().sniff(
                        first_bytes.decode(encoding, "ignore")
                    )
                else:
                    dialect = "excel-tab" if tsv else "excel"
                csv_reader_args = {"dialect": dialect}
                if delimiter:
                    csv_reader_args["delimiter"] = delimiter
                if quotechar:
                    csv_reader_args["quotechar"] = quotechar
                reader = csv_std.reader(decoded, **csv_reader_args)
                first_row = next(reader)
                if no_headers:
                    headers = [
                        "untitled_{}".format(i + 1) for i in range(len(first_row))
                    ]
                    reader = itertools.chain([first_row], reader)
                else:
                    headers = first_row
                docs = (dict(zip(headers, row)) for row in reader)
                if detect_types:
                    tracker = TypeTracker()
                    docs = tracker.wrap(docs)
            elif lines:
                docs = ({"line": line.strip()} for line in decoded)
            elif text:
                docs = ({"text": decoded.read()},)
            else:
                try:
                    if nl:
                        docs = (json.loads(line) for line in decoded if line.strip())
                    else:
                        docs = json.load(decoded)
                        if isinstance(docs, dict):
                            docs = [docs]
                except json.decoder.JSONDecodeError:
                    raise click.ClickException(
                        "Invalid JSON - use --csv for CSV or --tsv for TSV files"
                    )
                if flatten:
                    docs = (_flatten(doc) for doc in docs)

        if convert:
            variable = "row"
            if lines:
                variable = "line"
            elif text:
                variable = "text"
            fn = _compile_code(convert, imports, variable=variable)
            if lines:
                docs = (fn(doc["line"]) for doc in docs)
            elif text:
                # Special case: this is allowed to be an iterable
                text_value = list(docs)[0]["text"]
                fn_return = fn(text_value)
                if isinstance(fn_return, dict):
                    docs = [fn_return]
                else:
                    try:
                        docs = iter(fn_return)
                    except TypeError:
                        raise click.ClickException(
                            "--convert must return dict or iterator"
                        )
            else:
                docs = (fn(doc) or doc for doc in docs)

        extra_kwargs = {
            "ignore": ignore,
            "replace": replace,
            "truncate": truncate,
            "analyze": analyze,
        }
        if not_null:
            extra_kwargs["not_null"] = set(not_null)
        if default:
            extra_kwargs["defaults"] = dict(default)
        if upsert:
            extra_kwargs["upsert"] = upsert

        # docs should all be dictionaries
        docs = (verify_is_dict(doc) for doc in docs)

        # Apply {"$base64": true, ...} decoding, if needed
        docs = (decode_base64_values(doc) for doc in docs)

        # For bulk_sql= we use cursor.executemany() instead
        if bulk_sql:
            if batch_size:
                doc_chunks = chunks(docs, batch_size)
            else:
                doc_chunks = [docs]
            for doc_chunk in doc_chunks:
                with db.conn:
                    db.conn.cursor().executemany(bulk_sql, doc_chunk)
            return

        try:
            db[table].insert_all(
                docs, pk=pk, batch_size=batch_size, alter=alter, **extra_kwargs
            )
        except Exception as e:
            if (
                isinstance(e, OperationalError)
                and e.args
                and "has no column named" in e.args[0]
            ):
                raise click.ClickException(
                    "{}\n\nTry using --alter to add additional columns".format(
                        e.args[0]
                    )
                )
            # If we can find sql= and parameters= arguments, show those
            variables = _find_variables(e.__traceback__, ["sql", "parameters"])
            if "sql" in variables and "parameters" in variables:
                raise click.ClickException(
                    "{}\n\nsql = {}\nparameters = {}".format(
                        str(e), variables["sql"], variables["parameters"]
                    )
                )
            else:
                raise
        if tracker is not None:
            db[table].transform(types=tracker.types)
    finally:
        decoded.close()

# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Dict, Union

import pyarrow as pa

if TYPE_CHECKING:
    from kiara.models import KiaraModel


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

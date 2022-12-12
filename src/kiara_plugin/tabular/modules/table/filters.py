# -*- coding: utf-8 -*-
from typing import Any, Dict, Mapping, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.values.value import Value
from kiara.modules import ValueMapSchema
from kiara.modules.included_core_modules.filter import FilterModule

from kiara_plugin.tabular.models.table import KiaraTable


class TableFiltersModule(FilterModule):

    _module_type_name = "table.filters"

    @classmethod
    def retrieve_supported_type(cls) -> Union[Dict[str, Any], str]:

        return "table"

    def create_filter_inputs(self, filter_name: str) -> Union[None, ValueMapSchema]:

        if filter_name in ["select_columns", "drop_columns"]:

            return {
                "columns": {
                    "type": "list",
                    "doc": "The name of the columns to include.",
                    "optional": True,
                },
                "ignore_invalid_column_names": {
                    "type": "boolean",
                    "doc": "Whether to ignore invalid column names.",
                    "default": True,
                },
            }
        elif filter_name == "select_rows":
            return {
                "match": {
                    "type": "string",
                    "doc": "The string token to match.",
                    "optional": True,
                },
                "case_insensitive": {
                    "type": "boolean",
                    "doc": "Whether to ignore case.",
                    "default": True,
                },
            }

        return None

    def filter__select_columns(self, value: Value, filter_inputs: Mapping[str, Any]):

        import pyarrow as pa

        ignore_invalid = filter_inputs["ignore_invalid_column_names"]
        column_names = filter_inputs["columns"]

        if not column_names:
            return value

        table: KiaraTable = value.data
        arrow_table = table.arrow_table
        _column_names = []
        _columns = []

        for column_name in column_names:
            if column_name not in arrow_table.column_names:
                if ignore_invalid:
                    continue
                else:
                    raise KiaraProcessingException(
                        f"Can't select column '{column_name}' from table: column name not available. Available columns: {', '.join(arrow_table.column_names)}."
                    )

            column = arrow_table.column(column_name)
            _column_names.append(column_name)
            _columns.append(column)

        return pa.table(data=_columns, names=_column_names)

    def filter__drop_columns(self, value: Value, filter_inputs: Mapping[str, Any]):

        import pyarrow as pa

        ignore_invalid = filter_inputs["ignore_invalid_column_names"]
        column_names_to_ignore = filter_inputs["columns"]

        if not column_names_to_ignore:
            return value

        table: KiaraTable = value.data
        arrow_table = table.arrow_table

        for column_name in column_names_to_ignore:
            if column_name not in arrow_table.column_names:
                if ignore_invalid:
                    continue
                else:
                    raise KiaraProcessingException(
                        f"Can't select column '{column_name}' from table: column name not available. Available columns: {', '.join(arrow_table.column_names)}."
                    )

        _column_names = []
        _columns = []
        for column_name in arrow_table.column_names:

            if column_name in column_names_to_ignore:
                continue

            column = arrow_table.column(column_name)
            _column_names.append(column_name)
            _columns.append(column)

        return pa.table(data=_columns, names=_column_names)

    def filter__select_rows(self, value: Value, filter_inputs: Mapping[str, Any]):

        match = filter_inputs.get("match", None)
        if not match:
            return value

        case_insensitive = filter_inputs.get("case_insensitive", True)

        import duckdb

        _table: KiaraTable = value.data
        rel_from_arrow = duckdb.arrow(_table.arrow_table)

        if case_insensitive:
            # query_tokens = [f"LOWER({c}) GLOB LOWER('{match}')" for c in rel_from_arrow.columns]
            query_tokens = [
                f"regexp_matches(LOWER({c}), LOWER('{match}'))"
                for c in rel_from_arrow.columns
            ]
        else:
            query_tokens = [
                f"regexp_matches({c}, '{match}')" for c in rel_from_arrow.columns
            ]
        query = " OR ".join(query_tokens)

        result = rel_from_arrow.filter(query)
        return result.arrow()

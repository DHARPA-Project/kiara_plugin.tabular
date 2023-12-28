# -*- coding: utf-8 -*-
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Dict,
    Iterable,
    List,
    Mapping,
    Sequence,
    Union,
)

import pyarrow as pa
from pydantic import Field, PrivateAttr

from kiara.exceptions import KiaraException
from kiara.models import KiaraModel
from kiara.models.values.value import Value
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.tabular.models import TableMetadata
from kiara_plugin.tabular.utils.tables import extract_column_metadata

if TYPE_CHECKING:
    import pandas as pd
    import polars as pl
    from rich.console import ConsoleRenderable


class KiaraTable(KiaraModel):
    """A wrapper class to manage tabular data in a memory efficient way."""

    @classmethod
    def create_table(cls, data: Any) -> "KiaraTable":
        """Create a `KiaraTable` instance from an Apache Arrow Table, or dict of lists."""

        import polars as pl

        if isinstance(data, KiaraTable):
            return data
        elif isinstance(data, Value):
            if data.data_type_name != "table":
                raise KiaraException(
                    f"Invalid data type '{data.data_type_name}', need 'table'."
                )
            return data.data  # type: ignore

        table_obj = None
        if isinstance(data, (pa.Table)):
            table_obj = data
        elif isinstance(data, (pl.DataFrame)):
            table_obj = data.to_arrow()
        else:
            try:
                table_obj = pa.table(data)
            except Exception:
                pass

        if table_obj is None:
            if isinstance(data, (str)):
                raise Exception(
                    f"Can't create table, invalid source data type 'string', check if maybe invalid alias: {data}."
                )

            raise Exception(
                f"Can't create table, invalid source data type: {type(data)}."
            )

        column_metadata = extract_column_metadata(table_obj)

        obj = KiaraTable()
        obj._table_obj = table_obj
        obj._column_metadata = column_metadata
        return obj

    data_path: Union[None, str] = Field(
        description="The path to the (feather) file backing this array.", default=None
    )

    """The path where the table object is store (for internal or read-only use)."""
    _table_obj: pa.Table = PrivateAttr(default=None)
    _column_metadata: Union[Dict[str, Dict[str, KiaraModel]], None] = PrivateAttr(
        default=None
    )

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    @property
    def arrow_table(self) -> pa.Table:
        """Return the data as an Apache Arrow Table instance."""

        if self._table_obj is not None:
            return self._table_obj

        if not self.data_path:
            raise Exception("Can't retrieve table data, object not initialized (yet).")

        with pa.memory_map(self.data_path, "r") as source:
            table: pa.Table = pa.ipc.open_file(source).read_all()

        self._table_obj = table
        return self._table_obj

    @property
    def column_names(self) -> List[str]:
        """Retrieve the names of all the columns of this table."""
        result: List[str] = self.arrow_table.column_names
        return result

    @property
    def column_metadata(self) -> Mapping[str, Mapping[str, KiaraModel]]:

        if self._column_metadata is None:
            self._column_metadata = {}
        return self._column_metadata

    @property
    def num_rows(self) -> int:
        """Return the number of rows in this table."""
        result: int = self.arrow_table.num_rows
        return result

    def set_column_metadata(
        self,
        column_name: str,
        metadata_key: str,
        metadata: KiaraModel,
        overwrite_existing: bool = True,
    ):

        if column_name not in self.column_names:
            raise KiaraException(
                "Can't set column metadata, No column with name: " + column_name
            )

        if (
            not overwrite_existing
            and metadata_key in self.column_metadata.get(column_name, {}).keys()
        ):
            return

        self.column_metadata.setdefault(column_name, {})[metadata_key] = metadata  # type: ignore

    def get_column_metadata(self, column_name: str) -> Mapping[str, KiaraModel]:
        if column_name not in self.column_names:
            raise KiaraException("No column with name: " + column_name)

        if column_name not in self.column_metadata.keys():
            return {}

        return self.column_metadata[column_name]

    def get_column_metadata_for_key(
        self, column_name: str, metadata_key: str
    ) -> KiaraModel:

        if column_name not in self.column_names:
            raise KiaraException("No column with name: " + column_name)

        if column_name not in self.column_metadata.keys():
            raise KiaraException("No column metadata set for column: " + column_name)

        if metadata_key not in self.column_metadata[column_name].keys():
            raise KiaraException(
                "No column metadata set for column: "
                + column_name
                + " and key: "
                + metadata_key
            )

        return self.column_metadata[column_name][metadata_key]

    def to_pydict(self):
        """Convert and return the table data as a dictionary of lists.

        This will load all data into memory, so you might or might not want to do that.
        """
        return self.arrow_table.to_pydict()

    def to_pylist(self):
        """Convert and return the table data as a list of rows/dictionaries.

        This will load all data into memory, so you might or might not want to do that.
        """

        return self.arrow_table.to_pylist()

    def to_polars_dataframe(self) -> "pl.DataFrame":
        """Return the data as a Polars dataframe."""

        import polars as pl

        return pl.from_arrow(self.arrow_table)  # type: ignore

    def to_pandas_dataframe(
        self,
        include_columns: Union[None, str, Iterable[str]] = None,
        exclude_columns: Union[None, str, Iterable[str]] = None,
    ) -> "pd.DataFrame":
        """Convert and return the table data to a Pandas dataframe.

        This will load all data into memory, so you might or might not want to do that.

        Column names in the 'exclude_columns' argument take precedence over those in the 'include_columns' argument.

        """

        if include_columns is None:
            columns = self.arrow_table.column_names
        elif isinstance(include_columns, str):
            columns = [include_columns]
        else:
            columns = list(include_columns)

        if exclude_columns is not None:
            if isinstance(exclude_columns, str):
                columns = columns.remove(exclude_columns)
            elif exclude_columns:
                exclude_columns = list(exclude_columns)
                columns = [c for c in columns if c not in exclude_columns]

        table = self.arrow_table.select(columns)
        result: pd.DataFrame = table.to_pandas()
        return result

    def _repr_mimebundle_(
        self: "ConsoleRenderable",
        include: Sequence[str],
        exclude: Sequence[str],
        **kwargs: Any,
    ) -> Dict[str, str]:

        result: Dict[str, str] = super()._repr_mimebundle_(  # type: ignore
            include=include, exclude=exclude, **kwargs
        )
        pandas_dataframe = self.to_pandas_dataframe()  # type: ignore
        result["text/html"] = pandas_dataframe._repr_html_()

        return result


class KiaraTableMetadata(ValueMetadata):
    """File stats."""

    _metadata_key: ClassVar[str] = "table"

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["table"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "KiaraTableMetadata":

        kiara_table: KiaraTable = value.data

        md = TableMetadata.create_from_table(kiara_table)
        return KiaraTableMetadata(table=md)

    table: TableMetadata = Field(description="The table schema.")

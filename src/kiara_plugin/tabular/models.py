# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.tabular`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
import uuid
from typing import Any, Iterable, Optional

import pyarrow as pa
from kiara.defaults import ARRAY_MODEL_CATEOGORY_ID, TABLE_MODEL_CATEOGORY_ID
from kiara.models import KiaraModel
from pydantic import Field, PrivateAttr


class KiaraArray(KiaraModel):

    # @classmethod
    # def create_in_temp_dir(cls, ):
    #
    #     temp_f = tempfile.mkdtemp()
    #     file_path = os.path.join(temp_f, "array.feather")
    #
    #     def cleanup():
    #         shutil.rmtree(file_path, ignore_errors=True)
    #
    #     atexit.register(cleanup)
    #
    #     array_obj = cls(feather_path=file_path)
    #     return array_obj

    @classmethod
    def create_array(cls, data: Any) -> "KiaraArray":

        array_obj = None
        if isinstance(data, (pa.Array, pa.ChunkedArray)):
            array_obj = data
        elif isinstance(data, pa.Table):
            if len(data.columns) != 1:
                raise Exception(
                    f"Invalid type, only Arrow Arrays or single-column Tables allowed. This value is a table with {len(data.columns)} columns."
                )
            array_obj = data.column(0)
        else:
            try:
                array_obj = pa.array(data)
            except Exception:
                pass

        if array_obj is None:
            raise Exception(
                f"Can't create array, invalid source data type: {type(data)}."
            )

        obj = KiaraArray()
        obj._array_obj = array_obj
        return obj

    data_path: Optional[str] = Field(
        description="The path to the (feather) file backing this array."
    )

    _array_obj: pa.Array = PrivateAttr()

    def _retrieve_id(self) -> str:
        return str(uuid.uuid4())

    def _retrieve_category_id(self) -> str:
        return ARRAY_MODEL_CATEOGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    @property
    def arrow_array(self) -> pa.Array:

        if self._array_obj is not None:
            return self._array_obj

        if not self.data_path:
            raise Exception("Can't retrieve array data, object not initialized (yet).")

        with pa.memory_map(self.data_path, "r") as source:
            table: pa.Table = pa.ipc.open_file(source).read_all()

        if len(table.columns) != 1:
            raise Exception(
                f"Invalid serialized array data, only a single-column Table is allowed. This value is a table with {len(table.columns)} columns."
            )

        self._array_obj = table.column[0]
        return self._array_obj

    def to_pylist(self):
        return self.arrow_array.to_pylist()

    def to_pandas(self):
        return self.arrow_array.to_pandas()


class KiaraTable(KiaraModel):
    @classmethod
    def create_table(cls, data: Any) -> "KiaraTable":

        table_obj = None
        if isinstance(data, KiaraTable):
            return data

        if isinstance(data, (pa.Table)):
            table_obj = data
        else:
            try:
                table_obj = pa.table(data)
            except Exception:
                pass

        if table_obj is None:
            raise Exception(
                f"Can't create array, invalid source data type: {type(data)}."
            )

        obj = KiaraTable()
        obj._table_obj = table_obj
        return obj

    data_path: Optional[str] = Field(
        description="The path to the (feather) file backing this array."
    )
    _table_obj: pa.Table = PrivateAttr(default=None)

    def _retrieve_id(self) -> str:
        return str(uuid.uuid4())

    def _retrieve_category_id(self) -> str:
        return TABLE_MODEL_CATEOGORY_ID

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    @property
    def arrow_table(self) -> pa.Table:

        if self._table_obj is not None:
            return self._table_obj

        if not self.data_path:
            raise Exception("Can't retrieve table data, object not initialized (yet).")

        with pa.memory_map(self.data_path, "r") as source:
            table: pa.Table = pa.ipc.open_file(source).read_all()

        self._table_obj = table
        return self._table_obj

    @property
    def column_names(self) -> Iterable[str]:
        return self.arrow_table.column_names

    @property
    def num_rows(self) -> int:
        return self.arrow_table.num_rows

    def to_pydict(self):
        return self.arrow_table.to_pydict()

    def to_pylist(self):
        return self.arrow_table.to_pylist()

    def to_pandas(self):
        return self.arrow_table.to_pandas()

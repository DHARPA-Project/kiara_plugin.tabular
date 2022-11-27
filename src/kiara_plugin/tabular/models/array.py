# -*- coding: utf-8 -*-
from typing import Any, Union

import pyarrow as pa
from kiara.models import KiaraModel
from pydantic import Field, PrivateAttr


class KiaraArray(KiaraModel):
    """A class to manage array-like data.

    Internally, this uses an [Apache Arrow Array](https://arrow.apache.org/docs/python/generated/pyarrow.Array.html#pyarrow.Array) to handle the data in memory and on disk.
    """

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

        if isinstance(data, KiaraArray):
            return data

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
                f"Can't create table, invalid source data type: {type(data)}."
            )

        obj = KiaraArray()
        if not isinstance(array_obj, pa.lib.ChunkedArray):
            array_obj = pa.chunked_array(array_obj)
        obj._array_obj = array_obj
        return obj

    data_path: Union[str, None] = Field(
        description="The path to the (feather) file backing this array.", default=None
    )

    _array_obj: pa.Array = PrivateAttr(default=None)

    def _retrieve_data_to_hash(self) -> Any:
        raise NotImplementedError()

    def __len__(self):
        return len(self.arrow_array)

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

        self._array_obj = table.column(0)
        return self._array_obj

    def to_pylist(self):
        return self.arrow_array.to_pylist()

    def to_pandas(self):
        return self.arrow_array.to_pandas()

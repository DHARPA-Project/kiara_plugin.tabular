# -*- coding: utf-8 -*-
from typing import TYPE_CHECKING, Any, ClassVar, Dict, Iterable, Sequence, Union

import pyarrow as pa
from pydantic import Field, PrivateAttr

from kiara.models import KiaraModel
from kiara.models.values.value_metadata import ValueMetadata
from kiara_plugin.tabular.models import StorageBackend

if TYPE_CHECKING:
    from pandas import Series
    from rich.console import ConsoleRenderable

    from kiara.models.values.value import Value


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
            if isinstance(data, (str)):
                raise Exception(
                    f"Can't create array, invalid source data type 'string', maybe an invalid alias: {data}."
                )

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

    @property
    def num_rows(self) -> int:
        """Return the number of rows in the array."""

        return len(self.arrow_array)

    def to_pylist(self):
        return self.arrow_array.to_pylist()

    def to_pandas(self) -> "Series":
        result: Series = self.arrow_array.to_pandas()
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

        pandas_series = self.arrow_array.to_pandas()  # type: ignore
        result["text/html"] = pandas_series.to_frame()._repr_html_()

        return result


class KiaraArrayMetadata(ValueMetadata):
    """File stats."""

    _metadata_key: ClassVar[str] = "array"

    @classmethod
    def retrieve_supported_data_types(cls) -> Iterable[str]:
        return ["array"]

    @classmethod
    def create_value_metadata(cls, value: "Value") -> "KiaraArrayMetadata":

        kiara_array: KiaraArray = value.data
        _type = kiara_array.arrow_array.type

        type_id = _type.id
        size = kiara_array.arrow_array.nbytes

        backend_properties = {"type_id": type_id, "size": size}

        backend = StorageBackend(name="arrow", properties=backend_properties)
        return KiaraArrayMetadata(
            type_name=str(_type), rows=len(kiara_array), backend=backend
        )

    type_name: str = Field(
        description="The type of the data in the array (backend-specific)."
    )
    rows: int = Field(description="The number of rows.")
    backend: StorageBackend = Field(
        description="The storage backend that is used, and backend-specific properties."
    )

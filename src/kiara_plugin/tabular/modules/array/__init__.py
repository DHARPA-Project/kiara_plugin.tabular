# -*- coding: utf-8 -*-
from typing import Any, Mapping, Tuple, Type, Union

from kiara import KiaraModule
from kiara.models.module import KiaraModuleConfig
from kiara.models.module.jobs import JobLog
from kiara.models.values.value import SerializedData, ValueMap
from kiara.modules import ValueSetSchema
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from pydantic import Field

from kiara_plugin.tabular.models.table import KiaraArray


class DeserializeArrayModule(DeserializeValueModule):

    _module_type_name = "load.array"

    @classmethod
    def retrieve_supported_target_profiles(cls) -> Mapping[str, Type]:
        return {"python_object": KiaraArray}

    @classmethod
    def retrieve_serialized_value_type(cls) -> str:
        return "array"

    @classmethod
    def retrieve_supported_serialization_profile(cls) -> str:
        return "feather"

    def to__python_object(self, data: SerializedData, **config: Any):

        assert "array.arrow" in data.get_keys() and len(list(data.get_keys())) == 1

        chunks = data.get_serialized_data("array.arrow")

        # TODO: support multiple chunks
        assert chunks.get_number_of_chunks() == 1
        files = list(chunks.get_chunks(as_files=True, symlink_ok=True))
        assert len(files) == 1

        array_file = files[0]

        array = KiaraArray(data_path=array_file)
        return array


class ExtractDateConfig(KiaraModuleConfig):

    # resolution: Literal["year", "month", "day"] = Field(
    #     description="The resolution of the resolved date.", default="day"
    # )
    min_index: Union[None, int] = Field(
        description="The minimum index from where to start parsing the string(s).",
        default=None,
    )
    max_index: Union[None, int] = Field(
        description="The maximum index until whic to parse the string(s).", default=None
    )


class ExtractDateModule(KiaraModule):

    _module_type_name = "create.date_array"
    _config_cls = ExtractDateConfig

    def create_inputs_schema(
        self,
    ) -> ValueSetSchema:

        return {"array": {"type": "array", "doc": "The input array."}}

    def create_outputs_schema(
        self,
    ) -> ValueSetSchema:

        return {
            "date_array": {
                "type": "array",
                "doc": "The resulting array with items of a date data type.",
            }
        }

    def process(self, inputs: ValueMap, outputs: ValueMap, job_log: JobLog):

        import polars as pl
        import pyarrow as pa
        from dateutil import parser

        min_pos: Union[None, int] = self.get_config_value("min_index")
        if min_pos is None:
            min_pos = 0
        max_pos: Union[None, int] = self.get_config_value("min_index")

        errors = []

        def parse_date(_text: Tuple[str]):

            text = _text[0]

            if min_pos:
                try:
                    text = text[min_pos:]  # type: ignore
                except Exception:
                    return None
            if max_pos:
                try:
                    text = text[0 : max_pos - min_pos]  # type: ignore  # noqa
                except Exception:
                    pass

            try:
                d_obj = parser.parse(text, fuzzy=True)
            except Exception as e:
                errors.append(e)
                return None

            if d_obj is None:
                return None

            return (d_obj,)

        value = inputs.get_value_obj("array")
        array: KiaraArray = value.data

        # TODO: use array directly once new polars version is released
        table = pa.Table.from_arrays([array.arrow_array], ["array"])
        df = pl.DataFrame(data=table)

        result: pl.DataFrame = df.apply(
            parse_date, return_dtype=pl.datatypes.List(inner=pl.datatypes.Date)
        )
        if errors:
            job_log.add_log(f"Parsing finished, could not parse {len(errors)} strings")

        column = result.get_column("column_0")

        _array = column.to_arrow()
        chunked = pa.chunked_array(_array)

        outputs.set_value("date_array", chunked)

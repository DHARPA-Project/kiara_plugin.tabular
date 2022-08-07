# -*- coding: utf-8 -*-
from typing import Any, Iterable, List, Mapping, Type, Union

from kiara.exceptions import KiaraProcessingException
from kiara.models.module.jobs import JobLog
from kiara.models.values.value import SerializedData, ValueMap
from kiara.modules import ValueMapSchema
from kiara.modules.included_core_modules.serialization import DeserializeValueModule
from kiara_plugin.core_types.modules import AutoInputsKiaraModule, KiaraInputsConfig
from pydantic import Field

from kiara_plugin.tabular.models.array import KiaraArray


class DeserializeArrayModule(DeserializeValueModule):
    """Deserialize array data."""

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


FORCE_NON_NULL_DOC = "If set to 'True', raise an error if any of the strings in the array can't be parsed."
MIN_INDEX_DOC = "The minimum index from where to start parsing the string(s)."
MAX_INDEX_DOC = "The maximum index until whic to parse the string(s)."
REMOVE_TOKENS_DOC = "A list of tokens/characters to replace with a single white-space before parsing the input."


class ExtractDateConfig(KiaraInputsConfig):

    force_non_null: bool = Field(description=FORCE_NON_NULL_DOC, default=True)
    min_index: Union[None, int] = Field(
        description=MIN_INDEX_DOC,
        default=None,
    )
    max_index: Union[None, int] = Field(description=MAX_INDEX_DOC, default=None)
    remove_tokens: List[str] = Field(
        description=REMOVE_TOKENS_DOC, default_factory=list
    )


class ExtractDateModule(AutoInputsKiaraModule):
    """Create an array of date objects from an array of strings.

    This module is very simplistic at the moment, more functionality and options will be added in the future.

    At its core, this module uses the standard parser from the
    [dateutil](https://github.com/dateutil/dateutil) package to parse strings into dates. As this parser can't handle
     complex strings, the input strings can be pre-processed in the following ways:

    - 'cut' non-relevant parts of the string (using 'min_index' & 'max_index' input/config options)
    - remove matching tokens from the string, and replace them with a single whitespace (using the 'remove_tokens' option)

    By default, if an input string can't be parsed this module will raise an exception. This can be prevented by
    setting this modules 'force_non_null' config option or input to 'False', in which case un-parsable strings
    will appear as 'NULL' value in the resulting array.
    """

    _module_type_name = "parse.date_array"
    _config_cls = ExtractDateConfig

    def create_inputs_schema(
        self,
    ) -> ValueMapSchema:

        inputs = {"array": {"type": "array", "doc": "The input array."}}
        return inputs

    def create_outputs_schema(
        self,
    ) -> ValueMapSchema:

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

        force_non_null: bool = self.get_data_for_field(
            field_name="force_non_null", inputs=inputs
        )
        min_pos: Union[None, int] = self.get_data_for_field(
            field_name="min_index", inputs=inputs
        )
        if min_pos is None:
            min_pos = 0
        max_pos: Union[None, int] = self.get_data_for_field(
            field_name="max_index", inputs=inputs
        )
        remove_tokens: Iterable[str] = self.get_data_for_field(
            field_name="remove_tokens", inputs=inputs
        )

        def parse_date(_text: str):

            text = _text
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

            if remove_tokens:
                for t in remove_tokens:
                    text = text.replace(t, " ")

            try:
                d_obj = parser.parse(text, fuzzy=True)
            except Exception as e:
                if force_non_null:
                    raise KiaraProcessingException(e)
                return None

            if d_obj is None:
                if force_non_null:
                    raise KiaraProcessingException(
                        f"Can't parse date from string: {text}"
                    )
                return None

            return d_obj

        value = inputs.get_value_obj("array")
        array: KiaraArray = value.data

        series = pl.Series(name="tokens", values=array.arrow_array)
        job_log.add_log(f"start parsing date for {len(array)} items")
        result = series.apply(parse_date)
        job_log.add_log(f"finished parsing date for {len(array)} items")
        result_array = result.to_arrow()

        # TODO: remove this cast once the array data type can handle non-chunked arrays
        chunked = pa.chunked_array(result_array)
        outputs.set_values(date_array=chunked)

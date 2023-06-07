# -*- coding: utf-8 -*-

"""This module contains the metadata (and other) models that are used in the ``kiara_plugin.tabular`` package.

Those models are convenience wrappers that make it easier for *kiara* to find, create, manage and version metadata -- but also
other type of models -- that is attached to data, as well as *kiara* modules.

Metadata models must be a sub-class of [kiara.metadata.MetadataModel][kiara.metadata.MetadataModel]. Other models usually
sub-class a pydantic BaseModel or implement custom base classes.
"""
from typing import TYPE_CHECKING, Any, Dict

from pydantic import BaseModel, Field

from kiara.models import KiaraModel

if TYPE_CHECKING:
    pass


class ColumnSchema(BaseModel):
    """Describes properties of a single column of the 'table' data type."""

    type_name: str = Field(
        description="The type name of the column (backend-specific)."
    )
    metadata: Dict[str, KiaraModel] = Field(
        description="Other metadata for the column.", default_factory=dict
    )

    def _retrieve_data_to_hash(self) -> Any:

        metadata_hash = {k: v.instance_cid for k, v in self.metadata.items()}
        return {"type_name": self.type_name, "metadata": metadata_hash}

# -*- coding: utf-8 -*-
from kiara.models.values.value import Value
from kiara_plugin.tabular.models.tables import KiaraTables


def check_tables_result(tables: Value):

    # we can check properties here like we did in the outputs.yaml file
    # for that you need to look up the metadata Python classes, which is something that
    # is not documented yet, not sure how to best do that
    assert (
        tables.get_property_data("metadata.tables").tables["JournalEdges1902"].rows
        == 321
    )

    # more interestingly, we can test the data itself
    tables_data: KiaraTables = tables.data

    assert "JournalEdges1902" in tables_data.table_names
    assert "JournalNodes1902" in tables_data.table_names

    edges_table = tables_data.get_table("JournalEdges1902")
    assert "Source" in edges_table.column_names
    assert "Target" in edges_table.column_names

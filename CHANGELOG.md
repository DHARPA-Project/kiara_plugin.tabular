=========
Changelog
=========

## Version 0.5.5

- improve csv parsing when creating a table:
  - better error message when parsing fails
  - option to specify the delimiter
  - add auto-detecting delimiter feature if no delimiter is specified
- update duckdb dependency to 1.0.0
- update pandas dependency to 2.0.0

## Version 0.5.4

- BREAKING CHANGE:
  - key/value pair of 'column_map' config option for 'table.merge' module was switched: key is now value, value is now key
- new operation(s):
  - `table.add_column`: add a (single) column to a table, this might be extended in the future (for example to let users select the index where to insert the column, etc.)
- support for Python 3.12 (following duckdb dependency got support for it)

## Version 0.5.3

- new operations:
  - `tables.pick.table`: pick a table from a `tables` instance
  - `tables.pick.column`: pick a column from a `tables` instance

## Version 0.5.2

- support polars dataframe as input when creating a KiaraTable instance
- support Jupyter preview of KiaraTable data

=========
Changelog
=========

## Version 0.5.4 (Upcoming)

- BREAKING:
  - key/value pair of 'column_map' config option for 'table.merge' module was switched: key is now value, value is now key

- new operation:
  - `table.add_column`: add a (single) column to a table

## Version 0.5.3

- new operations:
  - `tables.pick.table`: pick a table from a `tables` instance
  - `tables.pick.column`: pick a column from a `tables` instance

## Version 0.5.2

- support polars dataframe as input when creating a KiaraTable instance
- support Jupyter preview of KiaraTable data

# Pipelines required files

This folder contains the neccessary json files that the pipeline needs to work. It is divided in two folders, each one of them need to be uploaded to its respective volumes, as configured in the pipeline DAB yaml.

## Schemas

They have the top level keys of the table it is refrencing to. Each key is a dictionary that has the next keys:

* type: Indicating the type that will have in databricks
* format: The date or timestamp format if the type is date.

## Configuration

They have each specific table configuration, each file must have a single dictionary with a the next keys and subkeys:

* last_snapshot:
    * partition_cols: Mandatory, list of str indicating the partition cols.
    * extra_cols_to_skip: Mandatory, list of str indicating the extra cols to skip. null if want to leave empty.
* history: the value can be null if you don't want to make a history subset or use it.
    * cols_to_keep: list of str indicating the cols that you need to keep from snapshot history.
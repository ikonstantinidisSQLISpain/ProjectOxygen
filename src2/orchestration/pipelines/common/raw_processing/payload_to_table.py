# This file has all the functions neccessary to translate a payload variant column from raw table into a bronze table.

def transform_payload_to_table(og_df, known_schema, metadata_cols):
    """
    Reads og_df, that expects a table with metadata columns, payload and snapshot date column.

    Known schema is a dict with the top level type of the columns as keys and the value as type.
    If there are nested structures they are expected to be variants.
    metadata_cols is a list with the extra metadata columns that we need to pick.
    """
    cols = [
        F.col(f"payload.`{field_name}`").cast(data_type).alias(field_name)
        for field_name, data_type in known_schema.items()
    ]

    cols.extend(metadata_cols)
    return og_df.select(*cols)
import pyspark.sql.functions as F




def uniqueness_checking(df, unique_col, col_to_check):
    """
    Takes a df as input and checks if each value in the "unique_col" param,
    there is a maximum of 1 value in the "col_to_check" column.
    """

    # We replace null values in order to count them as different values
    rename_col = f"z_{col_to_check}".replace(".","_") # renamed to avoid naming conflict
    # Replace dot to avoid conflict
    
    df = df.select(
        F.col(unique_col),
        F.col(col_to_check).alias(rename_col) 
    ).fillna("NULL_VALUE")

    grouped = df.groupby(unique_col).agg(
        F.count_distinct(rename_col).alias(f"n_{rename_col}")
    )

    result = grouped.filter(
        F.col(f"n_{rename_col}") > 1
    ).count() == 0
    return result
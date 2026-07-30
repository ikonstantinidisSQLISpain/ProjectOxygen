"""
The BU JSON is composed of the next keys

id : BIGINT
active : BOOL
type : STRING
symbole
name
calendar
society_id
entity_id

"""

import pyspark.sql.types as ty

JSON_SCHEMA = ty.StructType([
    ty.StructField("id", ty.LongTYpe(), False),
    ty.StructField("active", ty.BoolType(), False),
    ty.StructField("type", ty.StringType(), False),
    ty.StructField("symbole", ty.StringType(), False),
    ty.StructField("name", ty.StringType(), False),
    ty.StructField("calendar", ty.StringType(), False),
    ty.StructField("society", 
                   ty.StructType([
                       ty.StructField("id")
                   ]),
                   False)
])
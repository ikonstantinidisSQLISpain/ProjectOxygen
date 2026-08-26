import pyspark.pipelines as dp
from silver_constants import CATALOG, TARGET_SCHEMA, READ_SCHEMA, MAPPING_SCHEMA, METADATA_COLUMNS
CATALOG, TARGET_SCHEMA, READ_SCHEMA, MAPPING_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark), MAPPING_SCHEMA(spark)

TABLE_MAPPINGS = {
    # Read_Table: (Target_Table, Mapping_Table)
    "departments": ("departments", "department"),
    "bu": ("bu", "bucu"),
    "entities": ("entities", "entity"),
    "service_lines": ("service_lines", "service_line"),
    "sites": ("sites", "site"),
    "societies": ("societies", "society"),
}


def mapper_pipe_builder(read_table, target_table, mapping_table):

    @dp.table(
        name=f"{CATALOG}.{TARGET_SCHEMA}.{target_table}",
        table_properties={
            "quality":"silver"
        }
    )
    def f():
        
        bronze = spark.read.table(f"{CATALOG}.{READ_SCHEMA}.{read_table}")
        bronze = bronze.withColumnRenamed("name", "a_name")

        maps = spark.read.table(f"{CATALOG}.{MAPPING_SCHEMA}.{mapping_table}")


        df = bronze.join(maps, bronze['a_name'] == maps['anonymized'], how='left')

        for c in [*["a_name", "anonymized"], *METADATA_COLUMNS]:
            df = df.drop(c)

        return df

    return None


for read_table, (target_table, mapping_table) in TABLE_MAPPINGS.items():
    mapper_pipe_builder(read_table, target_table, mapping_table)
    











@dp.table(
    name=f"{CATALOG}.{TARGET_SCHEMA}.worker",
    table_properties={
        "quality":"silver"
    }
)
def mapper_worker_mail():
    
    bronze = spark.read.table(f"{CATALOG}.{READ_SCHEMA}.worker")
    bronze = bronze.withColumnRenamed("id", "a_name")
    bronze = bronze.withColumnRenamed("mail", "a_mail")

    worker_maps = spark.read.table(f"{CATALOG}.{MAPPING_SCHEMA}.ids")
    mails_maps = spark.read.table(f"{CATALOG}.{MAPPING_SCHEMA}.emails")
    mails_maps = mails_maps.withColumnRenamed("name", "email")
    mails_maps = mails_maps.withColumnRenamed("anonymized", "email_anonymized")


    df = bronze.join(worker_maps, bronze['a_name'] == worker_maps['anonymized'], how='left')
    df = df.join(mails_maps, df['a_mail'] == mails_maps['email_anonymized'], how='left')

    for c in [*["a_name", "anonymized", "email_anonymized", "a_mail"], *METADATA_COLUMNS]:
        df = df.drop(c)

    return df

from pyspark.sql.types import (
    StructType, StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    DataType,
    VariantType,
    DateType,
    TimestampType,
)

def spark_type(type_name: str) -> DataType:
    types = {
        "STRING": StringType(),
        "INTEGER": IntegerType(),
        "INT": IntegerType(),
        "LONG": LongType(),
        "BIGINT": LongType(),
        "DOUBLE": DoubleType(),
        "FLOAT": FloatType(),
        "BOOLEAN": BooleanType(),
        "DATE": StringType(),
        "TIMESTAMP": StringType(),
        "VARIANT": VariantType(),
    }

    try:
        return types[type_name.upper()]
    except KeyError:
        raise ValueError(f"Tipo Spark no soportado: {type_name}")


CATALOG = lambda spark: spark.conf.get("read.catalog")
READ_SCHEMA = lambda spark: spark.conf.get("raw.schema")
TARGET_SCHEMA = lambda spark: spark.conf.get("bronze.schema")

METADATA_COLUMNS = [
    "_source_file",
    "_source_file_name",
    "_source_file_size",
    "_source_file_modified_at",
    "snapshot_ts"
]


# We want to define schemas instead of allowing for inferece because we want to check if a new key has been added.
# Hardcoded structures cause we cant read files
KNOWN_SCHEMAS_PATHS = {
    "certifications": {"id": {"type": "STRING"}, "profileId": {"type": "STRING"}, "aiOnly": {"type": "BOOLEAN"}, "aptitudeReferences": {"type": "VARIANT"}, "createdBy": {"type": "STRING"}, "createdDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "deliveringEntity": {"type": "STRING"}, "lastModifiedBy": {"type": "STRING"}, "lastModifiedDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "obtentionDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "talentId": {"type": "STRING"}, "title": {"type": "STRING"}, "workspaceId": {"type": "STRING"}, "qualificationIds": {"type": "VARIANT"}, "attachment": {"type": "VARIANT"}, "attachmentId": {"type": "STRING"}, "description": {"type": "STRING"}, "endDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "expirationDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "startDate": {"type": "DATE", "format": "yyyy-MM-dd"}},
    #
    "profiles": {"aptitudes": {"type": "VARIANT"}, "id": {"type": "STRING"}, "talentId": {"type": "STRING"}, "versionName": {"type": "STRING"}, "main": {"type": "BOOLEAN"}, "contentLanguage": {"type": "STRING"}, "federationId": {"type": "STRING"}, "completionDetails": {"type": "VARIANT"}, "completionRate": {"type": "FLOAT"}, "completionRateLastComputedDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "createdBy": {"type": "STRING"}, "createdDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "customFields": {"type": "VARIANT"}, "functionalDomains": {"type": "VARIANT"}, "headline": {"type": "VARIANT"}, "lastExplicitUpdate": {"type": "STRING"}, "lastExplicitUpdateBy": {"type": "STRING"}, "lastModifiedBy": {"type": "STRING"}, "lastModifiedDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "permissionScope": {"type": "STRING"}, "qualificationIds": {"type": "VARIANT"}, "removed": {"type": "BOOLEAN"}, "resumeRelationStatus": {"type": "STRING"}, "schedules": {"type": "VARIANT"}, "skillRatings": {"type": "VARIANT"}, "status": {"type": "STRING"}, "targetFunctionalDomains": {"type": "VARIANT"}, "targetSkillRatings": {"type": "VARIANT"}, "targetSkills": {"type": "VARIANT"}, "travelRange": {"type": "STRING"}, "positions": {"type": "VARIANT"}, "hobbies": {"type": "STRING"}},
    #
    "skills": {"id": {"type": "STRING"}, "alsoPartOf": {"type": "VARIANT"}, "name": {"type": "VARIANT"}, "description": {"type": "VARIANT"}, "terms": {"type": "VARIANT"}, "hiddenTerms": {"type": "VARIANT"}, "wikipediaLink": {"type": "VARIANT"}, "depiction": {"type": "VARIANT"}, "type": {"type": "STRING"}, "granularity": {"type": "STRING"}, "removed": {"type": "BOOLEAN"}, "nature": {"type": "STRING"}, "parentId": {"type": "STRING"}, "federationId": {"type": "STRING"}},
    #
    "talents": {"profile": {"type": "VARIANT"}, "id": {"type": "STRING"}, "federationId": {"type": "STRING"}, "userId": {"type": "STRING"}, "createdBy": {"type": "STRING"}, "createdDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "lastModifiedBy": {"type": "STRING"}, "removed": {"type": "BOOLEAN"}, "lastModifiedDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "lastConnectionDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "permissionScope": {"type": "STRING"}, "tags": {"type": "VARIANT"}, "aspirations": {"type": "VARIANT"}, "maxWorkingHours": {"type": "VARIANT"}, "sharingDestinations": {"type": "VARIANT"}, "history": {"type": "VARIANT"}, "recruitment": {"type": "VARIANT"}, "timeEntryPreferredUnit": {"type": "STRING"}, "qualificationIds": {"type": "VARIANT"}, "customFields": {"type": "VARIANT"}, "freelyAssignable": {"type": "BOOLEAN"}, "workspaceId": {"type": "STRING"}, "endUser": {"type": "BOOLEAN"}, "staffable": {"type": "BOOLEAN"}, "lastInvitationDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "initials": {"type": "STRING"}, "managerId": {"type": "STRING"}, "workingLifeEntryDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "yearsOfExperience": {"type": "INTEGER"}, "removedDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "gender": {"type": "STRING"}, "phoneNumber": {"type": "STRING"}, "mentorId": {"type": "STRING"}, "externalId": {"type": "STRING"}, "photo": {"type": "VARIANT"}, "language": {"type": "STRING"}, "birthdate": {"type": "STRING"}, "availabilityConfirmationDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "availabilityConfirmationDateLastUpdatedBy": {"type": "STRING"}, "remoteWork": {"type": "BOOLEAN"}},
    #
    "users": {"id": {"type": "STRING"}, "idpId": {"type": "STRING"}, "enabled": {"type": "BOOLEAN"}, "username": {"type": "STRING"}, "formerUsernames": {"type": "VARIANT"}, "workspaceRoles": {"type": "VARIANT"}, "federationRoles": {"type": "VARIANT"}, "agenticStudioRoles": {"type": "VARIANT"}, "createdBy": {"type": "STRING"}, "createdDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "lastConnectionDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "lastModifiedBy": {"type": "STRING"}, "lastModifiedDate": {"type": "DATE", "format": "yyyy-MM-dd"}, "removed": {"type": "BOOLEAN"}, "language": {"type": "STRING"}, "theme": {"type": "STRING"}},
}









def dict_to_struct_type(schema_dict, add_history_cols=False):

    struct_fields = list()

    for column, definition in schema_dict.items():
        struct_fields.append(
            StructField(column, spark_type(definition["type"]), True)
        )
        if definition["type"].upper() == "VARIANT":
            struct_fields.append(
                StructField(f"hash_{column}", spark_type("STRING"), True)
            )

    for meta in METADATA_COLUMNS:
        if meta == "_source_file_size":
            meta_type = LongType()
        elif meta == "_source_file_modified_at":
            meta_type = TimestampType()
        elif meta == "snapshot_ts":
            meta_type = DateType()
        else:
            meta_type = StringType()
        struct_fields.append(
            StructField(meta, meta_type, True)
        )

    if add_history_cols:
        struct_fields.append(
            StructField("__START_AT", DateType(), True)
        )
        struct_fields.append(
            StructField("__END_AT", DateType(), True)
        )

    return StructType(struct_fields)




KNOWN_SCHEMAS = dict()
KNOWN_SCHEMAS_HISTORY = dict()
EXLCUDE_HISTORY_COLUMNS = dict()

for table, schema_data in KNOWN_SCHEMAS_PATHS.items():
    KNOWN_SCHEMAS[table] = dict_to_struct_type(schema_data)
    KNOWN_SCHEMAS_HISTORY[table] = dict_to_struct_type(schema_data, True)
    exclude_history = list()
    for col, desc in schema_data.items():
        if desc["type"].upper() == "VARIANT":
            exclude_history.append(col)
    EXLCUDE_HISTORY_COLUMNS[table] = exclude_history
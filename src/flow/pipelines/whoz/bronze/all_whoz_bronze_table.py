import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as ty
from pathlib import Path
import json
from bronze_constants import CATALOG, READ_SCHEMA, TARGET_SCHEMA, METADATA_COLUMNS
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)



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

def parse_data_type(dt_):
    if dt_ == "DATE":
        return "STRING"
    return dt_


def transform_payload_to_table(og_df, known_schema, metadata_cols):
    """
    Reads og_df, that expects a table with metadata columns, payload and snapshot date column.

    Known schema is a dict with the top level type of the columns as keys and the value as type.
    If there are nested structures they are expected to be variants.
    metadata_cols is a list with the extra metadata columns that we need to pick.
    """

    cols = list()

    for field_name, data_type in known_schema.items():
        col = (
            F.expr(f"variant_get(payload, '$.{field_name}', 'string')")
            .cast(parse_data_type(data_type["type"]))
            .alias(field_name)
        )
        cols.append(col)
        if data_type["type"] == "VARIANT":
            col2 = (
                F.expr(f"xxhash64(variant_get(payload, '$.{field_name}', 'string'))")
                .cast(parse_data_type(data_type["type"]))
                .alias(f"hash_{field_name}")
            )
            cols.append(col2)

    cols.extend(metadata_cols)
    return og_df.select(*cols)



"""This phase simply takes the payload and """
def pipe_maker(table_name):


    @dp.materialized_view(
        name=f"{CATALOG}.{TARGET_SCHEMA}.base_{table_name}"
    )
    def f():
        table = spark.read.table(f"{CATALOG}.{TARGET_SCHEMA}.raw_{table_name}")
        return transform_payload_to_table(table, KNOWN_SCHEMAS_PATHS[table_name], METADATA_COLUMNS)

    return None


for table in KNOWN_SCHEMAS_PATHS.keys():
    pipe_maker(table)



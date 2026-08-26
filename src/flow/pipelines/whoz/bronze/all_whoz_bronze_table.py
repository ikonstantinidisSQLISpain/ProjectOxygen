import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as ty
from pathlib import Path
import json
from bronze_constants import CATALOG, READ_SCHEMA, TARGET_SCHEMA, METADATA_COLUMNS
CATALOG, TARGET_SCHEMA, READ_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark), READ_SCHEMA(spark)






def load_json(json_str: str) -> dict:
    """
    Loads a JSON string and return its contents as a Python dictionary.

    Args:
        path: String of json.

    Returns:
        A dictionary containing the parsed JSON.

    Raises:
        FileNotFoundError: If the file does not exist.
        json.JSONDecodeError: If the file does not contain valid JSON.
        TypeError: If the top-level JSON object is not a dictionary or list.
    """
    data = json.loads(json_str)

    if not isinstance(data, dict) and not isinstance(data, list):
        raise TypeError(f"Expected a JSON object, got {type(data).__name__}")

    return data



# We want to define schemas instead of allowing for inferece because we want to check if a new key has been added.
# Hardcoded structures cause we cant read files
KNOWN_SCHEMAS_PATHS = {
    "certifications": """{"id": "str", "profileId": "str", "aiOnly": "bool", "aptitudeReferences_list": {"aptitudeId": "str", "conceptId": "str", "name": "str", "type": "str"}, "createdBy": "str", "createdDate": "str", "deliveringEntity": "str", "lastModifiedBy": "str", "lastModifiedDate": "str", "obtentionDate": "str", "talentId": "str", "title": "str", "workspaceId": "str", "qualificationIds_list": {}, "attachment": {"uid": "str", "contentType": "str", "name": "str", "size": "int"}, "attachmentId": "str", "description": "str", "endDate": "str", "expirationDate": "str", "startDate": "str"}""",
    #
    "profiles": """{"aptitudes_list": {"id": "str", "profileId": "str", "name": "str", "conceptId": "str", "cumulativeExperience": "float", "proficiency": "int", "talentId": "str", "type": "str", "visibility": "str", "augmentedWithAi": "bool"}, "id": "str", "talentId": "str", "versionName": "str", "main": "bool", "contentLanguage": "str", "federationId": "str", "completionDetails": {"BIO_MIN_LENGTH": {"satisfied": "bool", "weight": "int"}, "EDUCATIONS_MIN_ONE": {"satisfied": "bool", "weight": "int"}, "JOB_TITLE": {"satisfied": "bool", "weight": "int"}, "LANGUAGES_MIN_ONE": {"satisfied": "bool", "weight": "int"}, "PROFESSIONAL_EXPERIENCES_ALL_COMPLETE": {"satisfied": "bool", "weight": "int"}, "PROFESSIONAL_EXPERIENCES_MIN_ONE": {"satisfied": "bool", "weight": "int"}, "PROFILE_PICTURE": {"satisfied": "bool", "weight": "int"}, "SKILLS_ALL_WITH_PROFICIENCY": {"satisfied": "bool", "weight": "int"}, "SKILLS_MIN_FIFTEEN_COMPLETE": {"satisfied": "bool", "weight": "int"}, "SKILLS_MIN_FIVE_COMPLETE": {"satisfied": "bool", "weight": "int"}, "SKILLS_MIN_TEN_COMPLETE": {"satisfied": "bool", "weight": "int"}, "SKILLS_MIN_THREE_COMPLETE_HIGHLIGHTED": {"satisfied": "bool", "weight": "int"}, "WORKING_LIFE_ENTRY_DATE": {"satisfied": "bool", "weight": "int"}}, "completionRate": "float", "completionRateLastComputedDate": "str", "createdBy": "str", "createdDate": "str", "customFields_list": {}, "functionalDomains_list": {}, "headline": {"aim": "NoneType", "internationalMobility": "NoneType", "jobTitle": "str", "mobilityDate": "NoneType", "mobilityDestinations_list": {}, "mobilityNote": "NoneType", "nationalMobility": "NoneType", "permissionScope": "str", "seekingOpportunities": "NoneType", "seekingOpportunitiesLastModifiedDate": "NoneType", "mobilityDestinations": "NoneType"}, "lastExplicitUpdate": "str", "lastExplicitUpdateBy": "str", "lastModifiedBy": "str", "lastModifiedDate": "str", "permissionScope": "str", "qualificationIds_list": {}, "removed": "bool", "resumeRelationStatus": "str", "schedules_list": {}, "skillRatings_list": {"skill": "str", "rating": "int"}, "status": "str", "targetFunctionalDomains_list": {}, "targetSkillRatings_list": {}, "targetSkills_list": {}, "travelRange": "str", "positions_list": {"id": "str", "title": "str", "aptitudeReferences_list": {"aptitudeId": "str", "conceptId": "str", "name": "str", "type": "str"}, "companyName": "str", "current": "bool", "customFields_list": {}, "description": "str", "isMission": "bool", "profileId": "str", "qualificationIds_list": {}, "startDate": "str", "endDate": "str", "employerName": "str", "missionContext": "str", "missionName": "str", "parentPositionId": "str"}, "hobbies": "str", "completionDetails_list": {}}""",
    #
    "skills": """{"id": "str", "alsoPartOf_list": {}, "name_list": {"text": "str", "language": "str"}, "description_list": {"text": "str", "language": "str"}, "terms_list": {"text": "str", "language": "str"}, "hiddenTerms_list": {"text": "str", "language": "str"}, "wikipediaLink_list": {"text": "str", "language": "str"}, "depiction_list": {"text": "str", "language": "str"}, "type": "str", "granularity": "str", "removed": "bool", "nature": "str", "parentId": "str", "federationId": "str"}""",
    #
    "talents": """{"profile": {"id": "str", "talentId": "str", "versionName": "str", "main": "bool", "federationId": "str", "aptitudes_list": {"id": "str", "profileId": "str", "name": "str", "conceptId": "str", "cumulativeExperience": "float", "proficiency": "int", "talentId": "str", "type": "str", "visibility": "str", "augmentedWithAi": "bool"}, "completionDetails_list": {}, "completionRate": "float", "createdBy": "str", "customFields_list": {}, "educations_list": {}, "functionalDomains_list": {}, "headline": {"aim": "NoneType", "bio": "NoneType", "company": "NoneType", "internationalMobility": "NoneType", "jobTitle": "str", "mobilityDate": "NoneType", "mobilityDestinations": "NoneType", "mobilityNote": "NoneType", "nationalMobility": "NoneType", "permissionScope": "NoneType", "seekingOpportunities": "NoneType", "seekingOpportunitiesLastModifiedDate": "NoneType"}, "lastExplicitUpdate": "str", "lastExplicitUpdateBy": "str", "lastModifiedBy": "str", "links_list": {}, "mainSkills_list": {}, "permissionScope": "str", "positions_list": {}, "qualificationIds_list": {}, "removed": "bool", "secondarySkills_list": {}, "status": "str", "targetFunctionalDomains_list": {}, "unclassifiedSkills_list": {}}, "id": "str", "federationId": "str", "userId": "str", "createdBy": "str", "createdDate": "str", "lastModifiedBy": "str", "removed": "bool", "lastModifiedDate": "str", "lastConnectionDate": "str", "permissionScope": "str", "tags_list": {}, "aspirations_list": {}, "maxWorkingHours_list": {}, "sharingDestinations_list": {}, "history_list": {"since": "str", "scope": "str", "workspaceId": "str", "unitRate": {"value": "float", "currencyCode": "str"}, "unitCost": {"value": "float", "currencyCode": "str"}, "unitInternalRate": {"value": "float", "currencyCode": "str"}, "orgUnit": "str", "gradeId": "str", "roleId": "str"}, "recruitment": {"stageDates_list": {}, "workflowStepDates_list": {}, "sourcerId": "str", "recruiterId": "str"}, "timeEntryPreferredUnit": "str", "qualificationIds_list": {}, "customFields_list": {}, "freelyAssignable": "bool", "workspaceId": "str", "endUser": "bool", "staffable": "bool", "lastInvitationDate": "str", "initials": "str", "managerId": "str", "workingLifeEntryDate": "str", "yearsOfExperience": "int", "removedDate": "str", "recruitment_list": {}, "gender": "str", "phoneNumber": "str", "mentorId": "str", "externalId": "str", "photo": {"name": "str", "contentType": "str", "size": "int", "uid": "str", "empty": "bool"}, "language": "str", "birthdate": "str", "availabilityConfirmationDate": "str", "availabilityConfirmationDateLastUpdatedBy": "str", "remoteWork": "bool"}""",
    #
    "users": """{"id": "str", "idpId": "str", "enabled": "bool", "username": "str", "formerUsernames_list": {}, "workspaceRoles": {"668be62650f9cf5d75f87e62": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "674d911998271817b43b0f2e": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "674d91be8998971f87ea2a11": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "6824d03a4be325696d537f6f": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "6824d68183151b7a2252cd43": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "6824d8a54be325696d537f71": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "682609fe4be325696d5381c2": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b4a8091f7d959bcbaa5c3": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b4bbdfe239d7dc14db1fb": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b4f1dfe239d7dc14db204": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b517691f7d959bcbaa5d5": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b53edfe239d7dc14db21a": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b551691f7d959bcbaa5f0": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b56e8fe239d7dc14db224": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b5a3091f7d959bcbaa602": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b5c16fe239d7dc14db22f": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b5dd691f7d959bcbaa60d": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b5fcd91f7d959bcbaa60f": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b615f91f7d959bcbaa617": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b746bfe239d7dc14db25a": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b758691f7d959bcbaa641": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b76be91f7d959bcbaa642": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698c522591f7d959bcbaa766": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698c538bfe239d7dc14db382": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698c567afe239d7dc14db388": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698c57b891f7d959bcbaa77a": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698c5a1591f7d959bcbaa782": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698c5c4a91f7d959bcbaa786": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698c5da8fe239d7dc14db39c": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698f48db91f7d959bcbaaeee": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698f4a83fe239d7dc14dbab0": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "66fd6ffa1481522e4bd01a07": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "66fd6ffb1481522e4bd01a08": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}, "698b56a7fe239d7dc14db222": {"workspaceId": "str", "workspaceExternalId": "NoneType", "roles_list": {}}}, "federationRoles": {"668be62550f9cf5d75f87e61": {"federationId": "str", "roles_list": {}}}, "agenticStudioRoles_list": {}, "createdBy": "str", "createdDate": "str", "lastConnectionDate": "str", "lastModifiedBy": "str", "lastModifiedDate": "str", "removed": "bool", "language": "str", "theme": "str", "workspaceRoles_list": {}, "federationRoles_list": {}}""",
}

KNOWN_SCHEMAS_SUBKEYS = {
    "certifications": {
        "aptitudeReferences": {
            "aptitudeId": "str", 
            "conceptId": "str"
    },
    "profiles": {
        
    }
}

def structure_to_fields(json_str): # Skips nested dict and reads everything as string
    structure = load_json(json_str)
    fields = [
        {"name": k, "type": "string", "nullable": True}
        for k in structure.keys()
    ]
    return fields

def structure_to_schema(json_str):
    schema_json = {"type": "struct", "fields": structure_to_fields(json_str)}
    schema = ty.StructType.fromJson(schema_json)
    return schema


"""This phase simply takes the payload and """
def pipe_maker(table_name):


    @dp.materialized_view(
        name=f"{CATALOG}.{TARGET_SCHEMA}.base_{table_name}"
    )
    def f():
        schema = structure_to_schema(KNOWN_SCHEMAS_PATHS[table_name]) # Must be here, same reason as import
        table = spark.read.table(f"{CATALOG}.{TARGET_SCHEMA}.raw_{table_name}")\
                        .withColumn("_data", F.from_json("payload", schema))\
                        .select(
                              *[F.col(c) for c in METADATA_COLUMNS],
                              "_data.*")

        return table

    return None


for table in KNOWN_SCHEMAS_PATHS.keys():
    pipe_maker(table)
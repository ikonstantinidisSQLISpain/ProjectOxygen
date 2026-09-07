from common.constants import CATALOG, RAW_SCHEMA, BRONZE_SCHEMA
from common.raw_processing import raw_reader as rr

CATALOG, READ_SCHEMA, TARGET_SCHEMA = CATALOG(spark), RAW_SCHEMA(spark), BRONZE_SCHEMA(spark)

BASE_VOL =  rr.RawReader.volume_path_maker(CATALOG, READ_SCHEMA, "source")

WHOZ_DATA = {
    "accreditations": (BASE_VOL, "*whoz_accreditation*.json"),
    'certifications': (BASE_VOL, "*whoz__certification*.json"),
    "profiles": (BASE_VOL, "*whoz__profile*.json"),
    "skills": (BASE_VOL, "*whoz__skill*.json"),
    "users": (BASE_VOL, "*whoz__user*.json"),
    "talents": (BASE_VOL, "*whoz__talent*.json")
}

for name, (vol, regex) in WHOZ_DATA.items():
    raw_pipe_maker(CATALOG, TARGET_SCHEMA, name, vol, regex)
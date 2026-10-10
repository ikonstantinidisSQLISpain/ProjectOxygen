from pathlib import Path
import yaml


def create_structure(quality, platform, lvl, table, target_folder):

    data_format = "Table"
    description = ""
    origin = ""
    if quality == "bronze" and lvl == "raw":
        grain = "JSON row"
        data_format = "JSON"
        description = "Original data for "
        origin = "Sharepoint files from SQLi-Internal/app-reports."
    else:
        grain = ""

    if platform == "whoz" and lvl == "raw":
        origin = "Whoz API (https://developer.whoz.com/) stored in Sharepoint files from SQLi-Internal/app-reports"


    if lvl == "base":
        description = "Top level keys of the JSON have been transformed to columns, the data remains untouched."

    if lvl == "current":
        description = "The data from base level have been filtered and the last snapshot only has been selected."

    if lvl == "history":
        description = "The data from base level remains the same, however some columns might have been dropped, while retaining the historical data."

    if lvl == "cleaned":
        description = "The data from the latest snapshot has been processed, cleaned, filtered and transformed."

    if lvl == "dim":
        description = "The data represents a dimension and it is ready to be consumed by Business apps."

    if lvl == "fact":
        description = "The data represents the fact that are meant to be studied in analytics reports."

    metadata_base = {
        "description": description,
        "grain": grain,
        "source": {
            "origin": origin,
            "data": {
                "description": "",
                "format": data_format
            },
            "update_frequency": "daily"
        }
    }
    raw_schema = {
        "payload": {
            "description": "The original JSON data, where each row is an element in the original file JSON list.",
            "type": "variant",
            "nullable": False,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            },
            "nested_structures": dict()
        },
        "_source_file": {
            "description": "source file path.",
            "type": "string",
            "nullable": False,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            },
        },
        "_source_file_name": {
            "description": "source file name.",
            "type": "string",
            "nullable": False,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            },
        },
        "_source_file_size": {
            "description": "source file size.",
            "type": "bigint",
            "nullable": False,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            },
        },
        "_source_file_modified_at": {
            "description": "last modification time of the soource file.",
            "type": "timestamp",
            "nullable": False,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            },
        },
        "snapshot_ts": {
            "description": "snapshot date of the file",
            "type": "date",
            "nullable": False,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            },
        },
        "file_name_error": {
            "description": "error in the source file name, if present",
            "type": "string",
            "nullable": True,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            },
        },
        "ingest_ts": {
            "description": "timestamp when record was ingested",
            "type": "timestamp",
            "nullable": False,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            },
        },
    }

    base_schema_metadata = {k:v for k,v in raw_schema.items() if k != "payload"}

    base_else = {
        "id": {
            "description": "Identifier",
            "type": "variant",
            "nullable": False,
            "is_pk": False,
            "is_fk": False,
            "quality": {
                "null_percen": 0,
                "unique_percen": 1,
            }
        }
    }

    # Obtener el contenido asociado a los parámetros
    if lvl == "raw":
        content = raw_schema
    elif lvl == "base":
        content = base_schema_metadata
    else:
        content = base_else

    # Crear la ruta completa, incluyendo directorios intermedios
    folder = Path(target_folder) / quality / platform / table / lvl
    folder.mkdir(parents=True, exist_ok=True)

    
    for filename in ("schema.yml", "metadata.yml"):
        if filename == "schema.yml":
            with (folder / filename).open("w", encoding="utf-8") as f:
                yaml.safe_dump(
                    content,
                    f,
                    allow_unicode=True,
                    sort_keys=False
                )
        else:
            with (folder / filename).open("w", encoding="utf-8") as f:
                yaml.safe_dump(
                    metadata_base,
                    f,
                    allow_unicode=True,
                    sort_keys=False
                )

    return None



qualities = ["bronze", "silver"]
platforms = ["whoz", "analytic", "perso"]
schemas = [f"{quality}_{platform}" for quality in qualities for platform in platforms]
table_lvl = ["raw", "base", "current", "history", "cleaned", "fact"]
platform_tables = {
    "whoz": ["accreditation", "certification", "skill", "user", "talent", "profile"],
    "analytic": ["bu", "service_line", "entity", "society", "department", "site"],
    "perso": ["collab_status", "leave", "workers"]
}

quality_lvls = {
    "bronze": ["raw", "base", "current", "history"],
    "silver": ["cleaned"],
    "gold": ["fact"]
}

extra_tables = ["profile_educations", "profile_positions", "profile_aptitudes", "certification_aptitudes", "accreditation_aptitudes", "dim_organization", "dim_date", "collab_referential", "_user", "_talent"]

schema_tables = list()
for q in qualities:
    for p, ts in platform_tables.items():
        for t in ts:
            for lvl in quality_lvls[q]:
                schema_tables.append((f"{q}_{p}", f"{lvl}_{t}", q, p, lvl, t))

extra = [
    ("silver_whoz", "profile_educations", "silver", "whoz", "cleaned", "profile_educations"),
    ("silver_whoz", "profile_positions", "silver", "whoz", "cleaned", "profile_positions"),
    ("silver_whoz", "profile_aptitudes", "silver", "whoz", "cleaned", "profile_aptitudes"),
    ("silver_whoz", "certification_aptitudes", "silver", "whoz", "cleaned", "certification_aptitudes"),
    ("silver_whoz", "accreditation_aptitudes", "silver", "whoz", "cleaned", "accreditation_aptitudes"),
    ("gold_analytic", "dim_organization", "gold", "analytic", "dim", "organization"),
    ("gold_date", "dim_date", "gold", "date", "dim", "date"),
    ("gold_perso", "collab_referential", "gold", "perso", "fact", "collab_referential"),
    ("gold_whoz", "_user", "gold", "whoz", "fact", "_user"),
    ("gold_whoz", "_talent", "gold", "whoz", "fact", "_talent")
]

all_tables = schema_tables + extra

for t in all_tables:
    create_structure(*t[2:], Path(__file__).resolve().parent / "../Upload_to_volumes/comments")

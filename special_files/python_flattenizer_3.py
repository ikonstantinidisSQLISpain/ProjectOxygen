from pathlib import Path
from typing import Iterable


def merge_python_files(
    input_files: list[str],
    output_file: str,
    replacements: Iterable[str] | None = None,
) -> None:
    """
    Une varios archivos Python en uno.

    Reglas:
    - Concatena los archivos en el orden recibido.
    - Si una línea empieza por 'import common', la comenta.
    - Añade '# NombreArchivo.py' antes del contenido de cada archivo.
    - Sustituye todos los textos indicados en replacements por ''.
    """

    replacements = list(replacements or [])

    merged_parts = []

    for file_path in input_files:
        path = Path(file_path)

        content = path.read_text(encoding="utf-8")

        processed_lines = []

        for line in content.splitlines():
            if line.lstrip().startswith("import common"):
                processed_lines.append(f"# {line}")
            else:
                processed_lines.append(line)

        content = "\n".join(processed_lines)

        for replacement in replacements:
            content = content.replace(replacement, "")

        merged_parts.append(
            f"# {path.name}\n\n{content}\n"
        )

    Path(output_file).write_text(
        "\n\n".join(merged_parts),
        encoding="utf-8",
    )

    return None


merge_python_files(
    [
        "src2/orchestration/pipelines/common/constants.py",
        "src2/orchestration/pipelines/common/utils.py",
        "src2/orchestration/pipelines/common/raw_processing/raw_reader.py",
        "src2/orchestration/pipelines/common/raw_processing/payload_to_table.py",
        "src2/orchestration/pipelines/common/raw_processing/snapshot_processor.py",
        "src2/orchestration/pipelines/common/silver_processing/basic_filtering_and_quarantine.py",
        "src2/orchestration/pipelines/all_tables_bronze_silver.py"
    ],
    "src2/orchestration/pipelines/all_tables_bronze_silver_single_file_2.py",
    ["c.", "ut.", "rr.", "sp.", "pt.", "bfq."]
)
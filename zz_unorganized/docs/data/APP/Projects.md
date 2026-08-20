# Project

From introduction, "Originally named as "activity" and self-explanatory. It represents the individual project that is being done by the company."

| Column          | Type   | Description                                    | Contraints | PK/FK |
|-----------------|--------|------------------------------------------------|------------|-------|
| type_id         | STRING | Identifier of the project.                     |            | PK    |
| type_lib        | STRING | Label of the project.                          |            |       |
| activity        | STRING | Code that categorize the project.              |            |       |
| is_project      | BOOL   | If the project is billable or not.             |            |       |
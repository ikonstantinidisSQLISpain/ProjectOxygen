# Project and service lines.

It represents the many to many relationship between a project and a service line.

| Column          | Type   | Description                                     | Contraints | PK/FK |
|-----------------|--------|-------------------------------------------------|------------|-------|
| type_id         | STRING | Identifier of the activity.                     |            | PK FK |
| service_line_id | BIGINT | The service line which the activity belongs to. |            | PK FK |
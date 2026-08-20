# Service Lines, Zones and Departments.

This is the middle table that holds the relationship mentioned in the introduction as "a `department` may work at specific `service_line` at specific `zone` (Triple relation)."

| Column          | Type   | Description                    | Contraints | PK/FK |
|-----------------|--------|--------------------------------|------------|-------|
| service_line_id | BIGINT | Identifier of the service line |            | PK FK |
| zone_id         | BIGINT | Identifier of the zone         |            | PK FK |
| department_id   | BIGINT | Identifier of the department   |            | PK FK |
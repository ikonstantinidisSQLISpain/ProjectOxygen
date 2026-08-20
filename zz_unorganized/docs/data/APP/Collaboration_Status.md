# Collaboration Status

From introduction, "Represent the collaboration between each worker and each service line, under each department over time."


| Column                  | Type    | Description                                                | Contraints | PK/FK |
|-------------------------|---------|------------------------------------------------------------|------------|-------|
| worker_id               | BIGINT  | Identifier of the worker.                                  |            | PK FK |
| department_id           | BIGINT  | Indentifier of the department where the worker is working. |            | PK FK |
| service_line_id         | BIGINT  | Identifier of the service line the worker is working on.   |            | PK FK |
| year                    | INT     | Year when this data is measured.                           |            | PK    |
| month                   | TINYINT | Month when this data is measured.                          |            | PK    |
| status                  | STRING  | Status when the data was measured.                         |            |       |
| standard_cost_category  | STRING  | Self explanatory.                                          |            |       |
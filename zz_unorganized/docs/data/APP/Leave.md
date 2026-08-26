# Leaves

From introduction, "represent the leave requests made by the workers."

| Column          | Type   | Description                                | Contraints | PK/FK   |
|-----------------|--------|--------------------------------------------|------------|---------|
| worker_id       | STRING | Indentifier of the worker.                 |            | PK / FK |
| departure_date  | DATE   | Date when the worker leaves.               |            | PK      |
| departure_type  | STRING | Type of departure, holidays, day off, etc. |            |         |
| date_of_receipt | DATE   | Date when the leave request was requested. |            |         |
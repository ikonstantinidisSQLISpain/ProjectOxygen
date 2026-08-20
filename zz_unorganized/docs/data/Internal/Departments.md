# Department

From introduction, "a department is a group of workers that have a set of characteristics.".

| Column | Type   | Description                          | Contraints | PK/FK |
|--------|--------|--------------------------------------|------------|-------|
| id     | BIGINT | Identifier of the department.        |            |   PK  |
| name   | STRING | Name of the department.              |            |       |
| code   | STRING | Code that identifies the department. |            |       |
| active | BOOL   | If the department is active or not.  |            |       |
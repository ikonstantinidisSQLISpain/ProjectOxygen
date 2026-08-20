# Units

A unit, also named as business unit, even though business is the type of the unit, is, as mentioned in the introduction, a subdivision of the company that act as an independent company, with the purpose of the main company.

| Column     | Type    | Description                    | Contraints | PK/FK |
|------------|---------|--------------------------------|------------|-------|
| id         | BIGINT  | Identifier of the unit.        |            |   PK  |
| active     | BOOL    | Whether the unit is active.    |            |       |
| type       | LITERAL | The type of the unit.          | BU \| CU   |       |
| symbole    | STRING  | Code that identifies the unit. |            |       |
| name       | STRING  | Name of the unit.              |            |       |
| calendar   | STRING  | Code that identifies the unit. |            |       |
| society_id | BIGINT  |                                |            |   FK  |
| entity_id  | BIGINT  |                                |            |   FK  |


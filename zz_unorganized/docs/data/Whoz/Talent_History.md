# Talent history

This holds the data about the history of each talent.

| Column             | Type   | Description                                               | Contraints | PK/FK | Notes                      |
|--------------------|--------|-----------------------------------------------------------|------------|-------|----------------------------|
| talent_id          | STRING | ID of the talent.                                         |            | PK/FK |                            |
| since              | DATE   | Date of the event when something changed for this talent. |            | PK    |                            |
| scope              | STRING | State of the talent.                                      |            |       |                            |
| workspace_id       | STRING | Self-explanatory.                                         |            | FK    |                            |
| unit_rate          | STRUCT |                                                           |            |       | Keys: value, currency_code |
| unit_cost          | STRUCT |                                                           |            |       | Same as previous.          |
| unit_internal_rate | STRUCT |                                                           |            |       | Same as previous.          |
| org_unit           | STRING | Possible a Unit (Business or Cost)                        |            |       | To be determined properly. |
| grade_id           | STRING | ID, no clue to what.                                      |            | FK    |                            |
| role_id            | STRING | ID, no clue to what.                                      |            | FK    |                            |



# Skills and what it is part of.

This table holds the many to many relations between a skills as part of another.

| Column                | Type   | Description                               | Contraints | PK/FK   | Notes |
|-----------------------|--------|-------------------------------------------|------------|---------|-------|
| skill_id              | STRING | Self-explanatory.                         |            | PK      |       |
| also_part_of_skill_id | STRING | ID of the Skill that skill_id is part of. |            | PK / FK |       |
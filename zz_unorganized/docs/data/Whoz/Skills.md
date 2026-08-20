# Skill

From introduction, "a skill represent the individual known ability that a user can add to a profile."

| Column        | Type   | Description                         | Contraints               | PK/FK | Notes |
|---------------|--------|-------------------------------------|--------------------------|-------|-------|
| id            | STRING | Identifier of the skill.            |                          | PK    |       |
| type          | STRING | Type of the skill.                  |                          |       |       |
| granularity   | STRING | A skill or a group of skills.       | Literal: SKILL \| DOMAIN |       |       |
| removed       | BOOL   | Whether the skill has been removed. |                          |       |       |
| nature        | STRING | We don't know what this is.         | Literal: Skill \| Tom    |       |       |
| parent_id     | STRING | Parent Skill ID.                    |                          | FK    |       |
| federation_id | STRING | Federation ID.                      |                          | FK    |       |
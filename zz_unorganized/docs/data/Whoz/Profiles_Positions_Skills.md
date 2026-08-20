# Profiles, Positions and Skill.

This table holds the skills that each position has, named as aptitudes.

| Column      | Type   | Description                                        | Contraints | PK/FK   | Notes |
|-------------|--------|----------------------------------------------------|------------|---------|-------|
| profile_id  | STRING | ID of the profile.                                 |            | PK / FK |       |
| position_id | STRING | ID of this position for this profile.              |            | PK      |       |
| skill_id    | STRING | ID of the skill in this position for this profile. |            | PK / FK |       |
| concept_id  | STRING | We don't know what this is.                        |            | FK      |       |
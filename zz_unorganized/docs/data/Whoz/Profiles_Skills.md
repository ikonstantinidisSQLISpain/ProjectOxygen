# Profile and skill

This holds the "aptitudes" data, the relationship between a profile and its skills. Since the profile has skill ratings, the rate has been added here.

| Column                | Type   | Description                                                 | Contraints | PK/FK   | Notes                     |
|-----------------------|--------|-------------------------------------------------------------|------------|---------|---------------------------|
| profile_id            | STRING | ID of the profile.                                          |            | PK / FK |                           |
| skill_id              | STRING | ID of the skill.                                            |            | PK / FK |                           |
| concept_id            | STRING | We don't know what this is.                                 |            | FK      |                           |
| cumulative_experience | INT    | Self-explanatory.                                           |            |         | This might be calculated. |
| proficiency           | INT    | Self-explanatory.                                           |            |         |                           |
| visibility            | STRING | Self-explanatory.                                           |            |         |                           |
| augmented_with_ai     | BOOL   | Self-explanatory.                                           |            |         |                           |
| rating                | FLOAT  | The rate that the user added to this skill in this profile. |            |         |                           |


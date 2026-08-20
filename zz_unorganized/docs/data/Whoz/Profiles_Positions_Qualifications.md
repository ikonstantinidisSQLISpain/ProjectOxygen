# Profiles, Positions and Qualifications

This table holds the qualifications each position has in each profile.

| Column           | Type   | Description                                                | Contraints | PK/FK   | Notes |
|------------------|--------|------------------------------------------------------------|------------|---------|-------|
| profile_id       | STRING | ID of the profile.                                         |            | PK / FK |       |
| position_id      | STRING | ID of this position for this profile.                      |            | PK      |       |
| qualification_id | STRING | ID of the qualification in this position for this profile. |            | PK / FK |       |
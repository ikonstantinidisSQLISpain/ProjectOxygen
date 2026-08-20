# Profiles and positions

Holds the relationship between a profile and its positions.

| Column             | Type   | Description                           | Contraints | PK/FK   | Notes                                         |
|--------------------|--------|---------------------------------------|------------|---------|-----------------------------------------------|
| profile_id         | STRING | ID of the profile.                    |            | PK / FK |                                               |
| position_id        | STRING | ID of this position for this profile. |            | PK      |                                               |
| title              | STRING | Title of the position.                |            |         |                                               |
| company_name       | STRING | Self-explanatory.                     |            |         |                                               |
| current            | BOOL   | Whether this is the current position. |            |         |                                               |
| description        | STRING | Description of this position.         |            |         |                                               |
| is_mission         | BOOL   |                                       |            |         |                                               |
| start_date         | DATE   | Self-explanatory.                     |            |         |                                               |
| end_date           | DATE   | Self-explanatory.                     |            |         |                                               |
| employer_name      | STRING | Self-explanatory.                     |            |         |                                               |
| mission_context    | STRING | Description for the mission.          |            |         |                                               |
| mission_name       | STRING | Name of the mission.                  |            |         | A mission is a subobjective within a company. |
| parent_position_id | STRING | Self-explanatory.                     |            | FK      |                                               |
# Profile

It is similar to a digital CV.

| Column                  | Type    | Description                                                            | Contraints                               | PK/FK | Notes                                               |
|-------------------------|---------|------------------------------------------------------------------------|------------------------------------------|-------|-----------------------------------------------------|
| id                      | STRING  | ID of the profile.                                                     |                                          | PK    |                                                     |
| talent_id               | STRING  | ID of the talent.                                                      | Not nullable.                            | FK    |                                                     |
| version_name            | STRING  | Name of the profiles version.                                          |                                          |       |                                                     |
| main                    | BOOL    | Whether this profiles is the main or not.                              |                                          |       |                                                     |
| content_language        | STRING  | Self-explanatory.                                                      |                                          |       |                                                     |
| federation_id           | STRING  | Self-explanatory.                                                      |                                          | FK    |                                                     |
| completion_details      | VARIANT | No clue what this is exactly.                                          |                                          |       | Variant cause there are a lot of keys and sub-keys. |
| completion_rate         | FLOAT   | The percentage of profiles completion.                                 |                                          |       |                                                     |
| headline                | STRUCT  | More metadata about the profile.                                       |                                          |       | I'm not typing the struct here.                     |
| created_by              | STRING  | User ID. Self-explanatory.                                             |                                          | FK    |                                                     |
| created_date            | DATE    | Self-explanatory.                                                      |                                          |       |                                                     |
| last_explicit_update    | DATE    | Self-explanatory.                                                      |                                          |       |                                                     |
| last_explicit_update_by | STRING  | User ID. Self-explanatory.                                             |                                          | FK    |                                                     |
| last_modified_by:       | STRING  | User ID. Self-explanatory.                                             |                                          | FK    |                                                     |
| last_modified_date      | DATE    | Self-explanatory.                                                      |                                          |       |                                                     |
| permission_scope        | STRING  | No idea what this is.                                                  | Literal: SECRET                          |       |                                                     |
| removed                 | BOOL    | Self-explanatory.                                                      |                                          |       |                                                     |
| resume_relation_status  | STRING  | Status of process of creation of profile from CV.                      | Nullable                                 |       |                                                     |
| status                  | STRING  | Status of the creation of the profile.                                 | Literal: DRAFT \| SUBMITTED \| VALIDATED |       |                                                     |
| travel_range            | STRING  | Only one value has been seen, not enough to know exactly what this is. | Literal: DEFAULT                         |       |                                                     |
| hobbies                 | STRING  | Self-explanatory. Long String. Could be anything.                      |                                          |       |                                                     |




There are more columns, that has been skipped since they had no data.
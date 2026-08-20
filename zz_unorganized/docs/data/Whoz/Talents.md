# Talent

From introduction "a talent is the object used to store the data that recruiters need about the potential future workers during both the recruitment phase and the phase as employee."


| Column                                        | Type    | Description                                    | Contraints      | PK/FK | Notes                                                                                   |
|-----------------------------------------------|---------|------------------------------------------------|-----------------|-------|-----------------------------------------------------------------------------------------|
| id                                            | STRING  | ID of the talent.                              |                 | PK    |                                                                                         |
| federation_id                                 | STRING  | Self-explanatory.                              |                 | FK    |                                                                                         |
| user_id                                       | STRING  | The purpose of this is unknown.                |                 | FK    | Note 1                                                                                  |
| created_by                                    | STRING  | The user that created this talent.             |                 | FK    |                                                                                         |
| created_date                                  | DATE    | Self-explanatory.                              |                 |       |                                                                                         |
| last_modified_by                              | STRING  | User ID. Self-explanatory.                     |                 | FK    |                                                                                         |
| last_modified_date                            | DATE    | Self-explanatory.                              |                 |       |                                                                                         |
| last_connection_date                          | DATE    | Self-explanatory.                              |                 |       |                                                                                         |
| permission_scope                              | STRING  | No clue what this is.                          | Literal: SECRET |       |                                                                                         |
| time_entry_preferred_unit                     | STRING  |                                                |                 |       |                                                                                         |
| freely_assignable                             | BOOL    |                                                |                 |       |                                                                                         |
| end_user                                      | BOOL    | Whether the user_id is and end_user. Probably. |                 |       |                                                                                         |
| staffable                                     | BOOL    |                                                |                 |       |                                                                                         |
| last_invitation_date                          | DATE    |                                                |                 |       |                                                                                         |
| initials                                      | STRING  |                                                |                 |       |                                                                                         |
| manager_id                                    | STRING  | Talent ID. Self-explanatory.                   |                 | FK    |                                                                                         |
| working_life_entry_date                       | DATE    | Self-explanatory.                              |                 |       |                                                                                         |
| years_of_experience                           | INT     | Self-explanatory.                              |                 |       |                                                                                         |
| removed_date                                  | DATE    |                                                |                 |       |                                                                                         |
| gender                                        | STRING  | Self-explanatory.                              |                 |       |                                                                                         |
| phone_number                                  | STRING  | Self-explanatory.                              |                 |       |                                                                                         |
| mentor_id                                     | STRING  | Talent ID. Self-explanatory.                   |                 | FK    |                                                                                         |
| external_id                                   | STRING  | No clue to what.                               |                 | FK    |                                                                                         |
| photo                                         | STRUCT  | Photo for this talent.                         |                 |       | Keys: name, content_type, size, uid, empty                                              |
| language                                      | STRING  | Self-explanatory.                              |                 |       |                                                                                         |
| birth_date                                    | DATE    | Self-explanatory.                              |                 |       |                                                                                         |
| availability_confirmation_date                | DATE    | Self-explanatory.                              |                 |       |                                                                                         |
| availability_confirmation_date_last_update_by | STRING  | User ID. Self-explanatory.                     |                 | FK    |                                                                                         |
| remote_work                                   | BOOL    | Full remote work.                              |                 |       |                                                                                         |
| recruitment                                   | VARIANT |                                                |                 |       | Keys: stage_dates, workflow_step_dates, sourcer_id, recruiter_id. Both ids are talents. |



* Note 1: The theory is that the talent is almost equivalent to worker or potential worker. User is the actual whoz user if it have it.
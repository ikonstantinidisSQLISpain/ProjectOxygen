# User

From introduction, "it represents the individual user from Whoz platform."

| Column               | Type   | Description                                                                 | Contraints | PK/FK | Notes                |
|----------------------|--------|-----------------------------------------------------------------------------|------------|-------|----------------------|
| id                   | BIGINT | Identifier of the user.                                                     |            | PK    |                      |
| idp_id               | STRING | Identity provider ID, in case the log in is done with a different provider. |            |       |                      |
| enabled              | BOOL   | Whether the user is enabled.                                                |            |       |                      |
| username             | STRING | Self-explanatory.                                                           |            |       |                      |
| created_by           | BIGINT | ID of user that created this user.                                          | Nullable   | FK    | Self referencing FK. |
| created_date         | DATE   | Date of creation of this user.                                              |            |       |                      |
| last_connection_date | DATE   | Self-explanatory.                                                           |            |       |                      |
| last_modified_by     | BIGINT | ID of user that made the last modification to this user.                    |            | FK    |                      |
| last_modified_date   | DATE   | Self-explanatory.                                                           |            |       |                      |
| removed              | BOOL   | Whether this user has been removed.                                         |            |       |                      |
| language             | STRING | The language of the UI.                                                     |            |       |                      |
| theme                | STRING | The theme of the UI.                                                        |            |       |                      |
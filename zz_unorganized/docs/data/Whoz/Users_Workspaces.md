# User and workspace

This table holds the relation many to many between a user and a workspace. Adding the roles, a multi-valued attribute.

| Column                | Type   | Description       | Contraints | PK/FK | Notes                                                                         |
|-----------------------|--------|-------------------|------------|-------|-------------------------------------------------------------------------------|
| user_id               | STRING | Self-explanatory. |            | PK/FK |                                                                               |
| workspace_id          | STRING | Self-explanatory. |            | PK/FK |                                                                               |
| workspace_external_id | STRING | Self-explanatory. |            | PK/FK |                                                                               |
| role                  | STRING | Self-explanatory. |            | PK    | For each workspace, a user may have multiple roles. That is why this is a PK. |
# User and federation

Same as with user and workspace, this table holds the relationship many to many between users and federations.

| Column        | Type   | Description       | Contraints | PK/FK | Notes                                                                          |
|---------------|--------|-------------------|------------|-------|--------------------------------------------------------------------------------|
| user_id       | STRING | Self-explanatory. |            | PK/FK |                                                                                |
| federation_id | STRING | Self-explanatory. |            | PK/FK |                                                                                |
| role          | STRING | Self-explanatory. |            | PK    | For each federation, a user may have multiple roles. That is why this is a PK. |
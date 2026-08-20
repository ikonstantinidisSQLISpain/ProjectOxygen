# User and former usernames.

One of the attributes of the original data is `former_usernames` that is a list of usernames, a list needs to be translated into another table.

| Column          | Type   | Description                              | Contraints | PK/FK | Notes                                                                               |
|-----------------|--------|------------------------------------------|------------|-------|-------------------------------------------------------------------------------------|
| user_id         | STRING | Self-explanatory.                        |            | PK/FK |                                                                                     |
| username_index  | BININT | Position in list of the former username. |            | PK    | It is said as position in list because the data for this table is read from a list. |
| former_username | STRING | Former username of the user.             |            |       |                                                                                     |
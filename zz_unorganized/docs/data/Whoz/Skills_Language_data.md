# Skills metadata.

This table holds the data about skills that might be in different languages.

| Column         | Type   | Description                                 | Contraints | PK/FK | Notes |
|----------------|--------|---------------------------------------------|------------|-------|-------|
| skill_id       | STRING | Self-explanatory.                           |            | PK/FK |       |
| language       | STRING | Language code.                              |            | PK    |       |
| name           | STRING | Name of that skill in that language.        | Nullable   |       |       |
| description    | STRING | Description of that skill in that language. | Nullable   |       |       |
| terms          | STRING |                                             | Nullable   |       |       |
| hidden_terms   | STRING |                                             | Nullable   |       |       |
| wikipedia_link | STRING |                                             | Nullable   |       |       |
| depiction      | STRING |                                             | Nullable   |       |       |


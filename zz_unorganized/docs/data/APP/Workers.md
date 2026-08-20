# Worker

A worker is equivalent to employee.

| Column                   | Type    | Description                              | Contraints | PK/FK | Notes                                                                                                |
|--------------------------|---------|------------------------------------------|------------|-------|------------------------------------------------------------------------------------------------------|
| id                       | BIGINT  | Identifier of the worker.                |            |   PK  |                                                                                                      |
| active                   | BOOL    | If the worker is active or not.          |            |       |                                                                                                      |
| start_date               | DATE    | Start date of the worker.                |            |       |                                                                                                      |
| mail                 | STRING  | mail of the worker.                |            |       |                                                                                                      |
| seniority_date           | TINYINT | Time since hire. Calculated.             |            |       |                                                                                                      |
| job_title                | STRING  | Job title of the worker.                 |            |       |                                                                                                      |
| fulltime_or_parttime     | STRING  | Self-explanatory                         |            |       | STRING until the value type is known                                                                 |
| productivity_coefficient | STRING  | For now, just some metrics.              |            |       | STRING until the value type is known                                                                 |
| gcm_job                  | STRING  | Global Category Manager Job.             |            |       |                                                                                                      |
| gcm_grade                | STRING  | If it is Junior, Mid, Senior             |            |       |                                                                                                      |
| gcm_job_family           | STRING  | Category of gcm_job                      |            |       |                                                                                                      |
| std_cost                 | VARIANT | JSON cause it is better than flattening. |            |       | amount_in_euros, amount_in_local_currency, bu_coefficient, profile                                   |
| tariff                   | VARIANT | JSON cause it is better than flattening. |            |       | amount_in_euros, amount_in_local_currency, local_currency, local_currency_conv, local_currency_rate  |
| skill                    | VARIANT |                                          |            |       | code, description_en, description_fr                                                                 |
| position                 | VARIANT |                                          |            |       | code, active, description_en, description_fr                                                         |
| employee_category        | VARIANT |                                          |            |       | code, active, description_en, description_fr                                                         |
| bu_id                    | BIGINT  | Unit that this person manages.           |            | FK    |                                                                                                      |
| site_id                  | BIGINT  | Site where this person works.            |            | FK    |                                                                                                      |
| tl_rh                    | BIGINT  | ID of associated Team Leader or RRHH.    | != id      | FK    | An ID from same table.                                                                               |
| direct_manager           | BIGINT  | ID of superior manager.                  | != id      | FK    | An ID from same table.                                                                               |
| -------                  |         |                                          |            |       | service_line and department has been discarded since they are portraited in the collab_status table. |

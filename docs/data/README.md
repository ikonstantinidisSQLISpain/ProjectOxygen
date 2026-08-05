# Data

The raw data consist of a series of JSON and Excel files. The Excel file on fone hand contain information about Opportunities, a concept that will be developed later, and a few of different sources. Regarding the JSON files they have information from different databases, such as SQLi APP, or SQLi WHOZ.

This data can be categorized depending on the source or their category. The options are the next:

* Source:
    * APP
    * WHOZ
    * External
* Category:
    * Corporate Structures
    * People
    * Tasks or work
    * People Skills (WHOZ)


## Corporate Structures

The corporate structure category groups all the information related to the company's organization schema and hierarchy.

From top to bottom level the company is structure in the next concepts:

* Society: It is a group of different Business Units.
* Entity: It is a legal entity.
* Business Unit: It is a subdivision of the entity that acts as an independent entity.

This are related as follows:

* A business unit belongs to an entity, and an entity may have multiple business units.
* A business unit belongs to a society, and a society may have multiple business units.

## People

In this category fall everything related to people and their organization estructure.

* Workers:
* Site:
* department: It is a group of workers.

There are the next relations:

* A worker work at one specific site.
* A worker does not necessarily belong to a specific department.

Additional relations with the previous entities are:

* A worker works at a specific business unit (BU)


## Task or work

This category contains everythin related to the task and their performance.

* service_line: Represents a set of services that are provided to the clients.
* zone: It is kept here since there is no apparent relation to anything else yet.
* collab_status: Represent the status of the collaboration between the workers in the different departments working over the different service lines over time. This can be read as the information of each worker at each time (month) working on each service line under each department.
* activity: This is a task that is being reflected in the timesheet
* timesheet: This is the time dedicated to each activity reported by each worker at each month on each activity.
* leave_report: Self explanatory.

The relations are the next:

* A service_line could be "done" in multiple zones:
* A timesheet record has 1 activity, but one activity may be in different records.
* An activity may have a service_line and a department.
* The relations in collab_status are clear
* A service line may have multiple zones.


Relations with previous entities:

* A timesheet is filled by a worker.
* An activity is done by a department.
* A department may have multiple zones.
* A leave report is done by a worker.


## Relational model


### Corporate Structure

#### BU

| Column     | Type    | Description                    | Contraints | PK/FK |
|------------|---------|--------------------------------|------------|-------|
| id         | BIGINT  | Identifier of the unit.        |            |   PK  |
| active     | BOOL    | If the unit is active or not.  |            |       |
| type       | LITERAL | The type of the unit.          | BU \| CU   |       |
| symbole    | STRING  | Code that identifies the unit. |            |       |
| name       | STRING  | Name of the unit.              |            |       |
| calendar   | STRING  | Code that identifies the unit. |            |       |
| society_id | BIGINT  |                                |            |   FK  |
| entity_id  | BIGINT  |                                |            |   FK  |

#### Society

| Column | Type   | Description                      | Contraints | PK/FK |
|--------|--------|----------------------------------|------------|-------|
| id     | BIGINT | Identifier of the society.       |            |   PK  |
| name   | STRING | Name of the society.             |            |       |
| active | BOOL   | If the society is active or not. |            |       |

#### Entity

| Column | Type   | Description                      | Contraints | PK/FK |
|--------|--------|----------------------------------|------------|-------|
| id     | BIGINT | Identifier of the entity.        |            |   PK  |
| name   | STRING | Name of the entity.              |            |       |
| active | BOOL   | If the entity is active or not.  |            |       |


### People

#### Site

| Column | Type   | Description                   | Contraints | PK/FK |
|--------|--------|-------------------------------|------------|-------|
| id     | BIGINT | Identifier of the site.       |            |   PK  |
| name   | STRING | Name of the site.             |            |       |
| active | BOOL   | If the site is active or not. |            |       |

#### Zone

| Column | Type   | Description                   | Contraints | PK/FK |
|--------|--------|-------------------------------|------------|-------|
| id     | BIGINT | Identifier of the zone.       |            |   PK  |

#### Department

| Column | Type   | Description                          | Contraints | PK/FK |
|--------|--------|--------------------------------------|------------|-------|
| id     | BIGINT | Identifier of the department.        |            |   PK  |
| name   | STRING | Name of the department.              |            |       |
| code   | STRING | Code that identifies the department. |            |       |
| active | BOOL   | If the department is active or not.  |            |       |


##### Department_Zone

The relationsip between each department and each zone.

| Column        | Type   | Description                   | Contraints | PK/FK |
|---------------|--------|-------------------------------|------------|-------|
| department_id | BIGINT | Identifier of the department. |            | PK FK |
| zone_id       | BIGINT | Identifier of the zone        |            | PK FK |


#### Worker

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


### Work

#### service_line

| Column | Type   | Description                          | Contraints | PK/FK |
|--------|--------|--------------------------------------|------------|-------|
| id     | BIGINT | Indetifier of the service line       |            | PK    |
| code   | STRING | Code identifier of the service line  |            |       |
| active | BOOL   | If the service line is active or not |            |       |
| name   | STRING | Name of the service line             |            |       |

##### service_line_zone

| Column          | Type   | Description                    | Contraints | PK/FK |
|-----------------|--------|--------------------------------|------------|-------|
| service_line_id | BIGINT | Identifier of the service line |            | PK FK |
| zone_id         | BIGINT | Identifier of the zone         |            | PK FK |

##### service_line_department

| Column          | Type   | Description                    | Contraints | PK/FK |
|-----------------|--------|--------------------------------|------------|-------|
| service_line_id | BIGINT | Identifier of the service line |            | PK FK |
| department_id   | BIGINT | Identifier of the department   |            | PK FK |


#### collab_status

| Column          | Type    | Description                                                | Contraints | PK/FK |
|-----------------|---------|------------------------------------------------------------|------------|-------|
| worker_id               | BIGINT  | Identifier of the worker.                                  |            | PK FK |
| department_id           | BIGINT  | Indentifier of the department where the worker is working. |            | PK FK |
| service_line_id         | BIGINT  | Identifier of the service line the worker is working on.   |            | PK FK |
| year                    | INT     | Year when this data is measured.                           |            | PK    |
| month                   | TINYINT | Month when this data is measured.                          |            | PK    |
| status                  | STRING  | Status when the data was measured.                         |            |       |
| standard_cost_category  | STRING  | Self explanatory.                                          |            |       |

#### activity

Activity is not an entity you can find in the json but it is something we should extract as an indepent table for better readability.

| Column          | Type   | Description                                     | Contraints | PK/FK |
|-----------------|--------|-------------------------------------------------|------------|-------|
| type_id         | STRING | Identifier of the activity.                     |            | PK    |
| type_lib        | STRING | Label of the activity.                          |            |       |
| activity        | STRING | Code that categorize the activity.              |            |       |
| is_project      | BOOL   | If the activity is billable or not.             |            |       |


##### activity_service_line

An activity may have multiple service lines, and a service line may have multiple activities.

| Column          | Type   | Description                                     | Contraints | PK/FK |
|-----------------|--------|-------------------------------------------------|------------|-------|
| type_id         | STRING | Identifier of the activity.                     |            | PK FK |
| service_line_id | BIGINT | The service line which the activity belongs to. |            | PK FK |


##### activity_department

An activity may have multiple departments, and a department may have multiple activities.

| Column          | Type   | Description                                     | Contraints | PK/FK |
|-----------------|--------|-------------------------------------------------|------------|-------|
| type_id         | STRING | Identifier of the activity.                     |            | PK FK |
| department_id   | BIGINT | The department which the activity belongs to.   |            | PK FK |

#### timesheet

| Column           | Type    | Description                               | Contraints | PK/FK |
|------------------|---------|-------------------------------------------|------------|-------|
| worker_id        | BIGINT  | Identifier of the worker.                 |            | PK FK |
| year             | INT     | Year when the data is measured.           |            | PK    |
| month            | TINYINT | Month when the data is measured.          |            | PK    |
| activity_type_id | STRING  | Identifier of the activity.               |            | PK FK |
| imputation       | INT     | Amount of time dedicated to the activity. |            |       |

#### leave_report

| Column          | Type   | Description                             | Contraints | PK/FK |
|-----------------|--------|-----------------------------------------|------------|-------|
| worker_id       | BIGINT | Identifier of the worker.               |            | PK FK |
| departure_date  | DATE   | Date when the leave is requested.       |            | PK    |
| departure_type  | STRING | Self explanatory.                       |            |       |
| date_of_receipt | DATE   | Date when the leave was notified.       |            |       |
| user_category   | STRING | Category of worker not in workers data. |            |       |

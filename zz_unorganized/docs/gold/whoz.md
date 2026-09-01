# Whoz

Though neamed like this, it involves data from both Whoz and APP sources. The dashboards provided by this gold layer provide the analysis about the company's skills capacity aswell as the talen's whoz profile characteristics (completion, details, activity).

## Workers profiles

This table provides the needed data to know each talents profile evolution and current state. The table is aswell splitted into two in order to provide on one side a completion summary and on the other side the profiles details summary.

* **Table name:** Workers profile
* **Description:** Provides the neccessary information to know each talent's main profile evolution.
* **Business value:**
    * Provides insight on talents commitment to the platform.
    * Provides **valuable insight** on what personal needs to be redistributed, in order to optimze the projects performance.
    * In other words, it reduces cost and time by not wasting unnecessary time into analyzing if a project can be done using current workforce or there will be a need to acquire more workfoce.
* **Sources:** Collab_Status, Workers, Leaves, Zones, Department_Service_line_Zones, Users, Talents, Profiles
* *Steps to generate:*
    1. Take Workers, Leaves, collaboration and zone from APP.
        * For zone it is neccesary to add the names of each zone.
    2. Join this 4 tables by worker and date.
        * This allows to know which workers are working and on which department and service line.
        * For *collab_status* it sets the status values:
            * 0 => Employee
            * 1 => Regular
            * 2 => Subcontractor
            * 3 => Trainee
            * 4 or 5 => Compte Technique
    3. Take Users, Talents and profiles from Whoz.
        1. For **profile** calculate the `completionRateBucket`. This means the `completionRate` in five buckets of ten. (the value is a % and it is between 0 and 100). Add the additional column, *status* indicating completed or not completed, where completed means greater than 80%
        2. For **profile**, filter the main one and those that has not been removed.
        3. For **talent** selecciona los que no están eliminados.
        4. For **talent** get the number of days since last conection and bucket it.
        5. For **user** filters out those that start as `unknown` or `gdpr_request`.
        6. For **talent** classify seniority level based on **years of experience**
        7. For **profile** we need to calculate the score and the individual conditions.
    4. Join them by user, filtering first the talents that have a user linked. All the relations are 1 to 1, a user has a talent and has exactly one profile (the main one).
    5. Now join the Whoz with APP using the mail.
    6. Keep the next columns:
        * Date
        * user_id (collab_status)
        * user_mail (Users)
        * user_name (APP worker)
        * user_tlrh (APP worker) and mail
        * user_service_line (collab_status)
        * user_department (collab_status)
        * user_talent_leader (Talents) and mail
        * user_zone (Service_line_Zone_Department)
        * user_site (APP Worker)
        * user_country (APP site)
        * Seniority
        * Collab Status
        * Employee Type (collab_status)
        * Profiles completion checks and scores
        * CompletionRate bucket
        * last_user_connection (Users)
        * last_modified_by (Profiles)
        * profile_status (Profiles)
        * gcm_job (APP Workers gcm)
        * gcm_job_family (APP Workers gcm)



| Column                         | Description                                                         | Business value                                                                                                                  | Type  | Calculation                                                                        |
|--------------------------------|---------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------|-------|------------------------------------------------------------------------------------|
| Date                           | Date when the profile data was stored.                              | Time granularity detail.                                                                                                        | Field | Inherited from profiles history.                                                   |
| user_id                        | ID of the talent.                                                   | It is needed for the correct counts.                                                                                            | Field | Inherited from talent's user.                                                      |
| name                           | Name of the talent.                                                 | It is used for better visualization.                                                                                            | Field | Inherited from worker.                                                             |
| mail                           | Mail of the talent.                                                 | It is used for communication.                                                                                                   | Field | Inherited from worker.                                                             |
| tl_rh                          | Team lead or Human resource manager.                                | It is used for identification.                                                                                                  | Field | Inherited from worker.                                                             |
| tl_rh_mail                     | Team lead or Human resource manager mail.                           | It is used for communications.                                                                                                  | Field | Inherited from worker. Must be obtained by referencing the ID.                     |
| talent_leader                  | Talent Leader of this talent.                                       | It provides information about who is the Whoz talent leader of this user.                                                       | Field | Inherited from talent. The name must be obtained using the ID reference.           |
| talent_leader_mail             | Talent's leader mail.                                               | It is used for communications.                                                                                                  | Field | Inherited from talents.                                                            |
| service_line                   | User current service line.                                          | It provides information about what is the user working on.                                                                      | Field | Inherited from collab_status. The name must be obtained joining with service line. |
| department                     | User current department.                                            | It provides information about which department has this user.                                                                   | Field | Inherited from collab_status.                                                      |
| zone                           | User department and service line zone.                              | It provides information about which zone the department and service line is working on.                                         | Field | Inherited from department_service_line_zone mid table.                             |
| country                        | User current country.                                               | Geographic top granularity detail.                                                                                              | Field | Inherited from site.                                                               |
| site                           | User current site.                                                  | Geographic lowest granularity detail.                                                                                           | Field | Inherited from site.                                                               |
| seniority                      | User years of experience.                                           | Provides a summary of the experience level of the talent. Junior (<2 years), Intermediate (<5), Experience (<10), Senior (>10)  | Field | Calcualted from years of experience from talent.                                   |
| employee_type                  | Indicates what type of employee it is. Employee, Regular or Trainee | Provides employee type granularity level. Useful for making future decisions on the employee.                                   | Field | Inherited from collab_status.                                                      |
| profile_status                 | Indicates whether the profile is complete or not.                   | Allow to filter those profiles that hadn't been completed yet, providing a fine grained analysis on both groups.                | Field | From profile completion rate. Completed (>=80%), Incomplete (<80%).                |
| completion_rate_bucket         | Groups the completion rate in buckets of tweanties.                 | Provides a sufficient fine grained analysis of completion rate of employees, allowing for a more insightful dashboards.         | Field | From profile completion rate. Bucket = floor(completion_rate/5).                   |
| completion_details_checks      | This includes 16 extra columns that need to be defined yet.         | They provide what is missing in the profile to be completed. Useful to calculate the score.                                     | Field | Needs to be defined.                                                               |
| profile_score                  | A score calculated from checks columns.                             | It provides a summary of the talent's profile level of detail. Useful for selecting appropiate profiles.                        | Field | Needs to be defined.                                                               |
| completion_score_bucket        | Groups the profile score in buckets of tens.                        | Provides a sufficient fine grained analysisi of profile score of talents, allowing for more insightful dashboards.              | Field | From profile score. Bucket = floor(profile_score/10)                               |
| last_user_connection_date      | Last user connection date.                                          | Provides insight on user adoption of platform.                                                                                  | Field | Inherited from Users, if it has one.                                               |
| last_profile_modification_date | Last modification date of the profile.                              | Provides insight on the adoption of the platform.                                                                               | Field | Inherited from Profiles.                                                           |
| gcm_job_family                 | GCM Worker's Job Family.                                            | Job category top granularity of worker.                                                                                         | Field | Inherited from Workers.                                                            |
| gcm_job                        | GCM Worker's Job.                                                   | Job category lowest granularity of worker.                                                                                      | Field | Inherited from Workers.                                                            |


This table is later splitted and summarized into the next ones:

* **Organization_Tree:** Count of users grouping by the next columns.
    * Zone
    * Department
    * Service line
* **Geographical:** Count of users, grouping by the next columns.
    * Country
    * Site
* **Completion rate bucket:** Count of users, grouping by `completion_rate_bucket`
* **profile completion status:** Count of users, grouping by `profile_completion_status`
* **Profile score bucket:** Count of users, grouping by `profile_score_bucket`.
* **Profile score status:** Count of users, grouping by `profile_score_status`.
* **Profile status:** Count of users, grouping by `profile_status`.
    * Only `profiles` is needed.
* **Last_conection_date_bucket:** Count of users, grouping by `last_connection_date_bucket`.
* **last_modified_date_bucket:** Count of users, grouping by `last_modification_date_bucket`.
* **Global competency model tree:** Count of users, grouping by:
    * gcm_job_family
    * gcm_job


In all cases only one metric is calculated:

* **Headcount:** It represents the number of employees.
    * **Business value:** Headcount provides visibility into workforce size, distribution, and evolution over time. Combined with the available dimensions, it helps identify opportunities to redistribute and better utilize existing talent, reducing the need for additional hiring and therefore optimizing workforce costs.
* **Dimensions:** They indicate where the value of **headcounts** needs to be applied.
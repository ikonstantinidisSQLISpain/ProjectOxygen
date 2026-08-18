# Independent Objects

* BU: analytic
    * **id**
    * active
    * type
    * symbole
    * name
    * society: FK
    * entity: FK
    * manager: FK multi
    * calendar
* society: analytic
    * **id**
    * name
    * active
* entity: analytic
    * **id**
    * name
    * active
    * manager: FK multi
* manager:
    * id
* department:
    * **id**
    * code
    * active
    * name
    * associated_zone: FK multi
    * associated_service_line: FK multi
* service_line: analytic
    * **id**
    * code
    * active
    * name
    * associated_zone: FK multi
    * associated_practice_list: FK multi
* site: analytic
    * **id**
    * active
    * name
* timesheet_report: app
    * **id**
    * **year**
    * **month**
    * **uid**
    * bu: FK
    * activity: Doesnt seem a foreign key
        * **type_id**
        * activity: it is a code, very weird naming
        * type_lib
    * imputation
    * is_project: IDK if it is related to activity or to project
    * project_service_line_id: FK, uncertain but pretty sure it is the reference to middle table between project and service line
    * project_department_id: FK
    * user_service_line_id: FK
    * user_department_id: FK
* collab_status_report: person schema
    * **year**
    * **month**
    * **uid**
    * bu: FK
    * site: FK
    * status
    * standard_cost_category
    * service_line: FK
    * department: FK
* leave_report: person schema
    * **uid**
    * bu: FK
    * user_category: Should be a FK to contracts or similar
    * **departure_date**
    * departure_type
    * date_of_receipt
* workers
    * **id**
    * active
    * mail
    * start_date
    * seniority_date: seems calculated based on start date
    * job_title
    * productivity_coefficient
    * fulltime_or_parttime
    * gcm: Uncertain if it is a FK
        * grade
        * job
        * job_family
    * std_cost: Uncertain if it is a FK
        * amount_in_euros
        * amount_in_local_currency
        * bu_coefficient
        * profile
    * tariff:
        * amount_in_euros
        * amount_in_local_currency
        * local_currency
        * local_currency_conv
        * local_currency_rate
    * skill: Seems FK aswell
        * code
        * description_en
        * description_fr
    * position: Seems FK
        * code
        * active
        * description_en
        * description_fr
    * employee_category: Seems a FK
        * code
        * active
        * description_en
        * description_fr
    * bucu: FK, probably Business Unit Cost Unit
    * entity: FK
    * society: FK
    * site: FK
    * tl_rh: FK
    * direct_manager: FK
    * service_line: FK
    * department: FK
* certification_report: WHOZ
    * **id**:
    * profile_id: FK
    * ai_only:
    * aptitude_references: FK
    * created_by: Probably a FK, not in actual table
    * created_date
    * delivering_entity
    * last_modified_by: Probably a FK, not in actual table
    * obtention_date:
    * talent_id: FK
    * title:
    * workspace_id:
    * qualification_id: FK multi
    * attachment: FK, not included in the table
        * id
        * uid: I think this is a worker ID
        * content_type:
        * name
        * size
    * description
    * end_date
    * expiration_date
    * start_date
* aptitude:
    * **id**
    * concept_id: FK
    * name
    * type
* profile: WHOZ
    * **id**
    * talent_id: FK
    * aptitudes: FK Multi
    * version_name: No clue what this is
    * main
    * content_language
    * federation_id:
    * completion_details:
        * BIO_MIN_LENGTH:
            * satisfied
            * weight
        * EDUCATIONS_MIN_ONE:
            * satisfied
            * weight 
        * JOB_TITLE:
            * satisfied
            * weight
        * LANGUAGES_MIN_ONE:
            * satisfied
            * weight 
        * PROFESSIONAL_EXPERIENCES_ALL_COMPLETE:
            * satisfied
            * weight 
        * PROFESSIONAL_EXPERIENCES_MIN_ONE:
            * satisfied
            * weight
        * PROFILE_PICTURE:
            * satisfied
            * weight
        * SKILLS_ALL_WITH_PROFICIENCY:
            * satisfied
            * weight
        * SKILLS_MIN_FIFTEEN_COMPLETE:
            * satisfied
            * weight
        * SKILLS_MIN_FIVE_COMPLETE:
            * satisfied
            * weight
        * SKILLS_MIN_TEN_COMPLETE:
            * satisfied
            * weight
        * SKILLS_MIN_THREE_COMPLETE_HIGHLIGHTED:
            * satisfied
            * weight
        * WORKING_LIFE_ENTRY_DATE:
            * satisfied
            * weight
    * completion_rate:
    * completion_rate_last_computed_date:
    * custom_fields: FK Multi, all seem to be empty
    * functional_domains: FK Multi, all seem to be empty
    * headline: No clue if it is FK.
        * aim
        * international_mobility
        * job_title
        * mobility_date
        * mobility_destinations: FK multi, all seem to be empty or null
        * mobility_note
        * national_mobility
        * permission_scope
        * seeking_opportunities
        * seeking_opportunities_last_modified
        * mobility_destination
    * last_explicit_update
    * last_explicit_update_by: FK, uncertain
    * last_modified_by: FK, uncertain
    * last_modified_date:
    * permission_scope:
    * qualification_ids: FK multi
    * removed:
    * resume_relation_status:
    * schedules: FK multi, all seem to be empty
    * skill_ratings: FK multi
        * skill
        * rating
    * status
    * target_functional_domains: FK multi, all empty
    * target_skill_rating: FK multi, all empty
    * target_skills: FK multi, all empty
    * travel_range
    * positions: FK multi
    * hobbies
* talent: WHOZ
    * **id**
    * federation_id: FK, idk to what
    * profile: FK
    * user_id: FK
    * created_by: FK, maybe
    * created_date
    * last_modified_by: FK, maybe
    * removed
    * last_modified_date
    * last_connection_date
    * permission_scope
    * tags: FK multi
    * aspirations: FK multi, all seem to be empty
    * max_working_hours: FK multi, all seem to be empty
    * sharing_destinations: FK_multi, all seem to be empty
    * history: FK multi, maybe
        * since
        * scope
        * workspace_id: FK
        * unit_rate:
            * value
            * currency_code
        * unit_cost:
            * value
            * currency_code
        * unit_internal_rate:
            * value
            * currency_code
        * org_unit
        * grade_id
        * role_id
    * recruitment: Doesn't seem FK
        * stage_dates: Multi
        * workflow_step_dates
        * sourcer_id: FK, idk to what
        * recruiter_id: FK, idk what to
    * gender
    * phone_number
    * mentor_id: FK, idk what to
    * external_id: FK, idk what to
    * photo:
        * name
        * content_type
        * size
        * uid: I think it is worker ID
        * empty
    * language
    * birth_date
    * availability_confirmation_date
    * availability_confirmation_date_last_update_by: FK, maybe
    * remote_work
* aptitude:
    * **id**:
    * profile_id: Shouldn't have this FK
    * name
    * concept_id: FK, but idk to what
    * cumulative_experience
    * proficiency
    * talent_id: FK
    * type:
    * visibility:
    * augmented_with_ai:
* skill:
    * **id**
    * also_part_of: FK Multi
    * name: FK multi
        * text
        * language
    * description: FK multi
        * text
        * language
    * terms: FK multi
        * text
        * language
    * hiddent_terms: FK multi
        * text
        * language
    * wikipedia_link: FK multi
        * text
        * language
    * depiction: FK multi
        * text
        * language
    * type
    * granularity
    * removed
    * nature
    * parent
    * federation
* position
    * **id**
    * title
    * aptitude_references: FK multi
    * company_name
    * current
    * custom_fields: FK multi, all empty
    * description:
    * is_mission:
    * profile_id: FK, self referencing.
    * qualification_ids: FK multi
    * start_date:
    * end_date
    * employer_name
    * mission_context
    * mission_name
    * parent_position_id: FK
* user
    * **id**
    * idp_id: FK, maybe
    * enabled
    * username
    * former_usernames: Multi value
    * workspace_roles: FK multi
        * workspace_id: FK?
        * workspace_external_id: FK
        * roles: Multi, FK?
    * federation_roles: FK multi
    * agentic_studio_roles: multi
    * created_by: FK, maybe
    * created_date
    * last_connection_date
    * last_modified_by: FK, maybe
    * last_modified_date
    * removed
    * language
    * theme

## onetbp_worklogs

* uid: Worker ID probably
* worklogs:
    * date:
        * n:
            * Multi valued (list):
                * type
                * tbp_id
                * worklog
                * project_code
                * project_name
            * Dict base:
                * m:
                    * type
                    * tbp_id
                    * worklog
                    * project_code
                    * project_name
* available_date:
* worklogs_by_type:
    * date:
        * n:
            * abscence
            * project


- Both worklogs and worklogs by type have the same keys, a set of dates with format ("mm/YYYY").
- m and n are just random numbers
- 
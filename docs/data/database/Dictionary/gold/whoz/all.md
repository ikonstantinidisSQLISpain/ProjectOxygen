# Star Schema Proposal

## Grain

**One row** per User per Snapshot Date**

The source table appears to be a historical snapshot table, therefore the fact table should store one record for each user at each profile capture date.

---

# Table Classification

## FACT_PROFILE_SNAPSHOT

**Type:** Fact Table

**Grain:** One row per User and Snapshot Date

### Keys

- date_key
- user_id
- organization_key
- geography_key
- profile_key
- activity_key
- gcm_key

### Measures

- completion_rate
- profile_score

### Main KPI

- User Count = DISTINCTCOUNT(user_id)

---

## DIM_DATE

**Type:** Dimension

### Attributes

- date_key
- date
- year
- quarter
- month
- week

---

## DIM_USER

**Type:** Dimension

### Attributes

- user_id
- name
- mail
- tl_rh
- tl_rh_mail
- talent_leader
- talent_leader_mail
- seniority
- employee_type

---

## DIM_ORGANIZATION

**Type:** Dimension

### Attributes

- organization_key
- zone
- department
- service_line

### Used By

- Organization Tree

---

## DIM_GEOGRAPHY

**Type:** Dimension

### Attributes

- geography_key
- country
- site

### Used By

- Geographical

---

## DIM_PROFILE_STATUS

**Type:** Dimension

### Attributes

- profile_key
- profile_status
- profile_completion_status
- completion_rate_bucket
- completion_score_bucket
- completion_rate
- profile_score
- profile_score_status

### Optional Attributes

- completion_check_1
- completion_check_2
- ...
- completion_check_16

### Used By

- Completion Rate Bucket
- Profile Completion Status
- Profile Score Bucket
- Profile Score Status
- Profile Status

---

## DIM_ACTIVITY

**Type:** Dimension

### Attributes

- activity_key
- last_connection_date_bucket
- last_modification_date_bucket

### Used By

- Last Connection Date Bucket
- Last Modification Date Bucket

---

## DIM_GCM

**Type:** Dimension

### Attributes

- gcm_key
- gcm_job_family
- gcm_job

### Used By

- Global Competency Model Tree

---

# Dashboard Mapping

| Dashboard Aggregation | Dimension |
| --------------------- | --------- |
| Organization Tree | DIM_ORGANIZATION |
| Geographical | DIM_GEOGRAPHY |
| Completion Rate Bucket | DIM_PROFILE_STATUS |
| Profile Completion Status | DIM_PROFILE_STATUS |
| Profile Score Bucket | DIM_PROFILE_STATUS |
| Profile Score Status | DIM_PROFILE_STATUS |
| Profile Status | DIM_PROFILE_STATUS |
| Last Connection Date Bucket | DIM_ACTIVITY |
| Last Modification Date Bucket | DIM_ACTIVITY |
| Global Competency Model Tree | DIM_GCM |

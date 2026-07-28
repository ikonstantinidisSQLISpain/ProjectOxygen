USE CATALOG ${catalog};

CREATE SCHEMA IF NOT EXISTS bronze_organizations;
USE SCHEMA bronze_organizations;


CREATE TABLE IF NOT EXISTS bu(
    id BIGINT NOT NULL,
    name STRING,
    type STRING,
    symbole STRING,
    calendar STRING,
    active BOOL,
    society_id BIGINT NOT NULL,
    entity_id BIGINT NOT NULL,

    CONSTRAINT pk_bu PRIMARY KEY id,
)
USING DELTA;


CREATE TABLE
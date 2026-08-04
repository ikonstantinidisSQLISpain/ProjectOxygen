# Pipelines


## Overview bronze_app
This folder, named bronze_app, contains a set of Python scripts designed to read raw data from specific sources and load it into tables within a Databricks environment. The pipeline is structured to handle JSON files and transform them according to predefined schemas before being stored in the designated database schema with a quality level of "bronze".

### Files Description
1. worklogs.py
This script reads from a specific JSON file (onetbp_worklogs_anonymized.json) located at a specified path and transforms it according to a predefined schema, creating structured data that is then loaded into the target table in the database. The transformation includes parsing nested structures and handling different types of fields such as strings, maps, arrays, and variants.

2. bu.py
This script reads another JSON file (analytic_bu_anonymized.json) and extracts relevant business unit data into a structured format suitable for loading into the target table. It includes operations to infer schema from the raw data.

3. collab_status.py
Similar to other scripts, it reads a JSON file (perso_collab_status_report_anonymized.json) and extracts collaboration status data for loading into the appropriate table. It handles nested metadata fields appropriately.

4. departments_and_zones.py
This script processes multiple related JSON files to extract department and zone information, including handling of exploded arrays and unions where necessary. It creates two tables: departments and zones, as well as a join table between them.

5. entities.py
Extracts entity data from a JSON file and structures it for loading into the target table using Spark transformations.

6. service_line.py
Handles service line data extraction, including nested relations with zones and departments, through reading multiple related JSON files and transforming them according to required schema.

7. sites.py
Reads site data from a JSON file and structures it for loading into the target table after applying necessary transformations.

8. societies.py
Extracts society data from a JSON file, transforms it according to the required schema, and loads it into the target table.

9. timesheet_and_activities.py
Processes timesheet and activity data by reading related JSON files and transforming them for loading into appropriate tables in the database. This script handles multiple types of activities and their relations with service lines and departments.

10. worker.py
Reads worker-related data from a JSON file, including detailed nested structures such as business units, sites, and management roles. It transforms this data into a structured format suitable for loading into the target table.

### Usage
Each script is designed to be run within a Databricks environment where Spark is available. The scripts use PySpark functionalities to read JSON files from specified paths, apply necessary schema transformations, and load the transformed data into tables defined in the Databricks workspace.

### Configuration
Before running these scripts, ensure that configuration parameters such as read.catalog, target.schema, and read.schema are set appropriately in the Databricks environment or through script arguments if using a custom setup. These configurations determine where to read from and where to write to within the Databricks data lake.

### Dependencies
Ensure that all necessary PySpark libraries are available in your Python environment for these scripts to function correctly. Typically, this includes pyspark and related transformations modules like pyspark.sql.functions.

This setup provides a robust pipeline for handling raw JSON data from multiple sources into structured tables within Databricks, suitable for further analytical processing or reporting needs.


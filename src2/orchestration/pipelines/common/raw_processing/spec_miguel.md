# Raw Processing

## Objective

Provide the tools that allow the creation of the base bronze raw tables.

## Context

Currently most of the data is read from folders with multiple JSON files. These files are snapshot based and roughly contain the same data, in other words, the data is provided as a screenshot of the current state of that data in the database. This basically means the next:

* New rows may be added.
* Old elements (grains) might be modified in any of its columns (updated).
* Old elements may get removed.

Not all the data is managed by the company and is provided by third parties, such us whoz, and their schema may change over time. This means that the columns (or keys) might be added or removed over time or even renamed (in practice it is equivalent to remove one and add one, but conceptually the data aims to the same concept).

Additionally, after some analysis of the data it is shown that the values don't follow a strict type pattern, meaning that the type of the value is not always the same. For instance, in some cases where the value should be a dictionary (object) you may find an empty list when it should be a null.

## Asumptions

* The data is assumed to be in a single place and not splitted between different locations.
* The file names are assumed to maintain the same naming convention over time.

## Requirements

* It is required that the data is stored in a single place.
* It is required that the file names follow the same pattern.
* For the bronze layer it is mandatory to keep the data as close as possible to raw. This means that it is necessary to keep the original data splitted into different rows and as a single column with the original json data.
* It is required to analyze the schema, store the variations and log or inform before deciding what will be done with it in downstream consumers.
* It is required to provide a way to store who analyzed the new schemas and what decisions were taken about it.
* It is logged if the naming convention changes at any moment.


## Out of the scope

* It won't be checked if the data is splitted between different sources.
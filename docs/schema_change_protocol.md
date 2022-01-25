# Schema Change Protocol

This document describes the process for accommodating a JSON dataset schema change. There are two types of schema changes, _compatible_, when an existing Glue table schema can be updated to incorporate the new data into the existing parquet datasets, and _incompatible_, when we must create a new Glue table in order to read the JSON data into a data frame for relationalization and export to parquet. See also the accompanying diagram in the `diagrams` directory.

Author: Phil Snyder

## 1. Visually compare the new schema with the existing Glue table schema

- Do existing field datatypes change? Then these schemas are not compatible
    * Skip to 3g
- If fields are added, removed, or the datatypes stay the same the schemas may be compatible
    * Proceed to 2a

## 2. Schema changes will be tested using a new stack:

* a. This stack will be deployed like a new study, but without the SNS topic -> Lambda trigger.
* b. The Glue tables in this stack will need to have schemas which are compatible with both old and new JSON schemas
    * CFN templates need to be updated, on a separate branch, before stack deployment. If schema changes are extensive, we may need to use a crawler to discover a compatible schema (if it exists). In either case, this is done manually before stack deployment.
* c. Submit to `s3_to_json` workflow:
    * Archives with new schema (the backfill)
        - Archives with new schemas should be provided as annotated file entities in a separate Synapse project (in the same fashion as Bridge exporter 3.0).
    * A representative sample of archives with old schema data
        - To get our representative sample, we will reference the mapping file we will create between appVersion and datasets (more on this later). We will submit one archive from each appVersion that maps to the most recent dataset of this type (e.g., `weather_v2` if the weather JSON from the most recent build are being bucketed into the `weather_v2` dataset). We do this because although all JSON of this type are bucketed into the same dataset (and thus can be represented by a single Glue table), the actual JSON schemas may be different. We want to find a Glue table schema which works for all JSON schemas which map to the most recent dataset, if possible.
* d. Run `json_to_parquet` workflow for the relevant JSON dataset / Glue table
    * If the JSON dataset is able to be successfully loaded into a DynamicFrame and exported and read as parquet, our new schema is compatible with the older schemas
    * Otherwise, the schemas are not compatible

## 3. IF new schema is compatible with old schemas:

* a. Update the appVersion to dataset mapping to include the appVersion which will use the new schema.
* b. Archive existing and relevant parquet datasets.
    * This entails moving relevant datasets in `s3://app/study/parquet/` to an archive folder `s3://app/study/parquet/archive/{dataset}_{version}_{update_number}/` . For example, if the `taskData_v2` dataset has already undergone three compatible schema updates, then we move all `taskData_v2_*` parquet datasets to `s3://app/study/parquet/archive/taskData_v2_4/` for each study. `taskData_v2_1`, `taskData_v2_2`, and `taskData_v2_3` already exist in this archive folder because we have performed this step three times already in response to previous schema changes.
* c. Create or update the following AWS resources in the stack:
    * Update the Glue table schema for each affected study.
* d. Deploy new stack.
* e. Reset `json_to_parquet` job bookmark for this `{dataset} ` and`{dataset_version}`'s json_to_parquet job.
    * This will cause this job's next run to process every file in its respective JSON dataset.
* f. Delete existing parquet dataset. This dataset will be written again after the `json_to_parquet` job completes its run through the entire JSON dataset.

## ELSE IF new schema is not compatible
* g. This is highly problematic. Data from new builds will conform to the new schema, but we will still receive data from legacy builds -- which will conform to the old schema. How do we differentiate and process data with incompatible, mixed schemas?
    * The current preferred solution would be to consider the new schema as a distinct dataset.  We can include logic in the `s3_to_json` job which takes into account the `appVersion` when bucketing JSON from the archive into its JSON dataset. In this way we can construct JSON datasets with compatible schemas (they can be read as a DynamicFrame using a single Glue table schema as reference). For example, if the schema of `weather` changes in an incompatible way with the release of appVersion = 10, then any weather JSON from appVersion < 10 is put in `s3://bucket/study/raw_json/weather` and weather JSON from appVersion = 10 is put in `s3://bucket/study/raw_json/weather_v2`. This will require deploying a new Glue table, updating an existing crawler, and a new `json_to_parquet` job, in addition to the updates to the `json_to_parquet` workflow and the mapping file used by the `s3_to_json` job.
    * Another potential solution is to transform data with the old schema so that it matches the format of the new schema before depositing the newly formatted JSON into the JSON dataset bucket location. This has the added benefit of allowing us to maintain a single version of parquet datasets w.r.t. the corresponding JSON dataset, with the drawback that any incompatible schema changes require a not insignificant engineering effort to transform the old schema into the same format as the new schema. If there are multiple incompatible schema changes over a period of time (potentially years), then transformations need to be chained, or new transformation jobs need to be implemented for every dataset of this type. This does not scale well. Unfortunately, the preferred solution (versioning the datasets) simply pushes this harmonization work to the analysts. Ultimately, a fork of datasets is unavoidable when schemas change in an incompatible way, and resolving this fork is the price we pay for incompatible schema changes.
* h. Update the appVersion to dataset mapping to include the new appVersion and dataset version.
* i. Unlike in compatible schema changes, there is no need to archive any parquet datasets because we are creating a brand new parquet dataset, mutually disjoint from any preexisting parquet datasets.
* j. Create or update the following AWS resources in the stack:
    * Create a new Glue table for each affected study with name `dataset_{dataset_name}_{dataset_version}` over the new JSON dataset location. It's okay if these JSON dataset don't yet exist. This will have the same properties as our other Glue tables, but with a schema which is compatible with the new data.
    * Create a new Glue job named using the regular naming convention for our new dataset. This job should be parameterized to read from the `--table` which we created in the above step.
    * Add this job to the `start-json-to-parquet` trigger's actions for each affected study.
    * Update the appropriate crawler (either standard or array of records, depending on the format of the new schema) to include the new JSON dataset location in its scope.
* k. Deploy new stack

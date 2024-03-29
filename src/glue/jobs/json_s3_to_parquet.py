"""
This script runs as a Glue job and converts a collection of JSON files
(whose schema is defined by a Glue table), to a parquet dataset partitioned by
assessmentid / year / month / day. Additionally, if the table has nested data,
it will be separated into its own dataset with a predictable name. For example,
the info table (derived from info.json) has a field called "files" which is an
array of objects. We will write out two parquet datasets in this case, an `info`
dataset and an `info_files` dataset.

Before writing our tables to parquet datasets, we add the recordid,
assessmentid, year, month, and day to each record in each table.
"""

import os
import sys
import boto3

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark import SparkContext

def get_args():
    glue_client = boto3.client("glue")
    args = getResolvedOptions(
             sys.argv,
             ["WORKFLOW_NAME",
              "WORKFLOW_RUN_ID",
              "JOB_NAME",
              "table"])
    workflow_run_properties = glue_client.get_workflow_run_properties(
            Name=args["WORKFLOW_NAME"],
            RunId=args["WORKFLOW_RUN_ID"])["RunProperties"]
    return args, workflow_run_properties

def has_nested_fields(schema):
    """
    Determine whether a DynamicFrame schema has struct or array fields.

    If the DynamicFrame does not have fields of these data types, it is flat and
    can be written directly to S3.  If it does, then the DynamicFrame will need
    to be 'relationalized' so that all the data contained in the DynamicFrame has
    been flattened.

    Args:
        schema (awsglue.StructType): The schema of a DynamicFrame.

    Returns:
        bool: Whether this schema contains struct or array fields.
    """
    for col in schema:
        if col.dataType.typeName() == "array":
            return True
        elif col.dataType.typeName() == "struct":
            return True
    return False

def get_table(table_name, database_name, glue_context):
    """
    Return a table as a DynamicFrame with an unambiguous schema.

    Args:
        table_name (str): The name of the Glue table.
        database_name (str): The name of the Glue database

    Returns:
        awsglue.DynamicFrame
    """
    table = glue_context.create_dynamic_frame.from_catalog(
                 database=database_name,
                 table_name=table_name,
                 additional_options={"groupFiles": "inPartition"},
                 transformation_ctx="create_dynamic_frame")
    table = table.resolveChoice(
            choice="match_catalog",
            database=database_name,
            table_name=table_name)
    return table

def write_table_to_s3(dynamic_frame, bucket, key, glue_context):
    """
    Write a DynamicFrame to S3 as a parquet dataset.

    Args:
        dynamic_frame (awsglue.DynamicFrame): A DynamicFrame.
        bucket (str): An S3 bucket name.
        key (str): The key to write this DynamicFrame to.

    Returns:
        None
    """
    logger = glue_context.get_logger()
    s3_write_path = os.path.join("s3://", bucket, key)
    logger.info(f"Writing {os.path.basename(key)} to {s3_write_path}")
    glue_context.write_dynamic_frame.from_options(
            frame = dynamic_frame,
            connection_type = "s3",
            connection_options = {
                "path": s3_write_path,
                "partitionKeys": ["assessmentid", "year", "month", "day"]},
            format = "parquet",
            transformation_ctx="write_dynamic_frame")

def add_index_to_table(table_key, table_name, processed_tables, unprocessed_tables):
    """Add partition and index fields to a DynamicFrame.

    A DynamicFrame containing the top-level fields already includes the index
    fields, but DynamicFrame's which were flattened as a result of the
    DynamicFrame.relationalize operation need to inherit the index and partition
    fields from their parent. In order for this function to execute successfully,
    the table's parent must already have the index fields and be included in
    `processed_tables`.

    In addition to adding the index fields, this function formats the names
    of the (non-index) fields which were manipulated by the call to
    DynamicFrame.relationalize.

    Args:
        table_key (str): A key from the dict object returned by DynamicFrame.relationalize
        table_name (str): The name of the top-level parent table. All `table_key` values
            ought to be prefixed by this name.
        processed_tables (dict): A mapping from table keys to DynamicFrames which
            already have an index. Typically, this function will be invoked
            iteratively on a sorted list of table keys so that it is guaranteed
            that a child table may always reference the index of its parent table.
        unprocessed_tables (dict): A mapping from table keys to DynamicFrames which
        don't yet have an index.

    Returns:
        awsglue.DynamicFrame with index columns

    """
    this_table = unprocessed_tables[table_key].toDF()
    if table_key == table_name: # top-level fields already include index
        for c in list(this_table.columns):
            if "." in c: # a flattened struct field
                this_table = this_table.withColumnRenamed(
                        c, c.replace(".", "_"))
        df_with_index = this_table
    else:
        if ".val." in table_key:
            hierarchy = table_key.split(".val.")
            parent_key = ".val.".join(hierarchy[:-1])
            original_field_name = hierarchy[-1]
            parent_table = processed_tables[parent_key]
        else: # k is the value of a top-level field
            parent_key = table_name
            original_field_name = table_key.replace(f"{table_name}_", "")
            parent_table = unprocessed_tables[parent_key].toDF()
        parent_index = (parent_table
                .select(
                    [original_field_name, "assessmentid", "year",
                     "month", "day", "recordid"])
                .distinct())
        this_index = parent_index.withColumnRenamed(original_field_name, "id")
        df_with_index = this_table.join(
                this_index,
                on = "id",
                how = "inner")
        # remove prefix from field names
        field_prefix = table_key.replace(f"{table_name}_", "") + ".val."
        columns = list(df_with_index.columns)
        for c in columns:
            # do nothing if c is id, index, or partition field
            if f"{original_field_name}.val" == c: # field is an array
                succinct_name = c.replace(".", "_")
                df_with_index = df_with_index.withColumnRenamed(
                        c, succinct_name)
            elif field_prefix in c:
                succinct_name = c.replace(field_prefix, "").replace(".", "_")
                # If key is a duplicate we keep the original field name
                if succinct_name in df_with_index.columns:
                    continue
                df_with_index = df_with_index.withColumnRenamed(
                        c, succinct_name)
    return df_with_index

def main():
    # Get args and setup environment
    args, workflow_run_properties = get_args()
    glue_context = GlueContext(SparkContext.getOrCreate())
    logger = glue_context.get_logger()
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Get table info
    table_name = args["table"]
    table = get_table(
            table_name=table_name,
            database_name=workflow_run_properties["database"],
            glue_context=glue_context
    )
    table_schema = table.schema()

    # Export new table records to parquet
    if has_nested_fields(table_schema) and table.count() > 0:
        tables_with_index = {}
        table_relationalized = table.relationalize(
            root_table_name = table_name,
            staging_path = f"s3://{workflow_run_properties['parquet_bucket']}/tmp/",
            transformation_ctx="relationalize")
        # Inject partition fields (plus recordid) into child tables
        for k in sorted(table_relationalized.keys()):
            tables_with_index[k] = add_index_to_table(
                    table_key=k,
                    table_name=table_name,
                    processed_tables=tables_with_index,
                    unprocessed_tables=table_relationalized
            )
        for t in tables_with_index:
            clean_name = t.replace(".val.", "_")
            dynamic_frame_with_index = DynamicFrame.fromDF(
                    tables_with_index[t],
                    glue_ctx = glue_context,
                    name = clean_name
            )
            write_table_to_s3(
                    dynamic_frame=dynamic_frame_with_index,
                    bucket=workflow_run_properties["parquet_bucket"],
                    key=os.path.join(
                        workflow_run_properties["parquet_prefix"], clean_name),
                    glue_context=glue_context
            )
    elif table.count() > 0:
        write_table_to_s3(
                dynamic_frame=table,
                bucket=workflow_run_properties["parquet_bucket"],
                key=os.path.join(
                    workflow_run_properties["parquet_prefix"], args["table"]),
                glue_context=glue_context
        )
    job.commit()

if __name__ == "__main__":
    main()

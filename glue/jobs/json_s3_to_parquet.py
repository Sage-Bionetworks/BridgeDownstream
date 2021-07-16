# This script runs as a Glue job and converts a collection of JSON files
# (whose common schema is defined by a Glue table, created and maintained
# by a Glue crawler), to a parquet dataset partitioned by
# measure (taskIdentifier) / year / month / day / recordId
# Additionally, if the table has nested data, it will be separated out
# into its own dataset with a predictable name.
#
# For example, the info table (derived from info.json) has a field called
# "files" which is an array of objects. We will write out two parquet datasets
# in this case, an `info` dataset and an `info_files` dataset.
#
# Before writing our tables to parquet datasets, we will add the recordId
# measure (taskIdentifier), and year, month, day to each record in each table.

import sys
import os
from pyspark import SparkContext
from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
        sys.argv,
        ["database", "table-name", "s3-bucket-output", "s3-prefix-output"])
glueContext = GlueContext(SparkContext.getOrCreate())
table = glueContext.create_dynamic_frame.from_catalog(
             database=args["database"],
             table_name=args["table_name"])

def has_nested_fields(schema):
    for col in schema:
        if col.dataType.typeName() == "array":
            return True
        elif col.dataType.typeName() == "struct":
            return True
    return False

def write_to_partitioned_dataset(table, table_name, s3_bucket, s3_prefix):
    s3_write_path = os.path.join("s3://", s3_bucket, s3_prefix, table_name)
    table.write(
            connection_type = "s3",
            connection_options = {
                "path": s3_write_path,
                "partitionKeys": [
                    "taskIdentifier", "year", "month", "day", "recordid"]},
            format = "parquet")

if has_nested_fields(table.schema()):
    tables_with_index = {}
    table_relationalized = table.relationalize(
        root_table_name = args["table_name"],
        staging_path = f"s3://{args['s3_bucket_output']}/tmp/")
    # Inject partition fields into child tables
    for k in sorted(table_relationalized.keys()):
        this_table = table_relationalized[k].toDF()
        if k == args['table_name']: # top-level fields
            for c in list(this_table.columns):
                if "." in c: # a flattened struct field
                    this_table = this_table.withColumnRenamed(
                            c, c.replace(".", "_"))
            tables_with_index[k] = this_table
        else:
            if ".val." in k:
                hierarchy = k.split(".val.")
                parent_key = ".val.".join(hierarchy[:-1])
                original_field_name = hierarchy[-1]
                parent_table = tables_with_index[parent_key]
            else: # k is the value of a top-level field
                parent_key = args['table_name']
                original_field_name = k.replace(f"{args['table_name']}_", "")
                parent_table = table_relationalized[parent_key].toDF()
            parent_index = (parent_table
                    .select(
                        [original_field_name, "taskIdentifier", "year",
                         "month", "day", "recordId"])
                    .distinct())
            this_index = parent_index.withColumnRenamed(original_field_name, "id")
            df_with_index = this_table.join(
                    this_index,
                    on = "id",
                    how = "inner")
            # remove prefix from field names
            field_prefix = k.replace(f"{args['table_name']}_", "") + ".val."
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
                    else:
                        df_with_index = df_with_index.withColumnRenamed(
                                c, succinct_name)
            tables_with_index[k] = df_with_index
    for t in tables_with_index.keys():
        clean_name = t.replace(".val.", "_")
        dynamic_frame_with_index = DynamicFrame.fromDF(
                tables_with_index[t],
                glue_ctx = glueContext,
                name = clean_name)
        write_to_partitioned_dataset(
                table = dynamic_frame_with_index,
                table_name = clean_name,
                s3_bucket = args["s3_bucket_output"],
                s3_prefix = args["s3_prefix_output"])
else:
    write_to_partitioned_dataset(
            table = table,
            table_name = args["table_name"],
            s3_bucket = args["s3_bucket_output"],
            s3_prefix = args["s3_prefix_output"])

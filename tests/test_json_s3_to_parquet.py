import json
import os
import time
import boto3
import pandas
import pytest
from awsglue.context import GlueContext
from pyspark.sql.session import SparkSession
from src.glue.jobs.json_s3_to_parquet import *
# requires pytest-datadir to be installed

@pytest.fixture(scope="class")
def glue_database_name():
    return "pytest-database"

@pytest.fixture(scope="class")
def glue_nested_table_name():
    return "dataset_pytest_nested_table"

@pytest.fixture(scope="class")
def glue_flat_table_name():
    return "dataset_pytest_flat_table"

@pytest.fixture(scope="class")
def glue_database_path(artifact_bucket, namespace):
    glue_database_path = os.path.join(
            "s3://",
            artifact_bucket,
            "BridgeDownstream",
            namespace,
            "tests/test_json_s3_to_parquet"
    )
    return glue_database_path

@pytest.fixture(scope="class")
def glue_database(glue_database_name, glue_database_path):
    glue_client = boto3.client("glue")
    glue_database = glue_client.create_database(
            DatabaseInput={
                "Name": glue_database_name,
                "Description": "A database for pytest unit tests.",
            }
    )
    yield glue_database
    glue_client.delete_database(Name=glue_database_name)

@pytest.fixture(scope="class")
def glue_nested_table(glue_database, glue_database_name, glue_database_path,
                      glue_nested_table_name):
    glue_client = boto3.client("glue")
    glue_table = glue_client.create_table(
            DatabaseName=glue_database_name,
            TableInput={
                "Name": glue_nested_table_name,
                "Description": "A table for pytest unit tests.",
                "Retention": 0,
                "TableType": "EXTERNAL_TABLE",
                "StorageDescriptor": {
                    "Location": os.path.join(
                        glue_database_path,
                        glue_nested_table_name.replace("_", "=", 1)
                    ) + "/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "Compressed": False,
                    "StoredAsSubDirectories": False,
                    "Columns": [
                        {
                            "Name": "recordid",
                            "Type": "string"
                        },
                        {
                            "Name": "arrayofobjectsfield",
                            "Type": "array<struct<filename:string,timestamp:string>>"
                        },
                        {
                            "Name": "objectfield",
                            "Type": "struct<filename:string,timestamp:string>"
                        }
                    ]
                },
                "PartitionKeys": [
                    {
                        "Name": "assessmentid",
                        "Type": "string"
                    },
                    {
                        "Name": "year",
                        "Type": "string"
                    },
                    {
                        "Name": "month",
                        "Type": "string"
                    },
                    {
                        "Name": "day",
                        "Type": "string"
                    }
                ],
                "Parameters": {
                    "classification": "json",
                    "compressionType": "none",
                    "typeOfData": "file",
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "CrawlerSchemaSerializerVersion": "1.0"
                }
            }
    )
    return glue_table

@pytest.fixture(scope="class")
def glue_flat_table(glue_database, glue_database_name, glue_database_path,
                    glue_flat_table_name):
    glue_client = boto3.client("glue")
    glue_table = glue_client.create_table(
            DatabaseName=glue_database_name,
            TableInput={
                "Name": glue_flat_table_name,
                "Description": "A table for pytest unit tests.",
                "Retention": 0,
                "TableType": "EXTERNAL_TABLE",
                "StorageDescriptor": {
                    "Location": os.path.join(
                            glue_database_path,
                            glue_flat_table_name.replace("_", "=", 1)
                    ) + "/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "Compressed": False,
                    "StoredAsSubDirectories": False,
                    "Columns": [
                        {
                            "Name": "recordid",
                            "Type": "string"
                        }
                    ]
                },
                "PartitionKeys": [
                    {
                        "Name": "assessmentid",
                        "Type": "string"
                    },
                    {
                        "Name": "year",
                        "Type": "string"
                    },
                    {
                        "Name": "month",
                        "Type": "string"
                    },
                    {
                        "Name": "day",
                        "Type": "string"
                    }
                ],
                "Parameters": {
                    "classification": "json",
                    "compressionType": "none",
                    "typeOfData": "file",
                    "CrawlerSchemaDeserializerVersion": "1.0",
                    "CrawlerSchemaSerializerVersion": "1.0"
                }
            }
    )
    return glue_table

@pytest.fixture(scope="class")
def glue_crawler_role():
    iam_client = boto3.client("iam")
    role_name="pytest-crawler-role"
    managed_policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
    glue_crawler_role = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": ["glue.amazonaws.com"]
                        },
                        "Action": ["sts:AssumeRole"]
                    }
                ]
            }),
    )
    iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=managed_policy_arn
    )
    yield glue_crawler_role["Role"]["Arn"]
    iam_client.detach_role_policy(
            RoleName=role_name,
            PolicyArn=managed_policy_arn
    )
    iam_client.delete_role(RoleName=role_name)

@pytest.fixture(scope="class")
def glue_crawler(glue_database, glue_database_name, glue_database_path,
                 glue_flat_table, glue_flat_table_name, glue_nested_table,
                 glue_nested_table_name, glue_crawler_role):
    glue_client = boto3.client("glue")
    crawler_name = "pytest-crawler"
    time.sleep(10) # give time for the IAM role trust policy to set in
    glue_crawler = glue_client.create_crawler(
            Name=crawler_name,
            Role=glue_crawler_role,
            DatabaseName=glue_database_name,
            Description="A crawler for pytest unit test data.",
            Targets={
                "S3Targets": [
                    {
                        "Path": os.path.join(
                            glue_database_path,
                            glue_flat_table_name.replace("_", "=", 1)
                        ) + "/"
                    },
                    {
                        "Path": os.path.join(
                            glue_database_path,
                            glue_nested_table_name.replace("_", "=", 1)
                        ) + "/"
                    }
                ]
            },
            SchemaChangePolicy={
                "DeleteBehavior": "LOG",
                "UpdateBehavior": "LOG"
            },
            RecrawlPolicy={
                "RecrawlBehavior": "CRAWL_NEW_FOLDERS_ONLY"
            },
            Configuration=json.dumps({
                "Version":1.0,
                "CrawlerOutput": {
                    "Partitions": {
                        "AddOrUpdateBehavior":"InheritFromTable"
                    }
                },
                "Grouping": {
                    "TableGroupingPolicy":"CombineCompatibleSchemas"
                }
            })
    )
    glue_client.start_crawler(Name=crawler_name)
    response = {"Crawler": {}}
    for i in range(60): # wait up to 10 minutes for crawler to finish
        # This should take approximately 5 minutes
        response = glue_client.get_crawler(Name=crawler_name)
        if (
                "LastCrawl" in response["Crawler"]
                and "Status" in response["Crawler"]["LastCrawl"]
                and response["Crawler"]["LastCrawl"]["Status"] == "SUCCEEDED"
           ):
            break
        else:
            time.sleep(10)
    yield glue_crawler
    glue_client.delete_crawler(Name=crawler_name)

@pytest.fixture
def json_s3_objects(datadir, artifact_bucket, namespace):
    s3_client = boto3.client("s3")
    dataset_prefix = os.path.join(
            "BridgeDownstream", namespace, "tests")
    object_keys = []
    for dirpath, _, filenames in os.walk(datadir):
        if len(filenames) > 0:
            object_prefix = os.path.join(
                    dataset_prefix,
                    os.path.join(*dirpath.split("/")[-6:]))
            for filename in filenames:
                object_key = os.path.join(object_prefix, filename)
                s3_client.upload_file(
                        Filename=os.path.join(dirpath, filename),
                        Bucket=artifact_bucket,
                        Key=object_key)
                object_keys.append(object_key)
    return object_keys

@pytest.fixture(scope="class")
def glue_context():
    glue_context = GlueContext(SparkSession.builder.getOrCreate())
    return glue_context


class TestJsonS3ToParquet:


    def test_upload_s3_objects(self, json_s3_objects):
        """
        Since `datadir` is function scoped, we invoke the `json_s3_objects`
        fixture once here so that it doesn't need to be invoked for all the
        other test functions.
        """
        pass

    def test_get_table(self, glue_database_name, glue_flat_table_name,
                       glue_nested_table_name, glue_crawler, glue_context):
        flat_table = get_table(
                table_name=glue_flat_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        assert flat_table.count() == 3
        assert len(flat_table.schema().fields) == 5
        nested_table = get_table(
                table_name=glue_nested_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        assert nested_table.count() == 3
        assert len(nested_table.schema().fields) == 7

    def test_has_nested_fields(self, glue_database_name, glue_flat_table_name,
                               glue_nested_table_name, glue_context):
            flat_table = get_table(
                    table_name=glue_flat_table_name,
                    database_name=glue_database_name,
                    glue_context=glue_context
            )
            flat_table_schema = flat_table.schema()
            assert not has_nested_fields(flat_table_schema)
            nested_table = get_table(
                    table_name=glue_nested_table_name,
                    database_name=glue_database_name,
                    glue_context=glue_context
            )
            nested_table_schema = nested_table.schema()
            assert has_nested_fields(nested_table_schema)

    def test_add_index_to_table(self, glue_database_name, glue_database_path,
                                glue_nested_table_name, artifact_bucket, namespace,
                                glue_context):
        nested_table = get_table(
                table_name=glue_nested_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        nested_table_relationalized = nested_table.relationalize(
            root_table_name = glue_nested_table_name,
            staging_path = os.path.join(
                glue_database_path,
                "tmp/"
            )
        )
        tables_with_index = {}
        tables_with_index[glue_nested_table_name] = add_index_to_table(
                table_key=glue_nested_table_name,
                table_name=glue_nested_table_name,
                processed_tables=tables_with_index,
                unprocessed_tables=nested_table_relationalized
        )
        assert (
                set(tables_with_index[glue_nested_table_name].schema.fieldNames()) ==
                set(["recordid", "arrayofobjectsfield", "objectfield_filename",
                     "objectfield_timestamp", "assessmentid", "year", "month", "day"])
        )
        table_key = f"{glue_nested_table_name}_arrayofobjectsfield"
        tables_with_index[table_key] =  add_index_to_table(
                table_key=table_key,
                table_name=glue_nested_table_name,
                processed_tables=tables_with_index,
                unprocessed_tables=nested_table_relationalized
        )
        assert (
                set(tables_with_index[table_key].schema.fieldNames()) ==
                set(['id', 'index', 'filename', 'timestamp', 'assessmentid',
                     'year', 'month', 'day', 'recordid'])
        )
        child_table_with_index = (tables_with_index[table_key]
              .toPandas()
              .sort_values("recordid")
              .reset_index(drop=True)
              .drop(columns=["id", "index"]))
        print("Child table with index:")
        print(child_table_with_index)
        correct_df = pandas.DataFrame({
            "filename": ["one", "three", "two"],
            "timestamp": ["one", "three", "two"],
            "assessmentid": ["exampleassessment", "exampleassessment", "exampleassessment"],
            "year": ["2022", "2022", "2022"],
            "month": ["09", "09", "09"],
            "day": ["01", "02", "01"],
            "recordid": ["one", "three", "two"]
        })
        print("Correct table:")
        print(correct_df)
        for col in child_table_with_index.columns:
            print(col)
            print(child_table_with_index[col].values == correct_df[col].values)
            assert all(child_table_with_index[col].values == correct_df[col].values)

    def test_write_table_to_s3(self, artifact_bucket, namespace, glue_database_name,
                               glue_database_path, glue_flat_table_name, glue_context):
        flat_table = get_table(
                table_name=glue_flat_table_name,
                database_name=glue_database_name,
                glue_context=glue_context
        )
        parquet_key = os.path.join(
                    "BridgeDownstream",
                    namespace,
                    "tests/test_json_s3_to_parquet/flat_table"
        )
        write_table_to_s3(
                dynamic_frame=flat_table,
                bucket=artifact_bucket,
                key=parquet_key,
                glue_context=glue_context
        )
        s3_client = boto3.client("s3")
        parquet_dataset = s3_client.list_objects_v2(
                Bucket=artifact_bucket,
                Prefix=parquet_key)
        assert parquet_dataset["KeyCount"] > 0

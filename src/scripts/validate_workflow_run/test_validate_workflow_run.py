import pytest
import boto3
from validate_workflow_run import read_args, get_json_by_key, get_parquet_by_key
import pyarrow

bad_bucket = 'xbridge-downstream-dev-intermediate-data'
bad_key = 'x'
good_json_bucket = 'bridge-downstream-dev-intermediate-data'
good_json_key = 'ETL-91/example-app-1/study-1/raw_json/dataset=info_v1/assessmentid=dccs/year=2022/month=2/day=15/recordid=dsXJVIY0aFE5rAUdI3vpsGsr/dsXJVIY0aFE5rAUdI3vpsGsr.ndjson'
good_parquet_bucket = 'bridge-downstream-dev-parquet'
good_parquet_key = 'bridge-downstream/example-app-1/study-1/parquet/dataset_info_v1_files/assessmentid=ChangeLocalizationV1/year=2022/month=3/day=7/part-00154-7be7f212-ad59-4816-81b9-110401edfb4e.c000.snappy.parquet'

def set_up_configuration():
    aws_session = boto3.session.Session(profile_name='mobile-health-dev')
    s3_client = aws_session.client("s3")

    return s3_client


def test_get_json_by_key(capfd):
    client = set_up_configuration()

    get_json_by_key(client=client, bucket=bad_bucket, key=good_json_key)
    out, err = capfd.readouterr()
    assert out == "Bucket xbridge-downstream-dev-intermediate-data is not valid.\n"

    get_json_by_key(client=client, bucket=good_json_bucket, key=bad_key)
    out, err = capfd.readouterr()
    assert out == "Key x is not valid.\n"

    assert type(get_json_by_key(client=client, bucket=good_json_bucket, key=good_json_key)) == dict


def test_get_parquet_by_key(capfd):
    client = set_up_configuration()

    get_parquet_by_key(client=client, bucket=bad_bucket, key=good_json_key)
    out, err = capfd.readouterr()
    assert out == "Bucket xbridge-downstream-dev-intermediate-data is not valid.\n"

    get_parquet_by_key(client=client, bucket=good_parquet_bucket, key=bad_key)
    out, err = capfd.readouterr()
    assert out == "Key x is not valid.\n"

    assert type(get_parquet_by_key(client=client, bucket=good_parquet_bucket, key=good_parquet_key)) == pyarrow.lib.Table

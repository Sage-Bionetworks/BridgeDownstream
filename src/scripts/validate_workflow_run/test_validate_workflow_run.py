import pytest
import boto3
from validate_workflow_run import get_json_by_key, get_parquet_by_key
import pyarrow

BAD_BUCKET = 'xbridge-downstream-dev-intermediate-data'
BAD_KEY = 'x'
GOOD_JSON_BUCKET = 'bridge-downstream-dev-intermediate-data'
GOOD_JSON_KEY = 'ETL-91/example-app-1/study-1/raw_json/dataset=info_v1/assessmentid=dccs/year=2022/month=2/day=15/recordid=dsXJVIY0aFE5rAUdI3vpsGsr/dsXJVIY0aFE5rAUdI3vpsGsr.ndjson'
GOOD_PARQUET_BUCKET = 'bridge-downstream-dev-parquet'
GOOD_PARQUET_KEY = 'bridge-downstream/example-app-1/study-1/parquet/dataset_info_v1_files/assessmentid=ChangeLocalizationV1/year=2022/month=3/day=7/part-00154-7be7f212-ad59-4816-81b9-110401edfb4e.c000.snappy.parquet'


def set_up_configuration():
    aws_session = boto3.session.Session(profile_name='matt-mh-1')
    s3_client = aws_session.client("s3")

    return s3_client


def test_get_json_by_key(capfd):
    client = set_up_configuration()

    get_json_by_key(client=client,
                    bucket=BAD_BUCKET,
                    key=GOOD_JSON_KEY)
    out, err = capfd.readouterr()
    assert out == "Bucket xbridge-downstream-dev-intermediate-data is not valid.\n"

    get_json_by_key(client=client,
                    bucket=GOOD_JSON_BUCKET,
                    key=BAD_KEY)
    out, err = capfd.readouterr()
    assert out == "Key x is not valid.\n"

    assert type(get_json_by_key(client=client,
                                bucket=GOOD_JSON_BUCKET,
                                key=GOOD_JSON_KEY)) == dict


def test_get_parquet_by_key(capfd):
    client = set_up_configuration()

    get_parquet_by_key(client=client,
                       bucket=BAD_BUCKET,
                       key=GOOD_JSON_KEY)
    out, err = capfd.readouterr()
    assert out == "Bucket xbridge-downstream-dev-intermediate-data is not valid.\n"

    get_parquet_by_key(client=client,
                       bucket=GOOD_PARQUET_BUCKET,
                       key=BAD_KEY)
    out, err = capfd.readouterr()
    assert out == "Key x is not valid.\n"

    assert type(get_parquet_by_key(client=client,
                                   bucket=GOOD_PARQUET_BUCKET,
                                   key=GOOD_PARQUET_KEY)) == pyarrow.lib.Table

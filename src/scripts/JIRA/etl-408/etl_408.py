import os
import json
import boto3
import pandas as pd
import synapseclient
from pyarrow import fs, parquet

JSON_BUCKET = "bridge-downstream-intermediate-data"
PARQUET_BUCKET = "bridge-downstream-parquet"
INVALID_SQS = "https://sqs.us-east-1.amazonaws.com/611413694531/bridge-downstream-invalid-sqs-Queue"
MISSING_DATA_REFERENCE = "syn51185576"
APP = "mobile-toolbox"
STUDY = "fmqcjv"


def get_synapse_client(ssm_parameter=None, aws_session=None):
    """
    Return an authenticated Synapse client.

    Args:
        ssm_parameter (str): Name of the SSM parameter containing the
            BridgeDownstream Synapse password.
        aws_session (boto3.session.Session)

    Returns:
        synapseclient.Synapse
    """
    if ssm_parameter is not None:
        ssm_client = aws_session.client("ssm")
        token = ssm_client.get_parameter(Name=ssm_parameter, WithDecryption=True)
        syn = synapseclient.Synapse()
        syn.login(authToken=token["Parameter"]["Value"])
    else:  # try cached credentials
        syn = synapseclient.login()
    return syn


def get_json_records(aws_session, study):
    """
    Get a list of record IDs contained in the JSON dataset for a specific study.

    Args:
        aws_session (boto3.session.Session)
        study (str): The Bridge study identifier

    Returns:
        pandas.DataFrame: Containing one column (recordId)
    """
    s3 = aws_session.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        **{
            "Bucket": "bridge-downstream-intermediate-data",
            "Prefix": f"bridge-downstream/mobile-toolbox/{study}/raw_json/dataset=sharedSchema_v1/",
        }
    )
    json_s3_keys = []
    for page in page_iterator:
        for s3_obj in page["Contents"]:
            json_s3_keys.append(s3_obj["Key"])
    record_ids = [k.split("/")[-1].split(".")[0] for k in json_s3_keys]
    json_records = pd.DataFrame({"recordId": record_ids})
    return json_records


def get_parquet_dataset(dataset_uri, aws_session, columns):
    """
    Returns a Parquet dataset on S3 as a pandas dataframe

    Args:
        dataset_uri (str): The S3 URI of the parquet dataset.
        aws_session (boto3.session.Session)
        columns (list[str]): A list of columns to return from the parquet dataset.

    Returns:
        pandas.DataFrame
    """
    session_credentials = aws_session.get_credentials()
    table_source = dataset_uri.split("s3://")[-1]
    s3_fs = fs.S3FileSystem(
        access_key=session_credentials.access_key,
        secret_key=session_credentials.secret_key,
        session_token=session_credentials.token,
    )
    parquet_dataset = parquet.read_table(
        source=table_source, filesystem=s3_fs, columns=columns
    )
    return parquet_dataset.to_pandas()


def get_invalid_records(aws_session, queue_url, iterations, delete_after_sample=False):
    """
    Poll an SQS queue for 10 messages at a time for a number of iterations.
    If `delete_after_sample` is true, messages received from the queue are deleted.
    Otherwise, since bootstrap sampling is used, you are likely to receive a
    large number of duplicates.  If the queue is empty, returns the results
    collected so far.

    Args:
        aws_session (boto3.session.Session)
        queue_url (str): The URL of the SQS queue
        iterations (int): The number of times to poll the SQS queue
        delete_after_sample: Whether to delete received messages
            from the queue after they have been received.

    Returns:
        pandas.DataFrame: A dataframe representation of all messages received
    """
    messages = []
    sqs = aws_session.client("sqs")
    for i in range(iterations):
        try:
            response = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)
            for message in response["Messages"]:
                message_body = json.loads(message["Body"])
                messages.append(message_body)
            if delete_after_sample:
                sqs.delete_message_batch(
                    QueueUrl=queue_url,
                    Entries=[
                        {"Id": m["MessageId"], "ReceiptHandle": m["ReceiptHandle"]}
                        for m in response["Messages"]
                    ],
                )
        except Exception as e:
            print(
                "Encountered the following error while polling the SQS queue. "
                "Returning the messages collected thus far.\n"
            )
            print(str(e))
            break
    messages_df = pd.DataFrame(messages)
    validation_result_df = pd.DataFrame.from_records(
        messages_df.validation_result.values
    )
    invalid_records = pd.concat([messages_df, validation_result_df], axis=1)
    return invalid_records


def get_missing_records(syn, study):
    """
    Get file produced by Larsson which tracks each record

    Args:
        aws_session (boto3.session.Session)
        study (str): The Bridge study identifier

    Returns:
        pandas.DataFrame
    """
    # This version from 2023-04-11
    reference = pd.read_csv(syn.get(MISSING_DATA_REFERENCE).path)
    study_reference = reference.query(
        f"studyId == '{study}' and inStudyProject == 1 and inParquet == 0"
    )
    study_reference = study_reference.dropna(subset=["recordId"])
    return study_reference


def main():
    aws_session = boto3.session.Session(region_name="us-east-1")
    syn = get_synapse_client(
        ssm_parameter="synapse-bridgedownstream-auth", aws_session=aws_session
    )
    reference = get_missing_records(syn=syn, study=STUDY)
    parquet_base_s3_uri = (
        f"s3://{PARQUET_BUCKET}/bridge-downstream/{APP}/{STUDY}/parquet"
    )
    task_data = get_parquet_dataset(
        dataset_uri=os.path.join(parquet_base_s3_uri, "dataset_sharedschema_v1"),
        aws_session=aws_session,
        columns=["recordid"],
    )
    invalid_data = get_invalid_records(
        aws_session=aws_session, queue_url=INVALID_SQS, iterations=100
    )

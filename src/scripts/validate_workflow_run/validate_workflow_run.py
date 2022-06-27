import boto3
import argparse
import json
from pyarrow import parquet as pq
import io

s3 = boto3.resource('s3')

def read_args():
    #TODO: figure out how key is dynamically formed
    """Read command line arguments."""
    parser = argparse.ArgumentParser(
            description=("Builds set of configurations for tests "
                         "s3://{--bucket}/{--app}/{study}/parquet/archive/"
                         "{--dataset}_{--dataset-version}_{update-number}/"))
    parser.add_argument("--bucket",
                        help="The bucket's name.")
    parser.add_argument("--key",
                        help="The key indentifier for the s3 object.")
    parser.add_argument("--profile",
                        help="The AWS profile to use.")
    args = parser.parse_args()
    return args


def get_json_by_key(client, bucket: str, key: str) -> dict:
    """
    Retrieves a JSON file from s3 using the bucket name and the key

    Keyword arguments:
    client -- boto3 S3 client
    bucket -- The S3 bucket name.
    key -- Key referencing the object in the bucket

    Returns:
    Python dictionary composed by JSON string downloaded from S3
    """
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        obj_as_dict = json.loads(obj['Body'].read())
    except client.exceptions.NoSuchKey as invalid_key:
        print(f"Key {key} is not valid.")
        return None
    except client.exceptions.NoSuchBucket as invalid_bucket:
        print(f"Bucket {bucket} is not valid.")
        return None
    except json.decoder.JSONDecodeError as invalid_json:
        print(f"JSON file downloaded from s3 is not a valid JSON string.")
        return None

    return obj_as_dict

def get_parquet_by_key(client, bucket: str, key: str):
    """
    Retrieves a parquet file from s3 using the bucket name and the key

    Keyword arguments:
    client -- boto3 S3 client
    bucket -- The S3 bucket name.
    key -- Key referencing the object in the bucket

    Returns:
    Pyarrow table corresponding to the parquet dataset
    """
    try:
        obj = client.get_object(Bucket=bucket, Key=key)
        obj_as_parquet = pq.read_table(io.BytesIO(obj['Body'].read()))
    except client.exceptions.NoSuchKey as invalid_key:
        print(f"Key {key} is not valid.")
        return None
    except client.exceptions.NoSuchBucket as invalid_bucket:
        print(f"Bucket {bucket} is not valid.")
        return None
    except TypeError as invalid_parquet:
        print(f"{key} is not a valid parquet file")
        return None
    except Exception as e:
        print("Unexpected exception in " + e)
        return None

    return obj_as_parquet


def main():
    # args = read_args()
    # aws_session = boto3.session.Session(profile_name=args.profile)
    # s3_client = aws_session.client("s3")
    pass

if __name__ == "__main__":
    main()
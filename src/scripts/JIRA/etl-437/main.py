"""
This script copies all objects under an S3 prefix which begin with
a specific string to an object in the same "directory" but with
the basename prefaced with another string.
"""
import json
import os
import argparse
import boto3

def read_args():
    parser = argparse.ArgumentParser(
            description="Copy S3 objects to another object prefaced with a string.")
    parser.add_argument(
            "--s3-bucket",
            required=True,
            help="The name of the S3 bucket."
    )
    parser.add_argument(
            "--s3-prefix",
            required=True,
            help="The S3 prefix under which qualifying objects will be copied."
    )
    parser.add_argument(
            "--match-string",
            required=True,
            help="Objects will be copied if they begin with this string."
    )
    parser.add_argument(
            "--preface-string",
            required=True,
            help="Objects which are copied will be prefaced with this string."
    )
    parser.add_argument(
            "--profile",
            help="Optional. The local AWS profile to use."
    )
    args = parser.parse_args()
    return args

def get_matching_objects(
        s3_client: "botocore.client.S3",
        bucket: str,
        key_prefix: str,
        match_string: str) -> "list[str]":
    """
    Retrieves a list of object keys in an S3 bucket that match a specific key prefix
    and have a basename beginning with a specified string.

    Args:
        s3_client (botocore.client.S3): An instance of the S3 client from the boto3 library.
        bucket (str): The name of the S3 bucket.
        key_prefix (str): The prefix for the S3 object keys.
        match_string (str): The string that the basename of the object keys should begin with.

    Returns:
        list[str]: A list of object keys that match the specified criteria.

    Raises:
        botocore.exceptions.BotoCoreError: If any error occurs during the S3 API operation.
    """
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=key_prefix)
    matching_objects = []
    for page in page_iterator:
        if "Contents" in page:
            for obj in page["Contents"]:
                key = obj["Key"]
                base_name = os.path.basename(key)
                if base_name.startswith(match_string):
                    matching_objects.append(key)
    return matching_objects

def copy_objects(
        s3_client: "botocore.client.S3",
        bucket: str,
        keys: "list[str]",
        preface_string: str) -> "dict[str,str]":
    """
    Copies a list of S3 objects to a new object in the same key prefix by
    prefacing their basename with a provided string.

    Args:
        s3_client (botocore.client.S3): An instance of the S3 client from the boto3 library.
        bucket (str): The name of the S3 bucket.
        keys (list[str]): A list of object keys to be copied.
        preface_string (str): The string to be prepended to the basename of the object keys.

    Returns:
        dict[str,str]: A dictionary mapping objects to their new keys.

    Raises:
        botocore.exceptions.BotoCoreError: If any error occurs during the S3 API operations.
    """
    copied_objects = {}
    for key in keys:
        new_key = os.path.join(
                os.path.dirname(key),
                preface_string + os.path.basename(key)
        )
        response = s3_client.copy_object(
            CopySource={"Bucket": bucket, "Key": key},
            Bucket=bucket,
            Key=new_key
        )
        copied_objects[key] = new_key
        # Determining a successful copy operation is more complicated than this.
        #if response["ResponseMetadata"]["HTTPStatusCode"] == 200:
        #    s3_client.delete_object(Bucket=bucket, Key=key)
    return copied_objects

def main():
    args = read_args()
    aws_session = boto3.session.Session(
            region_name="us-east-1",
            profile_name=args.profile)
    s3_client = aws_session.client("s3")
    matching_objects = get_matching_objects(
            s3_client=s3_client,
            bucket=args.s3_bucket,
            key_prefix=args.s3_prefix,
            match_string=args.match_string
    )
    copied_objects = copy_objects(
            s3_client=s3_client,
            bucket=args.s3_bucket,
            keys=matching_objects,
            preface_string=args.preface_string
    )
    log_file_name = f"copied_objects_{args.s3_bucket}_{args.s3_prefix.replace('/', 'slash')}.json"
    with open(log_file_name, "w") as logs:
        json.dump(copied_objects, logs)


if __name__ == "__main__":
    main()

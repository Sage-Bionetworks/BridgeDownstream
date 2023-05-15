"""
Create an STS-enabled folder on Synapse over an S3 location.
"""

import json
import os
import synapseclient
import argparse
import boto3 as boto


def read_args():
    parser = argparse.ArgumentParser(
        description="Create an STS-enabled folder on Synapse over an S3 location."
    )
    parser.add_argument(
        "--synapse-parent", help="Synapse ID of the parent folder/project"
    )
    parser.add_argument(
        "--synapse-folder-name",
        help=(
            "Name of the Synapse folder to use with the external " "storage location."
        ),
    )
    parser.add_argument("--s3-bucket", help="S3 bucket name")
    parser.add_argument(
        "--s3-key",
        default=None,
        help="S3 key to serve as the root. `/` (root) by default.",
    )
    parser.add_argument(
        "--sts-enabled",
        action="store_true",
        help="Whether this storage location should be STS enabled",
    )
    parser.add_argument(
        "--configure-bucket",
        action="store_true",
        help=(
            "!! Overwrites any existing bucket policy !!"
            "Configure the bucket policy and CORS, "
            " and write an owner.txt containing the user identifier "
            " of the current user to the --s3-key argument."
        ),
    )
    parser.add_argument(
        "--aws-profile",
        default=None,
        help="AWS profile. Uses default credentials if not set.",
    )
    args = parser.parse_args()
    return args


def configure_bucket(
    s3_client,
    syn,
    bucket_name,
    bucket_key,
    bucket_policy_path="bucket_policy.txt",
    cors_path="cors.txt",
    aws_profile=None,
):
    # Write bucket policy
    with open(bucket_policy_path, "r") as f:
        policy = f.read()
        policy = policy.replace("thisisthenameofmybucket", bucket_name)
        s3_client.put_bucket_policy(
            Bucket=bucket_name, ConfirmRemoveSelfBucketAccess=False, Policy=policy
        )
    # Write owner.txt
    user_profile = syn.getUserProfile()
    with open("owner.txt", "w") as f:
        f.write(user_profile["ownerId"])
    with open("owner.txt", "rb") as f:
        s3_client.put_object(
            Body=f, Bucket=bucket_name, Key=os.path.join(bucket_key or "", "owner.txt")
        )
    # Write CORS
    with open(cors_path, "r") as f:
        cors_config = json.load(f)
        s3_client.put_bucket_cors(Bucket=bucket_name, CORSConfiguration=cors_config)


def main():
    args = read_args()
    syn = synapseclient.login()
    if args.configure_bucket:
        s3_client = boto.client("s3")
        configure_bucket(
            s3_client=s3_client,
            syn=syn,
            bucket_name=args.s3_bucket,
            bucket_key=args.s3_key,
            bucket_policy_path="bucket_policy.txt",
            cors_path="cors.txt",
            aws_profile=args.aws_profile,
        )
    storage_location = syn.create_s3_storage_location(
        parent=args.synapse_parent,
        folder_name=args.synapse_folder_name,
        bucket_name=args.s3_bucket,
        base_key=args.s3_key,
        sts_enabled=args.sts_enabled,
    )
    storage_location_info = {
        k: v
        for k, v in zip(
            ["synapse_folder", "storage_location", "synapse_project"], storage_location
        )
    }
    print(storage_location_info)
    return storage_location_info


if __name__ == "__main__":
    main()

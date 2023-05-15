"""
Archive a dataset and its derived datasets by copying them to the
archive folder. A typical use case is to archive a dataset before a compatible
schema change. Multiple compatible schema changes may happen to
a single dataset, thus archives have an update number appended
to their name. For example, if this is the third time we are
archiving the `dataset_metadata_v2` dataset, the archive will be named
`dataset_metadata_v2_3`. The script will automatically determine which
update number this is, therefore it is not necessary to pass the
update number as a command line argument.

Datasets are assumed to exist in:

    s3://{bucket}/{app}/{parquet}/

And archives will be stored to:

    s3://{bucket}/{app}/{parquet}/archive/

When relationalizing a dataset, as is done in the json_to_parquet jobs,
descendent datasets (such as `dataset_metadata_files`, a descendent
of `dataset_metadata`) are created. For ease of use with the typical
use case, descendent datasets are archived when passing the root
dataset to `--dataset`. For example, passing `--dataset=dataset_metadata`
and `--dataset-version=v2` will archive both `dataset_metadata_v2` and
`dataset_metadata_files_v2`.

Archive updates are tracked at the root dataset level.
For example, if a struct `folders` is added to the `metadata_v2`
JSON dataset in that dataset's second compatible schema update, then
the next time a compatible schema update occurs the third archive
operation will also occur. Although this is the first time that the
`metadata_folders_v2` dataset will be archived, its archive will be
named `metadata_folders_v2_3`.
"""
import argparse
import os
import shlex
import subprocess
import boto3


def read_args():
    """Read command line arguments."""
    parser = argparse.ArgumentParser(
        description=(
            "Archive a parquet dataset for each study of an app "
            "by copying it to the standard archive location: "
            "s3://{--bucket}/{--app}/{study}/parquet/archive/"
            "{--dataset}_{--dataset-version}_{update-number}/"
        )
    )
    parser.add_argument("--bucket", help="The name of the S3 bucket.")
    parser.add_argument("--app", help="The app identifier.")
    parser.add_argument("--dataset", help="The dataset name.")
    parser.add_argument("--dataset-version", help="The dataset name.")
    parser.add_argument("--profile", help="The AWS profile to use.")
    args = parser.parse_args()
    return args


def get_source_and_dest_prefix(s3_client, bucket, app, dataset, dataset_version):
    """Construct the source and destination S3 prefixes of the archives.

    Keyword arguments:
    s3_client -- An AWS S3 client object.
    bucket -- The S3 bucket name.
    app -- The app identifier, as used in the S3 prefix in the bucket, to archive.
    dataset -- The name of the dataset to archive.
    dataset_version -- The dataset version to archive.

    Returns:
    A dictionary with source prefixes as keys and destination
    prefixes as values.
    """
    source_and_dest = dict()
    study_prefix_obj = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=f"{app}/", Delimiter="/"
    )
    study_prefixes = [cp["Prefix"] for cp in study_prefix_obj["CommonPrefixes"]]
    for study_prefix in study_prefixes:
        parquet_prefix = f"{study_prefix}parquet/"
        parquet_dataset_prefix_obj = s3_client.list_objects_v2(
            Bucket=bucket, Prefix=parquet_prefix, Delimiter="/"
        )
        descendent_dataset_prefixes = [
            cp["Prefix"]
            for cp in parquet_dataset_prefix_obj["CommonPrefixes"]
            if f"{dataset}_" in cp["Prefix"] and dataset_version in cp["Prefix"]
        ]
        latest_archive_dataset_update = get_archive_dataset_update_number(
            s3_client=s3_client,
            bucket=bucket,
            study_prefix=study_prefix,
            dataset=dataset,
            dataset_version=dataset_version,
        )
        for descendent_dataset_prefix in descendent_dataset_prefixes:
            source_path = os.path.join("s3://", bucket, descendent_dataset_prefix)
            descendent_dataset = descendent_dataset_prefix.split("/")[-2]
            archive_dataset_name = "_".join(
                [descendent_dataset, str(latest_archive_dataset_update + 1)]
            )
            dest_path = os.path.join(
                "s3://",
                bucket,
                study_prefix,
                "parquet",
                "archive",
                archive_dataset_name,
            )
            source_and_dest[source_path] = dest_path
    return source_and_dest


def get_archive_dataset_update_number(
    s3_client, bucket, study_prefix, dataset, dataset_version
):
    """Get the most recent update number for the specified dataset and
    dataset version. The update number of the archive we are creating should
    be one more than this.

    Keyword arguments:
    s3_client -- An AWS S3 client object.
    bucket -- The S3 bucket name.
    study_prefix -- The S3 prefix down to the study level, e.g., {app}/{study}/
    dataset -- The name of the dataset to archive.
    dataset_version -- The dataset version to archive.

    Returns:
    An integer representing how many times this dataset and dataset version
    have been archived previously.
    """
    archive_prefix = f"{study_prefix}parquet/archive/"
    archive_dataset_prefix_obj = s3_client.list_objects_v2(
        Bucket=bucket, Prefix=archive_prefix, Delimiter="/"
    )
    relevant_archive_dataset_prefixes = []
    if "CommonPrefixes" in archive_dataset_prefix_obj.keys():
        relevant_archive_dataset_prefixes = [
            cp["Prefix"]
            for cp in archive_dataset_prefix_obj["CommonPrefixes"]
            if f"{dataset}_{dataset_version}" in cp["Prefix"]
        ]
    if len(relevant_archive_dataset_prefixes) == 0:
        # No datasets of this type and version in archive
        return 0
    preexisting_update_nums = [
        prefix.split("_")[-1][:-1] for prefix in relevant_archive_dataset_prefixes
    ]
    latest_version = max([int(n) for n in preexisting_update_nums])
    return latest_version


def copy_source_to_dest(source_and_dest):
    """Recursively copy each item in a dictionary from its key to its value.

    Keyword arguments:
    source_and_dest -- A dictionary with S3 source prefixes as keys
    and S3 destination prefixes as values.

    Returns: None
    """
    for source in source_and_dest:
        dest = source_and_dest[source]
        bash_command = f"aws s3 cp --recursive {source} {dest}"
        subprocess.run(shlex.split(bash_command), check=True)


def main():
    args = read_args()
    aws_session = boto3.session.Session(profile_name=args.profile)
    s3_client = aws_session.client("s3")
    source_and_dest = get_source_and_dest_prefix(
        s3_client=s3_client,
        bucket=args.bucket,
        app=args.app,
        dataset=args.dataset,
        dataset_version=args.dataset_version,
    )
    copy_source_to_dest(source_and_dest=source_and_dest)


if __name__ == "__main__":
    main()

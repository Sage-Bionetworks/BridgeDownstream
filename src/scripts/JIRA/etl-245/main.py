"""
Compares file counts in the JSON datasets to unique record
counts in the parquet datasets for each study.
"""
import os
import json
import boto3
from pyarrow import fs, parquet

JSON_BUCKET = "bridge-downstream-intermediate-data"
PARQUET_BUCKET = "bridge-downstream-parquet"
APP = "mobile-toolbox"
STUDIES = ["cxhnxd", "fmqcjv", "hktrrx", "htshxm", "pmbfzc"]
DATASETS = [
        "ArchiveMetadata_v1", "AudioLevelRecord_v1", "MotionRecord_v1",
        "sharedSchema_v1", "WeatherResult_v1"
]
OUTPUT_FILE_NAME = "record_counts.json"

def get_s3_object_count(s3_client, bucket, prefix):
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
            Bucket=bucket,
            Prefix=prefix
    )
    object_count = sum([page["KeyCount"] for page in page_iterator])
    return object_count

def main():
    s3 = fs.S3FileSystem(region="us-east-1")
    s3_client = boto3.client("s3")
    json_base_s3_uri = f"{JSON_BUCKET}/bridge-downstream/{APP}/{{study}}/raw_json"
    parquet_base_s3_uri = f"{PARQUET_BUCKET}/bridge-downstream/{APP}/{{study}}/parquet"
    study_counts = {}
    for study in STUDIES:
        study_counts[study] = {"json": [], "parquet": []}
        for dataset in DATASETS:
            object_count = get_s3_object_count(
                    s3_client=s3_client,
                    bucket=JSON_BUCKET,
                    prefix=os.path.join(
                        "/".join(json_base_s3_uri.format(study=study).split("/")[1:]),
                        f"dataset={dataset}")
            )
            study_counts[study]["json"].append((dataset, object_count))
            try:
                parquet_dataset = parquet.read_table(
                        source=os.path.join(
                            parquet_base_s3_uri.format(study=study),
                            f"dataset_{dataset.lower()}"),
                        columns=["recordid"],
                        filesystem=s3
                )
                parquet_df = parquet_dataset.to_pandas()
                record_counts = parquet_df["recordid"].nunique()
                study_counts[study]["parquet"].append(
                        (dataset, record_counts))
            except FileNotFoundError:
                study_counts[study]["parquet"].append((dataset, 0))
        study_counts[study]["json"] = sorted(study_counts[study]["json"])
        study_counts[study]["parquet"] = sorted(study_counts[study]["parquet"])
    with open(OUTPUT_FILE_NAME, "w") as f_out:
        json.dump(study_counts, f_out)

if __name__ == "__main__":
    main()

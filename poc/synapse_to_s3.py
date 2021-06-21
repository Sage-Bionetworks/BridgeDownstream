"""
Moves raw data archives from Synapse to S3
"""

import os
import synapseclient
import boto3 as boto
import argparse

def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--synapse-source", type=str)
    parser.add_argument("--s3-bucket", type=str)
    parser.add_argument("--s3-prefix", type=str)
    parser.add_argument("--aws-profile", type=str)
    parser.add_argument("--num", type=str, default = None,
            help="The number of files to copy to S3")
    args = parser.parse_args()
    return(args)

def download_from_synapse(syn, source_table, limit=None):
    q_string = (
            f"SELECT recordId, healthCode, rawData FROM {source_table} "
            f"WHERE rawData IS NOT NULL AND \"taskData.testVersion\" = '1.0.0' "
            f"ORDER BY createdOn DESC ")
    if limit is None:
        pass
    else:
        q_string = q_string + f"LIMIT {limit}"
    q = syn.tableQuery(q_string)
    paths = syn.downloadTableColumns(q, "rawData")
    df = q.asDataFrame()
    df["path"] = df["rawData"].astype(str).map(paths)
    return(df)

def copy_to_s3(df, session, bucket, basekey):
    s3 = session.resource("s3")
    responses = []
    for i, record in df.iterrows():
        key = f"{basekey}/{os.path.basename(record['path'])}"
        metadata_dic = {
                "recordId": record["recordId"],
                "healthCode": record["healthCode"]}
        with open(record["path"], "rb") as f:
            obj = s3.Object(
                    bucket_name = bucket,
                    key = key)
            response = obj.put(
                    Body = f,
                    Metadata = metadata_dic)
            responses.append(response)
    return responses

def main():
    args = read_args()
    syn = synapseclient.login()
    df = download_from_synapse(
            syn = syn,
            source_table = args.synapse_source,
            limit = args.num)
    session = boto.Session(profile_name = args.aws_profile)
    copy_to_s3(
            df = df,
            session = session,
            bucket = args.s3_bucket,
            basekey = args.s3_prefix)

if __name__ == "__main__":
    main()


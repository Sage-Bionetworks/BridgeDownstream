"""
Submit Synapse files stored on an external S3 bucket to a Glue workflow.
Of course, this assumes that the workflow has read permissions on the S3
bucket.

The --diff-* arguments allow a parquet dataset in a Synapse-linked S3 location to
be diffed upon before submitting matching --query results to the --glue-workflow.
In this way, data which has already been processed by the pipeline is not
processed again.
"""
import json
import argparse
import boto3
import synapseclient
from pyarrow import fs, parquet


def read_args():
    parser = argparse.ArgumentParser(
            description=("Submit files on Synapse to an AWS --glue-workflow"))
    parser.add_argument("--file-view",
                        required=True,
                        help=("The Synapse ID of a file view containing "
                              "files to be submitted."))
    parser.add_argument("--raw-folder-id",
                        required=True,
                        help=("The Synapse ID of a folder containing this "
                              "this data. If querying a --file-view, this is "
                              "usually the scope of that file view (or the "
                              "Synapse ID of a folder which contains everything "
                              "in the scope). If data is instead coming from the "
                              "folder specified by --synapse-parent, these values "
                              "are identical."))
    parser.add_argument("--query",
                        help=("An f-string formatted query which filters the "
                              "file view. Use {source_table} in the FROM clause. "
                              "If this argument is not specified, all files in "
                              "the --file-view will be submitted to the "
                              "--glue-workflow."))
    parser.add_argument("--glue-workflow",
                        required=True,
                        help="The name of the Glue workflow to submit to.")
    parser.add_argument("--diff-s3-uri",
                        help=("The S3 URI of a parquet dataset to diff with "
                              "before submitting to the --glue-workflow."))
    parser.add_argument("--diff-parquet-field",
                        help=("The field name in the parquet dataset to diff "
                              "upon."))
    parser.add_argument("--diff-file-view-field",
                        help=("The field name in the --file-view to diff upon."))
    parser.add_argument("--profile",
                        help="The AWS profile to use.")
    parser.add_argument("--ssm-parameter",
                        help=("The name of the SSM parameter containing "
                              "the Synapse personal access token. "
                              "If not provided, cached credentials are used"))
    args = parser.parse_args()
    return args


def get_synapse_client(ssm_parameter=None, aws_session=None):
    if ssm_parameter is not None:
        ssm_client = aws_session.client("ssm")
        token = ssm_client.get_parameter(
            Name=ssm_parameter,
            WithDecryption=True)
        syn = synapseclient.Synapse()
        syn.login(authToken=token["Parameter"]["Value"])
    else: # try cached credentials
        syn = synapseclient.login()
    return syn


def get_synapse_df(syn, entity_view, query=None):
    if query is not None:
        query_string = query.format(source_table=entity_view)
    else:
        query_string = f"select * from {entity_view}"
    synapse_q = syn.tableQuery(query_string)
    synapse_df = synapse_q.asDataFrame()
    return synapse_df


def get_parquet_dataset(dataset_uri, aws_session):
    session_credentials = aws_session.get_credentials()
    table_source = dataset_uri.split("s3://")[-1]
    s3_fs = fs.S3FileSystem(
            access_key=session_credentials.access_key,
            secret_key=session_credentials.secret_key,
            session_token=session_credentials.token)
    parquet_dataset = parquet.read_table(
            source=table_source,
            filesystem=s3_fs)
    return parquet_dataset


def diff_on_parquet(synapse_df, parquet_df, synapse_field, parquet_field):
    synapse_df = (
            synapse_df
            .set_index(synapse_field)
            .drop(parquet_df[parquet_field].values))
    return synapse_df.id.values


def submit_archives_to_workflow(
        syn, synapse_ids, raw_folder_id, glue_workflow, aws_session):
    glue_client = aws_session.client("glue")
    messages = []
    for synapse_id in synapse_ids:
        message = get_message(
                syn=syn,
                synapse_id=synapse_id,
                raw_folder_id=raw_folder_id)
        messages.append(message)
    workflow_run = glue_client.start_workflow_run(Name=glue_workflow)
    glue_client.put_workflow_run_properties(
            Name=glue_workflow,
            RunId=workflow_run["RunId"],
            RunProperties={"messages": json.dumps(messages)})


def get_message(syn, synapse_id, raw_folder_id):
    f = syn.get(synapse_id, downloadFile=False)
    bucket = f["_file_handle"]["bucketName"]
    key = f["_file_handle"]["key"]
    message = {
            "source_bucket": bucket,
            "source_key": key,
            "raw_folder_id": raw_folder_id}
    return message


def main():
    args = read_args()
    aws_session = boto3.session.Session(
            profile_name=args.profile,
            region_name="us-east-1")
    syn = get_synapse_client(
            ssm_parameter=args.ssm_parameter,
            aws_session=aws_session)
    synapse_df = get_synapse_df(
            syn=syn,
            entity_view=args.file_view,
            query=args.query)
    if (
            args.diff_s3_uri is not None
            and args.diff_parquet_field is not None
            and args.diff_file_view_field is not None
       ):
        parquet_dataset = get_parquet_dataset(
                dataset_uri=args.diff_s3_uri,
                aws_session=aws_session)
        parquet_index = parquet_dataset.select([args.diff_parquet_field])
        synapse_ids = diff_on_parquet(
                synapse_df=synapse_df,
                parquet_df=parquet_index.to_pandas(),
                synapse_field=args.diff_file_view_field,
                parquet_field=args.diff_parquet_field)
    else:
        synapse_ids = synapse_df.id.values
    if len(synapse_ids) > 0:
        submit_archives_to_workflow(
                syn=syn,
                synapse_ids=synapse_ids,
                raw_folder_id=args.raw_folder_id,
                glue_workflow=args.glue_workflow,
                aws_session=aws_session)


if __name__ == "__main__":
    main()

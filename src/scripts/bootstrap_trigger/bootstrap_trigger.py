"""
Submit Synapse files stored on an external S3 bucket to a Glue workflow.

The --diff-* arguments allow one or two parquet dataset in a Synapse-linked S3 location to
be diffed upon before submitting matching --query results from a Synapse table
to the --glue-workflow. In this way, data which does not appear in the specified
parquet datasets is submitted for processing.
"""
import json
import argparse
import boto3
import synapseclient
from pyarrow import fs, parquet


def read_args():
    parser = argparse.ArgumentParser(
            description=("Submit files on Synapse to an AWS --glue-workflow"))
    parser.add_argument("--glue-workflow",
                        required=True,
                        help="The name of the Glue workflow to submit to.")
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
                              "in the scope)."))
    parser.add_argument("--query",
                        help=("Optional. An f-string formatted query which filters the "
                              "file view. Use {source_table} in the FROM clause. "
                              "If this argument is not specified, all files in "
                              "the --file-view will be submitted to the "
                              "--glue-workflow."))
    parser.add_argument("--drop-duplicates",
                        default=False,
                        action="store_true",
                        help=("Optional. Only use the most recently exported record "
                              "for a given record ID. Defaults to false"))
    parser.add_argument("--diff-s3-uri",
                        help=("Optional. The S3 URI of a parquet dataset to diff with "
                              "before submitting to the --glue-workflow."))
    parser.add_argument("--diff-parquet-field",
                        help=("Optional. The field name in the parquet dataset to diff "
                              "upon. Defaults to 'recordid'."),
                        default="recordid")
    parser.add_argument("--diff-s3-uri-2",
                        help=("Optional. The S3 URI of a parquet dataset to take the "
                              "union with --diff-s3-uri before taking the diff "
                              "and submitting to the --glue-workflow."))
    parser.add_argument("--diff-parquet-field-2",
                        help=("Optional. The field name in the --diff-s3-uri-2 parquet "
                              "dataset to diff upon. Defaults to 'recordid'."),
                        default="recordid")
    parser.add_argument("--diff-file-view-field",
                        help=("Optional. The field name in the --file-view to diff "
                              "upon. Defaults to 'recordId'."),
                        default="recordId")
    parser.add_argument("--profile",
                        help=("Optional. The AWS profile to use. Uses the default "
                              "profile if not specified."))
    parser.add_argument("--ssm-parameter",
                        help=("Optional. The name of the SSM parameter containing "
                              "the Synapse personal access token. "
                              "If not provided, cached credentials are used"))
    args = parser.parse_args()
    return args


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
        token = ssm_client.get_parameter(
            Name=ssm_parameter,
            WithDecryption=True)
        syn = synapseclient.Synapse()
        syn.login(authToken=token["Parameter"]["Value"])
    else: # try cached credentials
        syn = synapseclient.login()
    return syn


def get_synapse_df(syn, entity_view, index_field, query=None):
    """
    Query a table/view on Synapse and return as a Pandas dataframe

    Args:
        syn (synapseclient.Synapse)
        entity_view (str): The Synapse ID of the entity view
        index_field (str): The field name to set as index on the returned DataFrame
        query (str): An optional query to filter out records from the entity view.

    Returns:
        pandas.DataFrame
    """
    if query is not None:
        query_string = query.format(source_table=entity_view)
    else:
        query_string = f"select * from {entity_view}"
    synapse_q = syn.tableQuery(query_string)
    synapse_df = synapse_q.asDataFrame()
    synapse_df_with_index = synapse_df.set_index(index_field)
    return synapse_df_with_index


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
            session_token=session_credentials.token)
    parquet_dataset = parquet.read_table(
            source=table_source,
            filesystem=s3_fs,
            columns=columns)
    return parquet_dataset.to_pandas()


def submit_archives_to_workflow(
        syn, synapse_ids, raw_folder_id, glue_workflow, aws_session):
    """
    Submit data on Synapse to a Glue workflow.

    Args:
        syn (synapseclient.Synapse)
        synapse_ids (list[str]): The Synapse IDs to submit to the Glue workflow.
        raw_folder_id (str): The Synapse ID of the Bridge Raw Data folder where
            the `synapse_ids` are scoped.
        glue_workflow (str): The name of the Glue workflow.
        aws_session (boto3.session.Session)

    Returns:
        None
    """
    glue_client = aws_session.client("glue")
    batch_size = 100
    synapse_id_groups = [
            synapse_ids[i:i+batch_size]
            for i in range(0,len(synapse_ids),batch_size)]
    for synapse_id_group in synapse_id_groups:
        messages = []
        for synapse_id in synapse_id_group:
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
    """
    Get Synapse file entity metadata and format for submission to the
    S3 to JSON S3 workflow. A helper function for `submit_archives_to_workflow`.

    Args:
        syn (synapseclient.Synapse)
        synapse_id (str)
        raw_folder_id (str): The Synapse ID of the Bridge Raw Data folder where
            `synapse_id` is scoped.

    Returns:
        dict: With keys
            * source_bucket
            * source_key
            * raw_folder_id
    """
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
            index_field=args.diff_file_view_field,
            query=args.query)
    if args.drop_duplicates:
        sorted_synapse_df = synapse_df.sort_values(by="exportedOn")
        synapse_df = sorted_synapse_df[
                ~sorted_synapse_df.index.duplicated(keep="last")]
    if (
            args.diff_s3_uri is not None
            and args.diff_parquet_field is not None
            and args.diff_file_view_field is not None
       ):
        parquet_dataset = get_parquet_dataset(
                dataset_uri=args.diff_s3_uri,
                aws_session=aws_session,
                columns=[args.diff_parquet_field])
        records_needing_processing = synapse_df.drop(
                parquet_dataset[args.diff_parquet_field].values)
        if args.diff_s3_uri_2 is not None and args.diff_parquet_field_2 is not None:
            parquet_dataset_2 = get_parquet_dataset(
                    dataset_uri=args.diff_s3_uri_2,
                    aws_session=aws_session,
                    columns=[args.diff_parquet_field_2])
            records_needing_processing_2 = synapse_df.drop(
                    parquet_dataset_2[args.diff_parquet_field_2].values)
            synapse_df = synapse_df.loc[
                    records_needing_processing.index.union(records_needing_processing_2.index)
            ]
        else:
            synapse_df = synapse_df.loc[
                    records_needing_processing.index
            ]
    if len(synapse_df) > 0:
        print(f"Submitting { len(synapse_df) } records to { args.glue_workflow }")
        submit_archives_to_workflow(
                syn=syn,
                synapse_ids=synapse_df.id.values,
                raw_folder_id=args.raw_folder_id,
                glue_workflow=args.glue_workflow,
                aws_session=aws_session)


if __name__ == "__main__":
    main()

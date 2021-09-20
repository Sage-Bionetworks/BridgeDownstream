import synapseclient as sc
import boto3
import argparse


def read_args():
    parser = argparse.ArgumentParser(
            description=(f"Submit files on Synapse to the {GLUE_WORKFLOW_NAME} "
                        f"AWS Glue workflow."))
    parser.add_argument("--synapse-parent",
                        help="The Synapse ID of the parent folder.")
    parser.add_argument("--glue-workflow",
                        help="The name of the Glue workflow to submit to.")
    parser.add_argument("--ssm-parameter",
                        help=("The name of the SSM parameter containing "
                              "the Synapse personal access token."))
    args = parser.parse_args()
    return args


def get_synapse_client(ssm_parameter):
    ssm_client = boto3.client("ssm")
    token = ssm_client.get_parameter(
        Name=ssm_parameter,
        WithDecryption=True)
    syn = sc.Synapse()
    syn.login(authToken=token["Parameter"]["Value"])
    return syn


def submit_archives_to_workflow(syn, synapse_parent, glue_workflow):
    glue_client = boto3.client("glue")
    children = syn.getChildren(
            parent=synapse_parent,
            includeTypes=['file'])
    for archive in children:
        s3_loc = get_s3_loc(
                syn=syn,
                synapse_id=archive['id'])
        workflow_run = glue_client.start_workflow_run(Name=glue_workflow)
        glue_client.put_workflow_run_properties(
                Name=glue_workflow,
                RunId=workflow_run["RunId"],
                RunProperties={
                    "input_bucket": s3_loc["bucket"],
                    "input_key": s3_loc["key"]})


def get_s3_loc(syn, synapse_id):
    f = syn.get(synapse_id, downloadFile=False)
    bucket = f["_file_handle"]["bucketName"]
    key = f["_file_handle"]["key"]
    s3_loc = {"bucket": bucket, "key": key}
    return s3_loc


def main():
    args = read_args()
    syn = get_synapse_client(ssm_parameter=args["ssm_parameter"])
    submit_archives_to_workflow(
            syn=syn,
            synapse_parent=args["synapse_parent"],
            glue_workflow=args["glue_workflow"])

if __name__ == "__main__":
    main()

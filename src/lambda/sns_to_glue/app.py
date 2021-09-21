import os
import boto3
import synapseclient

SSM_PARAMETER_NAME = os.getenv("SSM_PARAMETER_NAME")
GLUE_WORKFLOW_NAME = os.getenv("GLUE_WORKFLOW_NAME")
synapseclient.core.cache.CACHE_ROOT_DIR = '/tmp/.synapseCache'

def handler(sns_event, context):
    message = sns_event['Records'][0]['Sns']['Message']
    ssm_client = boto3.client("ssm")
    glue_client = boto3.client("glue")
    token = ssm_client.get_parameter(
        Name=SSM_PARAMETER_NAME,
        WithDecryption=True)
    s3_loc = get_s3_loc(
            synapse_id=message,
            auth_token=token["Parameter"]["Value"])
    workflow_run = glue_client.start_workflow_run(Name=GLUE_WORKFLOW_NAME)
    glue_client.put_workflow_run_properties(
            Name=GLUE_WORKFLOW_NAME,
            RunId=workflow_run["RunId"],
            RunProperties={
                "input_bucket": s3_loc["bucket"],
                "input_key": s3_loc["key"]})

def get_s3_loc(synapse_id, auth_token):
    syn = synapseclient.Synapse()
    syn.login(authToken=auth_token)
    f = syn.get(synapse_id, downloadFile=False)
    bucket = f["_file_handle"]["bucketName"]
    key = f["_file_handle"]["key"]
    s3_loc = {"bucket": bucket, "key": key}
    return s3_loc

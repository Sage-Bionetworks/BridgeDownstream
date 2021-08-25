import synapseclient as sc
import boto3

SSM_PARAMETER_NAME = "/phil/sns_to_glue"
GLUE_WORKFLOW_NAME = "archive_to_parquet"

def handler(sns_event, context):
    return None
    message = sns_event['Records'][0]['Sns']['Message']
    ssm_client = boto3.client("ssm")
    glue_client = boto3.client("glue")
    token = ssm_client.get_parameter(
        Name=SSM_PARAMETER_NAME,
        WithDecryption=True)
    s3_loc = get_s3_loc(
            message=message,
            auth_token=token["Value"])
    workflow_run = glue_client.start_workflow_run(Name=GLUE_WORKFLOW_NAME)
    glue_client.put_workflow_run_properties(
            Name=GLUE_WORKFLOW_NAME,
            RunId=workflow_run["RunId"],
            RunProperties={
                "input_bucket": s3_loc["bucket"],
                "input_key": s3_loc["key"]})

def get_s3_loc(message, auth_token):
    syn = sc.Synapse()
    syn.login(authToken=auth_token)
    f = syn.get(message["synapseId"], downloadFile=False)
    bucket = f["_file_handle"]["bucketName"]
    key = f["_file_handle"]["key"]
    s3_loc = {"bucket": bucket, "key": key}
    return s3_loc


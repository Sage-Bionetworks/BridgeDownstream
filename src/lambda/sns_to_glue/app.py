import json
import boto3


def lambda_handler(event, context):
    glue_client = boto3.client("glue")
    messages = {} # indexed by app and study
    for record in event["Records"]:
        body = json.loads(record["body"])
        message = json.loads(body["Message"])
        message_parameters = {
            "source_bucket": message["record"]["s3Bucket"],
            "source_key": message["record"]["s3Key"],
            "raw_folder_id": message["record"]["rawFolderId"]
        }
        related_studies = message["studyRecords"].keys()
        app = message["appId"]
        for study in related_studies:
            if app in messages:
                pass
            else:
                messages[app] = {study: []}
            if study in messages[app]:
                messages[app][study].append(message_parameters)
            else:
                messages[app][study] = [message_parameters]
    for app in messages:
        for study in messages[app]:
            workflow_name = f"{app}-{study}-S3ToJsonWorkflow"
            workflow_run = glue_client.start_workflow_run(
                Name=workflow_name)
            glue_client.put_workflow_properties(
                Name=workflow_name,
                RunId=workflow_run["RunId"],
                RunProperties={
                    "messages": json.dumps(messages[app][study])
                })

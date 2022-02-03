import json
import logging
import os

import boto3
import synapseclient

SSM_PARAMETER_NAME = os.getenv('SSM_PARAMETER_NAME')
GLUE_WORKFLOW_NAME = os.getenv('GLUE_WORKFLOW_NAME')
synapseclient.core.cache.CACHE_ROOT_DIR = '/tmp/.synapseCache'

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def lambda_handler(sns_event, context):
    logger.info(sns_event)
    try:
      for record in sns_event['Records']:
        payload = record['Sns']
        synapse_id = payload['MessageAttributes']['SynapseId']['Value']
        ssm_client = boto3.client('ssm')
        glue_client = boto3.client('glue')
        token = ssm_client.get_parameter(
          Name=SSM_PARAMETER_NAME,
          WithDecryption=True)
        s3_loc = get_s3_loc(
          synapse_id=synapse_id,
          auth_token=token['Parameter']['Value'])
        logger.debug(f'S3 location info: {s3_loc}')
        logger.debug(f'Start workflow {GLUE_WORKFLOW_NAME}')
        workflow_run = glue_client.start_workflow_run(Name=GLUE_WORKFLOW_NAME)
        glue_client.put_workflow_run_properties(
          Name=GLUE_WORKFLOW_NAME,
          RunId=workflow_run['RunId'],
          RunProperties={
            'source_bucket': s3_loc['bucket'],
            'source_key': s3_loc['key']})
    except Exception as e:
      logger.error(f'An error occurred while processing SNS message: {e}')


def get_s3_loc(synapse_id, auth_token):
    syn = synapseclient.Synapse()
    syn.login(authToken=auth_token, silent=True)
    f = syn.get(synapse_id, downloadFile=False)
    logger.debug(f'synapse get response: {f}')
    bucket = f['_file_handle']['bucketName']
    key = f['_file_handle']['key']
    s3_loc = {'bucket': bucket, 'key': key}
    return s3_loc

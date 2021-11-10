'''
This script creates a Synpase project, connects to an existing S3 bucket,
and syncs test data files to the project.
'''
import argparse
import logging
import sys

import boto3
import synapseclient
from synapseformation import client as synapseformation_client
import synapseutils


project_name = 'BridgeDownstreamTest'

logging.basicConfig()
log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)


def get_synapse_client(ssm_parameter):
  '''Get an instance of the synapse client'''
  ssm_client = boto3.client('ssm')
  token = ssm_client.get_parameter(
    Name=ssm_parameter,
    WithDecryption=True)
  syn = synapseclient.Synapse()
  syn.login(authToken=token['Parameter']['Value'])
  return syn


def get_project_id(syn, principal_id):
  '''Get the id of the synapse project if it exists'''
  log.info(f'Get project id for {project_name}')
  projects = syn.restGET(f'/projects/user/{principal_id}')
  BridgeDownstreamTest = next(
    filter(
      lambda x: x['name'] == project_name,
      projects.get('results')
      ),
    None)
  return '' if BridgeDownstreamTest is None else BridgeDownstreamTest.get('id')


def create_project(syn, template_path):
  '''Create a synapse project from a template'''
  log.info(f'Create project {project_name}')
  try:
    response = synapseformation_client.create_synapse_resources(template_path)
    if response is not None:
      return response.get('id')
  except Exception as e:
      print(e)
      sys.exit(1)


def setup_external_storage(syn, bucket_name, project_id, synapse_folder_name):
  '''Connect bucket as external storage for the Synapse project'''
  log.info(f'Use s3 bucket {bucket_name} as storage for {project_name}')
  storage_location = syn.create_s3_storage_location(
          parent = project_id,
          folder_name = synapse_folder_name,
          bucket_name = bucket_name)
  storage_location_info = {
         k: v for k, v in
         zip(['synapse_folder', 'storage_location', 'synapse_project'],
             storage_location)}
  return(storage_location_info)


def get_folder_id(syn, project_id, synapse_folder_name):
  log.info(f'Get synapse id for {synapse_folder_name} folder')
  response = syn.getChildren(project_id, includeTypes=['folder'])
  folder = next(
    filter(
      lambda x: x['name'] == synapse_folder_name,
      response
      ),
    None)
  return '' if folder is None else folder.get('id')

def main():

  log.info(f'Begin setting up test data')

  # get synapse client
  ssm_parameter = 'synapse-bridgedownstream-auth'
  syn = get_synapse_client(ssm_parameter=ssm_parameter)

  # see if project exists and get its id
  principal_id = '3432808' # BridgeDownstream Synapse service account
  project_id = get_project_id(syn, principal_id)
  if not project_id:
    template_path = './src/scripts/setup_test_data/synapse-formation.yaml'
    project_id = create_project(syn, template_path)

  # connect bucket and project
  bucket_name = 'bridge-downstream-dev-source'
  synapse_folder_name = 'test-data'
  storage_location_info = setup_external_storage(syn, bucket_name, project_id, synapse_folder_name)
  log.info(f'storage_location_info: {storage_location_info}')

  # get folder synapse id
  folder_id = get_folder_id(syn, project_id, synapse_folder_name)
  log.info(f'folder_id: {folder_id}')

  # generate synapse manifest
  file_dir_to_sync = './src/scripts/setup_test_data/data'
  sync_manifest_path = '/tmp/BridgeDownstreamTest-manifest'
  synapseutils.sync.generate_sync_manifest(syn, file_dir_to_sync, folder_id, sync_manifest_path)

  # sync the test data files to Synapse
  synapseutils.sync.syncToSynapse(syn, sync_manifest_path, dryRun=False, sendMessages=False)


if __name__ == "__main__":
  main()

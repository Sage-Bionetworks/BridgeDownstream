'''
This script creates a Synpase project, connects to an existing S3 bucket,
and syncs test data files to the project.
'''
import logging
import os
import sys

import boto3
import numpy as np
import pandas as pd
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
  synapse_auth_token = token['Parameter']['Value']
  os.environ['SYNAPSE_AUTH_TOKEN'] = synapse_auth_token
  syn.login(authToken=synapse_auth_token, silent=True)
  return syn


def get_project_id(syn, principal_id):
  '''Get the id of the synapse project if it exists'''
  log.info(f'Get project id for {project_name}')
  projects = syn.restGET(f'/projects/user/{principal_id}')
  log.info(projects)
  BridgeDownstreamTest = next(
    filter(
      lambda x: x['name'] == project_name,
      projects.get('results')
      ),
    None)
  return '' if BridgeDownstreamTest is None else BridgeDownstreamTest.get('id')


def create_project(syn, template_path):
  '''Create a synapse project from a template'''
  log.info(f'Create project {project_name} with template_path {template_path}')
  try:
    response = synapseformation_client.create_synapse_resources(syn, template_path)
    log.debug(f'Project response: {response}')
    if response is not None:
      return response.get('id')
  except Exception as e:
      print(e)
      sys.exit(1)


def setup_external_storage(syn, bucket_name, project_id, folder_id):
  '''Connect bucket as external storage for the Synapse project'''
  log.info(f'Use s3 bucket {bucket_name} as storage for {project_name}, ' +
    f'with folder {folder_id}.')
  storage_location = syn.create_s3_storage_location(
          parent = project_id,
          folder = folder_id,
          bucket_name = bucket_name,
          sts_enabled=True)
  storage_location_info = {
         k: v for k, v in
         zip(['synapse_folder', 'storage_location', 'synapse_project'],
             storage_location)}
  return(storage_location_info)


def get_folder_id(syn, project_id, synapse_folder_name):
  log.info(f'Get synapse id for {synapse_folder_name} folder, child of project {project_id}')
  response = list(syn.getChildren(project_id, includeTypes=['folder']))
  folder = next(item for item in response if item['name'] == synapse_folder_name)
  log.debug(f'folder: {folder}')
  return '' if folder is None else folder.get('id')

def main():

  log.info(f'Begin setting up test data')

  # get synapse client
  ssm_parameter = 'synapse-bridgedownstream-auth'
  syn = get_synapse_client(ssm_parameter=ssm_parameter)

  # see if project exists and get its id

  principal_id = '3432808' # BridgeDownstream Synapse service account
  project_id = get_project_id(syn, principal_id)

  # if there's a project id, assume the project is already connected to synapse
  connected_to_synapse = True if project_id else False

  # if no project id is available, create a new project
  if not project_id:
    template_path = './src/scripts/setup_test_data/synapse-formation.yaml'
    create_project(syn, template_path)
    project_id = get_project_id(syn, principal_id)

  # get folder synapse id
  synapse_folder_name = 'test-data'
  folder_id = get_folder_id(syn, project_id, synapse_folder_name)
  log.info(f'folder_id: {folder_id}')

  # connect bucket and project if this is a newly made project
  if not connected_to_synapse:
    bucket_name = 'bridge-downstream-dev-source'
    storage_location_info = setup_external_storage(syn, bucket_name, project_id, folder_id)
    log.info(f'storage_location_info: {storage_location_info}')

  # generate synapse manifest
  data_dir = './src/scripts/setup_test_data/data'
  manifest_data = np.array([
    [f'{data_dir}/1Dbywpp4w2zhXjZPgItrKvlh-raw.zip', folder_id, '1Dbywpp4w2zhXjZPgItrKvlh', '|mtb-alpha=NNJ18004|', 'SdVJHmCC8Dcpd_X-sjEK-wH3', '1623421775829', 'FNAME Test Form 1'],
    [f'{data_dir}/ugJWvqPLhnFV-ifu246CeZjZ-raw.zip', folder_id, 'ugJWvqPLhnFV-ifu246CeZjZ', '|mtb-alpha=MCH20002|', 'vsq7vt_iuT6xjntX09QoQtwk', '1624574992880', 'Vocabulary Form 1'],
    [f'{data_dir}/R7mBYI63NpCo3sJa9l0qnpza-raw.zip', folder_id, 'R7mBYI63NpCo3sJa9l0qnpza', '|mtb-alpha=NBS22002|', 'nzaLFo5nhlqHc1TyUKhvsBP7', '1625238358060', 'MTB Spelling Form 1'],
    [f'{data_dir}/Le9-WBxplHhGXo_FLNiY3SaI-raw.zip', folder_id, 'Le9-WBxplHhGXo_FLNiY3SaI', '|mtb-alpha=SBM50004|', 'XrvTSTKobPJAbxGwZ4sceNBf', '1624322928949', 'Vocabulary Form 1']
  ])
  df = pd.DataFrame(manifest_data, columns = ['path', 'parent', 'recordid', 'substudymemberships', 'healthcode', 'createdon', 'taskidentifier'])
  manifest_path = '/tmp/manifest.tsv'
  df.to_csv(manifest_path, sep = '\t', index = False)

  # sync the test data files to Synapse
  sync_response = synapseutils.sync.syncToSynapse(syn, manifest_path, dryRun=False, sendMessages=True)


if __name__ == "__main__":
  main()

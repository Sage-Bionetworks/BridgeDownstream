'''
This script creates a Synapse project if that project does not yet exist,
executes a query over a raw data folder, samples one of each assessment
revision for both iOS and Android platforms from that query, adds those records
to a (potentially preexisting) namespaced dataset if the items are not
already present, and creates a stable version (snapshot) of the dataset
if any new items were added, otherwise no snapshot is created.

This script will write the "psuedo" query which curated the dataset to the
dataset's snapshot comment. This "psuedo" query replaces what would
normally be a file view's Synapse ID in the FROM clause with the Synapse ID
of the bridge raw data folder, which contains production data from Bridge.
The default behavior of this script is to
take the first instance of each assessment revision for each assessment
(for each osName/appVersion, i.e., each build on each platform iPhone OS
or Android) from `syn26253352`, which contains all MTB data from across
all studies. Hence, the default behavior of this script is to curate a
dataset that is representative of any data which we might
encounter from the MTB app.
'''
import argparse
import json
import logging
import re
import sys
import uuid

import boto3
import pandas
import synapseclient
from synapseformation import client as synapseformation_client

PROJECT_NAME = 'BridgeDownstreamTest'
DEFAULT_QUERY = (
    "SELECT * FROM {source_table} WHERE "
    "assessmentId IS NOT NULL AND "
    "assessmentRevision IS NOT NULL AND "
    "clientInfo IS NOT NULL "
    "ORDER BY exportedOn"
    )

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def read_args():
  parser = argparse.ArgumentParser(
      description="Create a new test project or test dataset on Synapse")
  parser.add_argument(
      "--raw-data-folder",
      help=(
        "Optional. Synapse ID of the Bridge Raw Data folder "
        "to query for test data. Defaults to syn26253352, which "
        "contains MTB data from across all studies."),
      default="syn26253352")
  parser.add_argument(
      "--raw-data-query",
      help=(
        "Optional. A formatted string query to run against the "
        "--raw-data-folder to select test data. Use {source_table} in the "
        "FROM clause. Defaults to sorting by exportedOn and selecting the "
        "first instance of each assessment revision."),
      default=DEFAULT_QUERY)
  parser.add_argument(
      "--namespace",
      help=(
        "Optional. A testing stack identifier. "
        "Default bridge-downstream"),
      default="bridge-downstream")
  args = parser.parse_args()
  return args


def get_synapse_client(ssm_parameter):
  '''Get an instance of the synapse client'''
  ssm_client = boto3.client('ssm')
  token = ssm_client.get_parameter(
      Name=ssm_parameter,
      WithDecryption=True)
  syn = synapseclient.Synapse()
  synapse_auth_token = token['Parameter']['Value']
  syn.login(authToken=synapse_auth_token, silent=True)
  return syn


def get_project_id(syn, principal_id):
  '''Get the id of the synapse project if it exists'''
  logger.info(f'Getting Synapse project id for {PROJECT_NAME}')
  projects = syn.restGET(f'/projects/user/{principal_id}')
  BridgeDownstreamTest = next(
      filter(
        lambda x: x['name'] == PROJECT_NAME,
        projects.get('results')
        ),
      None)
  return '' if BridgeDownstreamTest is None else BridgeDownstreamTest.get('id')


def create_project(syn, template_path):
  '''Create a synapse project from a template'''
  logger.info(f'Creating Synapse project {PROJECT_NAME}, ' +
      f'with template_path {template_path}')
  try:
    response = synapseformation_client.create_synapse_resources(syn, template_path)
    logger.debug(f'Project response: {response}')
    if response is not None:
      return response.get('id')
  except Exception as e:
    logger.error(e)
    sys.exit(1)


class TempFileView():
  '''A class to be used in a "with" statement

  Handles creation and deletion of a file view over a Synapse Folder.'''
  def __init__(self, syn, parent, scope):
    self.syn = syn
    self.parent = parent
    self.scope = scope
    self.view = None

  def __enter__(self):
    # create file view
    view = synapseclient.EntityViewSchema(
        name=str(uuid.uuid4()),
        parent=self.parent,
        scopes=[self.scope],
        includeEntityTypes=[synapseclient.EntityViewType.FILE],
        addDefaultViewColumns=True)
    self.view = self.syn.store(view)
    return self

  def __exit__(self, type, value, traceback):
    self.syn.delete(self.view["id"])

  def as_data_frame(self, query_str, as_dataset_items=False):
    q = self.syn.tableQuery(query_str.format(source_table = self.view["id"]))
    df = q.asDataFrame()
    if as_dataset_items:
      dataset_items = [
          {"entityId": i, "versionNumber": v}
          for i, v in zip(df["id"], df["currentVersion"])]
      return dataset_items
    return df


def snapshot_stable_dataset_version(syn, dataset, query_info):
  '''Create a snapshot of a dataset and write query info to the version comments'''
  syn.restPOST(
      f"/entity/{dataset['id']}/table/transaction/async/start",
      body=json.dumps({
        "concreteType": "org.sagebionetworks.repo.model.table.TableUpdateTransactionRequest",
        "entityId": dataset["id"],
        "createSnapshot": True,
        "changes": [],
        "snapshotOptions": {
          "snapshotComment": query_info
          }
        })
      )


def create_or_update_dataset(
    syn, parent_project, dataset_name, column_ids, dataset_items, query_info):
  '''Create or update a Synapse dataset

  If the dataset does not yet exist, it is created.
  If the dataset exists but already contains the dataset items, does nothing.
  If the dataset exists and there are new dataset items, those items are added
  to the dataset's items and a stable version (snapshot) is published.'''
  datasets = syn.getChildren(
      parent=parent_project,
      includeTypes=["dataset"])
  for dataset in datasets:
    if dataset["name"] == dataset_name:
      dataset = syn.restGET(f"/entity/{dataset['id']}")
      existing_items = dataset.pop("items")
      existing_items_id = [ei["entityId"] for ei in existing_items]
      all_items = existing_items + [
          di for di in dataset_items if di["entityId"] not in existing_items_id]
      if len(all_items) == len(existing_items):
        # No new items to add to dataset
        return dataset
      dataset = syn.restPUT(
          f"/entity/{dataset['id']}",
          body=json.dumps({
            "items": all_items,
            **dataset})
          )
      snapshot_stable_dataset_version(
          syn=syn,
          dataset=dataset,
          query_info=query_info)
      return dataset
  # Did not find pre-existing dataset
  dataset = syn.restPOST(
      "/entity",
      body=json.dumps({
        "name": dataset_name,
        "parentId": parent_project,
        "concreteType": "org.sagebionetworks.repo.model.table.Dataset",
        "columnIds": column_ids,
        "items": dataset_items})
      )
  snapshot_stable_dataset_version(
      syn=syn,
      dataset=dataset,
      query_info=query_info)
  return dataset

def parse_client_info_metadata(client_info_str):
  '''Return a dict with appVersion and osName whether clientInfo uses the old
  or new (JSON) format.'''
  try:
    client_info = json.loads(client_info_str)
    if not ("appVersion" in client_info and "osName" in client_info):
      client_info = {
          "appVersion": None,
          "osName": None
          }
  except json.JSONDecodeError:
    app_version_pattern = re.compile(r"appVersion=[^,]+")
    os_name_pattern = re.compile(r"osName=[^,]+")
    app_version_search = re.search(app_version_pattern, client_info_str)
    os_name_search = re.search(os_name_pattern, client_info_str)
    if app_version_search is None:
      app_version = None
      print(client_info_str)
    else:
      try:
        app_version = int(app_version_search.group().split("=")[1])
      except ValueError:
        app_version = None
    if os_name_search is None:
      os_name = None
      print(client_info_str)
    else:
      os_name = os_name_search.group().split("=")[1]
    client_info = {
        "appVersion": app_version,
        "osName": os_name}
    return client_info

def sample_assessment_revisions(df):
  '''Sample first instance of iPhone OS and Android for each assessment revision'''
  parsed_client_info = df.clientInfo.apply(parse_client_info_metadata)
  client_info_df = pandas.DataFrame({
    "appVersion": parsed_client_info.apply(lambda c : c["appVersion"] if c is not None else c),
    "osName": parsed_client_info.apply(lambda c : c["osName"] if c is not None else c)
    })
  df = (
      df
      .join(client_info_df)
      .dropna(subset=["appVersion", "osName"])
      )
  df.loc[:,"appVersion"] = df.loc[:,"appVersion"].astype(int)
  df = (
      df
      .sort_values("exportedOn")
      .drop_duplicates(
        subset=["assessmentId", "assessmentRevision", "osName", "appVersion"])
      .query("osName == 'Android' | osName == 'iPhone OS'")
      )
  dataset_items = [
      {"entityId": i, "versionNumber": v}
      for i, v in zip(df["id"], df["currentVersion"])]
  return dataset_items

def curate_test_dataset(syn, parent_project, raw_data_folder, query_str, namespace):
  '''Curates a test dataset by querying for dataset items over a raw data folder'''
  with TempFileView(syn, parent_project, raw_data_folder) as v:
    df = v.as_data_frame(query_str)
    dataset_items = sample_assessment_revisions(df=df)
    query_info = {"query": query_str.format(source_table = raw_data_folder)}
    dataset = create_or_update_dataset(
        syn=syn,
        parent_project=parent_project,
        dataset_name=f"{namespace}-test-dataset",
        column_ids=v.view.columnIds,
        dataset_items=dataset_items,
        query_info=query_info)
    return dataset

def main():
  logger.info('Begin setting up test data.')
  args = read_args()

  # get synapse client
  ssm_parameter = 'synapse-bridgedownstream-auth'
  syn = get_synapse_client(ssm_parameter=ssm_parameter)

  # see if project exists and get its id

  principal_id = '3432808' # BridgeDownstream Synapse service account
  project_id = get_project_id(syn, principal_id)

  # if no project id is available, create a new project
  if not project_id:
    template_path = './src/scripts/setup_test_data/synapse-formation.yaml'
    create_project(syn, template_path)
    project_id = get_project_id(syn, principal_id)

  # Create test data Dataset from query
  curate_test_dataset(
      syn=syn,
      parent_project=project_id,
      raw_data_folder=args.raw_data_folder,
      query_str=args.raw_data_query,
      namespace=args.namespace)
  logger.info('Test data setup complete.')


if __name__ == "__main__":
  main()

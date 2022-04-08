'''
This is a utility script that generates a fake SQS message in json format
that is used to test the lambda, which in turn will exercise the Glue pipeline.

This contains a JSON message inside an SNS message which is transmitted
inside the SQS message.
'''
import argparse
import copy
import json
import sys
import boto3
import synapseclient

SINGLE_RECORD_OUTFILE = 'single-record.json'
MULTI_RECORD_OUTFILE = 'records.json'
SSM_PARAMETER = 'synapse-bridgedownstream-auth'

sqs_record_template = {
  "messageId" : "20530d39-538a-5330-b376-57429074a158",
  "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
  "attributes": {
      "ApproximateReceiveCount": "1",
      "SentTimestamp": "1545082649183",
      "SenderId": "AIDAIENQZJOLO23YVJ4VO",
      "ApproximateFirstReceiveTimestamp": "1545082649185"
  },
  "body" : "",
  "messageAttributes": {},
  "md5OfBody": "e4e68fb7bd0e697a0ae8f1bb342846b3",
  "eventSource": "aws:sqs",
  "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue",
  "awsRegion": "us-east-1"
}

sns_record_template = {
    "Type": "Notification",
    "MessageId": "45127357-4996-58e1-af22-2922586ba8f2",
    "TopicArn": "arn:aws:sns:us-east-1:563295687221:dummy-bridge-topic",
    "Message": "",
    "Timestamp": "2022-02-02T23:11:57.105Z",
    "SignatureVersion": "1",
    "Signature": "uJ4zpc5M/dImqUxw2uABcl8V2WeBkXRZolX4wwtVxyqp/OG5IqR0upEH35Pp7WHx2/tpAzMnSImjOFsqfveFce4cDum1CtQtlj7mkZyxq+sV1VKxgJot2N8DzMxTBxVmNELc9fbOGgukSwv76dQJ0tiu0GUITmL/8tHcRacimPkElPL6ZC9jFIiR0MM6f2wZkwbRMbvfo1sOdjYcF9VzD4J0fe6qbHjKFGoTGYQ98hJCgMU8mknTHWoGu2InLPAOZZ+hNl+gt/lCS7oihP1rBMoGg+yi8wF/F2bcoKierEuF5DmAkPkxOHi7j8ikfBmJ2o/zDFknx6XmRL4a9rMUow==",
    "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",
    "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:563295687221:phil_s3_bucket_update:89b889dd-9e68-41a5-b865-c194f09d024c"
}

def create_message_template(entity):
  return {
    "appId": "example-app-1",
    "recordId": entity.annotations.recordId.pop(),
    "record": {
      "parentProjectId": "syn0000000",
      "rawFolderId": entity.parentId,
      "fileEntityId": entity.id,
      "s3Bucket": entity._file_handle.bucketName,
      "s3Key": entity._file_handle.key
    },
    "studyRecords": {
      "study-1": {
        "parentProjectId": "syn0000000",
        "rawFolderId": "syn0000000",
        "fileEntityId": "syn0000000",
        "s3Bucket": "dummy-bucket",
        "s3Key": "dummy-key"
      }
    }
  }


def read_args():
  parser = argparse.ArgumentParser(
    description='Generate a json file of a mocked SQS event for testing.')
  parser.add_argument('--synapse-project-id',
    default='syn26721259',
    help='Synapse ID of the BridgeDownstreamTest project')
  parser.add_argument('--dataset-name',
    default='bridge-downstream-test-dataset',
    help=('Optional. Name of the Synapse dataset containing test data. '
          'Default is "bridge-downstream-test-dataset".'))
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


def get_dataset_id_by_name(syn, parent_id, dataset_name):
  response = list(syn.getChildren(parent_id, includeTypes=['dataset']))
  dataset = next(item for item in response if item['name'] == dataset_name)
  dataset_id = '' if dataset is None else dataset.get('id')
  if not dataset_id:
    print(f'No dataset {dataset_name} exists')
    sys.exit(1)
  return dataset_id
  return dataset


def get_latest_stable_dataset(syn, dataset_metadata):
  '''Gets the most recently snapshotted dataset

  A snapshotted dataset is a 'stable' version,
  in contrast with the current latest version of the dataset,
  which is the 'draft' version. The most recently snapshotted dataset
  is therefore the version one less than the current latest version.
  '''
  if dataset_metadata.get("versionNumber") < 2:
    raise Exception("No stable dataset version found.")
  stable_dataset_version = int(dataset_metadata["versionNumber"]) - 1
  stable_dataset_id = f"{dataset_metadata['id']}.{stable_dataset_version}"
  stable_dataset = syn.tableQuery(f"SELECT * FROM {stable_dataset_id}").asDataFrame()
  return stable_dataset


def main():
  args = read_args()
  syn = get_synapse_client(ssm_parameter=SSM_PARAMETER)
  test_dataset_id = get_dataset_id_by_name(
          syn=syn,
          parent_id=args.synapse_project_id,
          dataset_name=args.dataset_name)
  test_dataset_metadata = syn.restGET(f"/entity/{test_dataset_id}")
  test_dataset = get_latest_stable_dataset(
      syn=syn,
      dataset_metadata=test_dataset_metadata)
  test_data = [
      syn.get(entity=synapse_id, version=version, downloadFile=False)
      for synapse_id, version
      in zip(test_dataset.id, test_dataset.currentVersion)]
  sns_messages = [create_message_template(d) for d in test_data]
  records = []
  print(f'Generating mock sqs event from response...')
  for message in sns_messages:
    sqs_record = copy.deepcopy(sqs_record_template)
    sns_record = copy.deepcopy(sns_record_template)

    sns_record['Message'] = json.dumps(message)
    sqs_record['body'] = json.dumps(sns_record)
    sqs_record['messageAttributes'] = {
        "dataset_name": test_dataset_metadata["name"],
        "dataset_id": test_dataset_metadata["id"],
        "dataset_version": int(test_dataset_metadata["versionNumber"]) - 1,
        }
    records.append(sqs_record)

  multi_record_content = {}
  multi_record_content['Records'] = records
  single_record_content = {}
  single_record_content['Records'] = [records[0]]

  with open(MULTI_RECORD_OUTFILE, 'w') as outfile:
    json.dump(multi_record_content, outfile)
    print(f'Multiple records written to {outfile.name}.')

  with open(SINGLE_RECORD_OUTFILE, 'w') as outfile:
    json.dump(single_record_content, outfile)
    print(f'Single record written to {outfile.name}.')

  print('Done.')


if __name__ == "__main__":
    main()

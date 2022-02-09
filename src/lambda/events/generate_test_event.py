'''
This is a utility script that generates a fake SQS message in json format
that is used to test the lambda, which in turn will exercise the Glue pipeline.

This contains a JSON message inside an SNS message which is transmitted
inside the SQS message.
'''
import argparse
import copy
import json
import synapseclient

SINGLE_RECORD_OUTFILE = 'single-record.json'
MULTI_RECORD_OUTFILE = 'records.json'

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
    "TopicArn": "arn:aws:sns:us-east-1:563295687221:phil_s3_bucket_update",
    "Message": "",
    "Timestamp": "2022-02-02T23:11:57.105Z",
    "SignatureVersion": "1",
    "Signature": "uJ4zpc5M/dImqUxw2uABcl8V2WeBkXRZolX4wwtVxyqp/OG5IqR0upEH35Pp7WHx2/tpAzMnSImjOFsqfveFce4cDum1CtQtlj7mkZyxq+sV1VKxgJot2N8DzMxTBxVmNELc9fbOGgukSwv76dQJ0tiu0GUITmL/8tHcRacimPkElPL6ZC9jFIiR0MM6f2wZkwbRMbvfo1sOdjYcF9VzD4J0fe6qbHjKFGoTGYQ98hJCgMU8mknTHWoGu2InLPAOZZ+hNl+gt/lCS7oihP1rBMoGg+yi8wF/F2bcoKierEuF5DmAkPkxOHi7j8ikfBmJ2o/zDFknx6XmRL4a9rMUow==",
    "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",
    "UnsubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:563295687221:phil_s3_bucket_update:89b889dd-9e68-41a5-b865-c194f09d024c"
}

def create_message_template(project_id, folder_id):
  return {
    "appId": "example-app-1",
    "recordId": "-4I2GOqDSdjaXsbuw8oYXBKK",
    "record": {
      "parentProjectId": project_id,
      "rawFolderId": folder_id,
      "fileEntityId": "",
      "s3Bucket": "",
      "s3Key": ""
    },
    "studyRecords": {
      "study-1": {
        "parentProjectId": project_id,
        "rawFolderId": folder_id,
        "fileEntityId": "",
        "s3Bucket": "",
        "s3Key": ""
      }
    }
  }


def read_args():
  parser = argparse.ArgumentParser(
    description='Generate a json file of a mocked SNS event for testing.')
  parser.add_argument('--synapse-project-id',
    default='syn26721259',
    help='Synapse ID of the BridgeDownstreamTest project')
  args = parser.parse_args()
  return args


def main():
  args = read_args()
  project_id = args.synapse_project_id

  syn = synapseclient.Synapse()
  syn.login()

  folder_name = 'test-data'
  print(f'Get folder id')
  response = list(syn.getChildren(project_id, includeTypes=['folder']))
  folder = next(item for item in response if item['name'] == folder_name)
  folder_id = '' if folder is None else folder.get('id')
  if not folder_id:
    print(f'No folder {folder_name} exists')
    sys.exit(1)

  message_template = create_message_template(project_id, folder_id)
  print(f'Fetching children of synapse id {folder_id}...')
  response = list(syn.getChildren(folder_id, includeTypes=['file']))
  records = []
  print(f'Generating mock sqs event from response...')
  for item in response:
    syn_id = item['id']
    sqs_record = copy.deepcopy(sqs_record_template)
    sns_record = copy.deepcopy(sns_record_template)
    message = copy.deepcopy(message_template)
    get_response = syn.get(entity=syn_id, downloadFile=False)
    bucket = get_response._file_handle['bucketName']
    key = get_response._file_handle['key']

    outer_record = message['record']
    outer_record['fileEntityId'] = syn_id
    outer_record['s3Bucket'] = bucket
    outer_record['s3Key'] = key

    inner_record = message['studyRecords']['study-1']
    inner_record['fileEntityId'] = syn_id
    inner_record['s3Bucket'] = bucket
    inner_record['s3Key'] = key

    sns_record['Message'] = json.dumps(message)
    sqs_record['body'] = json.dumps(sns_record)
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

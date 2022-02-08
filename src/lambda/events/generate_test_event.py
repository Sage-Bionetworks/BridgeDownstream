'''
This is a utility script that generates a fake SNS message in json format
that is used to test the lambda, which in turn will exercise the Glue pipeline.
'''
import argparse
import copy
import json
import synapseclient

SINGLE_RECORD_OUTFILE = 'single-record.json'
MULTI_RECORD_OUTFILE = 'records.json'
# record_template = {
#   "EventVersion": "1.0",
#   "EventSubscriptionArn": "arn:aws:sns:us-east-2:123456789012:sns-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
#   "EventSource": "aws:sns",
#   "Sns": {
#     "SignatureVersion": "1",
#     "Timestamp": "2019-01-02T12:45:07.000Z",
#     "Signature": "tcc6faL2yUC6dgZdmrwh1Y4cGa/ebXEkAi6RibDsvpi+tE/1+82j...65r==",
#     "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem",
#     "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
#     "Message": "Bridge data update",
#     "MessageAttributes": {
#       "SynapseId": {
#         "Type": "String"
#       }
#     },
#     "Type": "Notification",
#     "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
#     "TopicArn":"arn:aws:sns:us-east-2:123456789012:sns-lambda",
#     "Subject": "TestInvoke"
#   }
# }

record_template = {
  "Type" : "Notification",
  "MessageId" : "20530d39-538a-5330-b376-57429074a158",
  "TopicArn" : "arn:aws:sns:us-east-1:634761300905:example-app-1-study-1-LambdaStack-KBX3NOTR34J9-SnsTopic-1NQI61F85369P",
  "Subject" : "foobar",
  "Message" : "",
  "Timestamp" : "2022-02-08T01:05:02.607Z",
  "SignatureVersion" : "1",
  "Signature" : "eglUUIYTcyTaad77ORCOI/DEkzZfl83J+OLjl4RwhqOY2ZOpI5/A6ItoYHMB+PwuG7w26Pc/uNvKT384NbPourkpAAznOkakKMA99wYAw1sO1v82k/DR/s/pQAAygM9u/WaF6VT9NFEQgf8RtWiS6MclYWpY0lG7TF/1Wux3RYjgS3659BYERUwOBzVDcrrqDJwHMyWe+CAFi6adu7Lx5b+FV3jwE84hZOVKOO1WkE6ZuYu6zXxHyGSOsfcjFr21tegDL00LBL4EkTUbYIKZHlPvsX9Edhi1dg/7Ws9NG+Sh9/SPZotiY5Vd0UkWgUeLaP7JtnJ8QYe1sMMI9HDH0Q==",
  "SigningCertURL" : "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-7ff5318490ec183fbaddaa2a969abfda.pem",
  "UnsubscribeURL" : "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:634761300905:example-app-1-study-1-LambdaStack-KBX3NOTR34J9-SnsTopic-1NQI61F85369P:2084d1ed-7a6b-4eb5-bffa-0c2a65f9431b"
}

def create_message_template(project_id, folder_id):
  return {
    "appId": "example-app",
    "recordId": "-4I2GOqDSdjaXsbuw8oYXBKK",
    "record": {
      "parentProjectId": project_id,
      "rawFolderId": folder_id,
      "fileEntityId": "syn26861165",
      "s3Bucket": "org-sagebridge-rawhealthdata-prod",
      "s3Key": "mobile-toolbox/2022-01-20/-4I2GOqDSdjaXsbuw8oYXBKK-MTB_Picture_Sequence_Memory"
    },
    "studyRecords": {
      "studyA": {
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
    record = copy.deepcopy(record_template)
    message = copy.deepcopy(message_template)
    study_a_record = message['studyRecords']['studyA']
    study_a_record['fileEntityId'] = syn_id
    get_response = syn.get(entity=syn_id, downloadFile=False)
    study_a_record['s3Bucket'] = get_response._file_handle['bucketName']
    study_a_record['s3Key'] = get_response._file_handle['key']
    record['Message'] = str(message)
    #record['Sns']['MessageAttributes']['SynapseId']['Value'] = item['id']
    records.append(record)
  multi_record_content = {}
  multi_record_content['Records'] = records
  single_record_content = {}
  single_record_content['Records'] = records[0]

  with open(MULTI_RECORD_OUTFILE, 'w') as outfile:
    json.dump(multi_record_content, outfile)
    print(f'Multiple records written to {outfile.name}.')

  with open(SINGLE_RECORD_OUTFILE, 'w') as outfile:
    json.dump(single_record_content, outfile)
    print(f'Single record written to {outfile.name}.')

  print('Done.')


if __name__ == "__main__":
    main()

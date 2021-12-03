'''
This is a utility script that generates a fake SNS message in json format
that is used to test the lambda, which in turn will exercise the Glue pipeline.
'''
import argparse
import copy
import json
import synapseclient


record_template = {
  "EventVersion": "1.0",
  "EventSubscriptionArn": "arn:aws:sns:us-east-2:123456789012:sns-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
  "EventSource": "aws:sns",
  "Sns": {
    "SignatureVersion": "1",
    "Timestamp": "2019-01-02T12:45:07.000Z",
    "Signature": "tcc6faL2yUC6dgZdmrwh1Y4cGa/ebXEkAi6RibDsvpi+tE/1+82j...65r==",
    "SigningCertUrl": "https://sns.us-east-2.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem",
    "MessageId": "95df01b4-ee98-5cb9-9903-4c221d41eb5e",
    "Message": "Bridge data update",
    "MessageAttributes": {
      "SynapseId": {
        "Type": "String"
      }
    },
    "Type": "Notification",
    "UnsubscribeUrl": "https://sns.us-east-2.amazonaws.com/?Action=Unsubscribe&amp;SubscriptionArn=arn:aws:sns:us-east-2:123456789012:test-lambda:21be56ed-a058-49f5-8c98-aedd2564c486",
    "TopicArn":"arn:aws:sns:us-east-2:123456789012:sns-lambda",
    "Subject": "TestInvoke"
  }
}

def read_args():
  parser = argparse.ArgumentParser(
    description='Generate a json file of a mocked SNS event for testing.')
  parser.add_argument('--synapse-folder-id',
    default='syn26467502',
    help='Synapse ID of the test-data folder in the BridgeDownstreamTest project')
  parser.add_argument('--filename',
    default='records.json',
    help='Name of the output file.')
  args = parser.parse_args()
  return args


def main():
  args = read_args()
  folder_id = args.synapse_folder_id
  filename = args.filename
  syn = synapseclient.Synapse()
  syn.login()

  print(f'Fetching children of synapse id {folder_id}...')
  response = list(syn.getChildren(folder_id, includeTypes=['file']))
  records = []
  print(f'Generating mock sns event from response...')
  for item in response:
    record = copy.deepcopy(record_template)
    record['Sns']['MessageAttributes']['SynapseId']['Value'] = item['id']
    records.append(record)

  with open(filename, 'w') as outfile:
    json.dump(records, outfile)
    print(f'Done. Event written to {outfile.name}.')


if __name__ == "__main__":
    main()

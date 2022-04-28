# sns_to_glue

The sns_to_glue lambda subscribes to an SNS topic that sends SNS messages
when new data is available to be processed. It starts the S3-to-Json workflow.

## Development

The Serverless Application Model Command Line Interface (SAM CLI) is an
extension of the AWS CLI that adds functionality for building and testing
Lambda applications.

To use the SAM CLI, you need the following tools.

* SAM CLI - [Install the SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html)
* Docker - [Install Docker community edition](https://hub.docker.com/search/?type=edition&offering=community)

You may need the following for local testing.
* [Python 3 installed](https://www.python.org/downloads/)

## Use the SAM CLI to build and test locally

Build your application with the `sam build` command.

```bash
lambda$ sam build
```

Test the function by invoking it directly with a test event.

## Test events

### Creating test events
Ready made test events are included in the `events` folder. If you would like to create new events,
you can use the `events/generate_test_event.py` script. This script takes
a Synapse dataset as input and includes the dataset name, version, and Synapse ID
in each SQS event's `messageAttributes` property, so you can always trace an event back
to the dataset which it derived from. For example, the events provided in the `events`
folder come from the `bridge-downstream-test-dataset` dataset, which is the dataset
we primarily use for testing. To create your own Synapse dataset containing
the entities which you would like to use in your test events, see the parent
directory `src/scripts/setup_test_data/setup_test_data.py`. This script
writes the "psuedo" query which curates the dataset to each dataset's
snapshot version comment property. This "psuedo" query replaces what would
normally be a file view's Synapse ID in the FROM clause with the Synapse ID
of the Bridge Raw Data folder, which contains production data from Bridge. For
example, we can see that the `bridge-downstream-test-dataset` was curated by
taking the first instance of each assessment revision for each assessment
from `syn26253352`, which contains all MTB data from across all studies.
*Hence, the `bridge-downstream-test-dataset` is representative of every
collection of data which we might encounter, as of the most recent snapshot date.*

### Invoking test events

To test the lambda locally, run the following command from the lambda directory.
Use `singe-record.json` to submit just one record to the Glue workflow or
`records.json` to submit multiple. The file `test-env-vars.json` contains
the namespace environment variable that is expected by the lambda script.
Don't forget to update the value of this variable
if you are testing a stack deployed as part of a feature branch.

First, configure your AWS credentials, if you have not already done so.

To invoke the lambda with one event:
```bash
lambda$ sam local invoke -e events/single-record.json --env-vars test-env-vars.json
```

To invoke the lambda with multiple events:
```bash
lambda$ sam local invoke -e events/records.json --env-vars test-env-vars.json
```

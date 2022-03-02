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

Log into ECR. Set environment variables for AWS_PROFILE, AWS_REGION, and AWS_ACCOUNT_ID, then run:

```bash
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin $AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com
```

Build your application with the `sam build` command.

```bash
lambda$ sam build
```

The SAM CLI builds a docker image from a Dockerfile and then installs
dependencies defined in `sns_to_glue/requirements.txt` inside the docker image.
The processed template file is saved in the `.aws-sam/build` folder.

Test the function by invoking it directly with a test event. Test events are
included in the `events` folder.

To test the lambda locally, run the following command from the lambda directory.
Ensure that `event.json` contains a valid Synapse id, and that `test-env-vars.json`
contains all the environment vars that are expected in `sns_to_glue/app.py`.

```bash
lambda$ sam local invoke -e events/single-record.json --env-vars test-env-vars.json
```

Tests are run inside a Docker container which includes all the necessary Glue/Spark dependencies and simulates the environment which the Glue jobs will be run in. A Dockerfile is included in this directory. To run tests locally, configure your AWS credentials and launch and attach to the docker container by referencing the following example command as a template:

```
$ docker run --rm -it \
  -v ~/.aws:/home/glue_user/.aws \
  -v ~/Documents/BridgeDownstream/:/home/glue_user/workspace/BridgeDownstream \
  -e DISABLE_SSL=true -p 4040:4040 -p 18080:18080 philsnyder/bridge-downstream-pytest
```

Note that the docker image included in the above example command is publicly accessible, but not updated regularly. We recommend building locally from the Dockerfile before launching a container.

From within the container, ensure that the root of the repo is added to $PATH so that the Glue job modules can be imported. This can be taken care of automatically by passing the `-m` flag to `python3`. Two command line arguments are required when running the tests: `--namespace` and `--artifact-bucket`. Test resources are uploaded and retreived from AWS and need to be namespaced in Glue. Likewise, test data is uploaded to an `--artifact-bucket`, which can differ depending on if we are running tests in develop or prod. These resources are deleted once the tests have completed.

If you are trying to run glue tests particularly the ones in the test_json_s3_to_parquet.py be sure to configure your aws credentials prior to running the below: 

```
aws configure set aws_access_key_id $AWS_ACCESS_KEY_ID
aws configure set aws_secret_access_key $AWS_SECRET_ACCESS_KEY
aws configure set aws_session_token $AWS_SESSION_TOKEN
aws configure set region $AWS_REGION
```

Run the following command from the repo root to run tests (in develop).

```
$ python3 -m pytest --namespace $(git rev-parse --abbrev-ref HEAD) --artifact-bucket sceptre-cloudformation-bucket-bucket-65ci2qog5w6l
```

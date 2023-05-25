"""
Manage cloudformation artifacts for a specific ref (namespace).

We store some resources used by the Glue jobs (such as the schema mapping)
on S3. This script makes sure those resources are uploaded to the
appropriate destination, dependent on the namespace.
"""
import argparse
import subprocess

REPO_NAME = 'BridgeDownstream'


def read_args():
  parser = argparse.ArgumentParser(
    description='Uploading to S3 and deletion from S3 of scripts and templates.',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
  parser.add_argument('--ref', help="The namespace to upload artifacts for.")
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument(
          '--upload',
          action='store_true',
          help="Indicates that artifacts should be uploaded."
  )
  group.add_argument(
          '--remove',
          action='store_true',
          help="Indicates that artifacts should be removed."
  )
  group.add_argument(
          '--list',
          action='store_true',
          help="Indicates that artifacts should be listed."
  )
  parser.add_argument(
          '--environment',
          default="develop",
          choices=["develop", "prod"],
          help="The deployment environment"
  )
  args = parser.parse_args()
  return args


def execute_command(cmd):
  print(f'Invoking command: {" ".join(cmd)}')
  subprocess.run(cmd, check=True)


def upload(ref, cfn_bucket):
  '''Copy Glue scripts to the artifacts bucket'''
  scripts_local_path = 'src/glue/'
  scripts_s3_path = f's3://{cfn_bucket}/{REPO_NAME}/{ref}/glue/'
  cmd = ['aws', 's3', 'sync', scripts_local_path, scripts_s3_path]
  execute_command(cmd)

  '''Copies Lambda code and template to the artifacts bucket'''
  lambda_local_path = 'src/lambda/'
  lambda_s3_path = f's3://{cfn_bucket}/{REPO_NAME}/{ref}/lambda/'
  cmd = ['aws', 's3', 'sync', lambda_local_path, lambda_s3_path]
  execute_command(cmd)

  '''Copy EC2 resources (e.g., crontab) to the artifacts bucket'''
  resources_local_path = 'src/ec2/resources/'
  resources_s3_path = f's3://{cfn_bucket}/{REPO_NAME}/{ref}/ec2/resources/'
  cmd = ['aws', 's3', 'sync', resources_local_path, resources_s3_path]
  execute_command(cmd)

  '''Copy CFN templates to the artifacts bucket'''
  templates_local_path = 'templates/'
  templates_s3_path = f's3://{cfn_bucket}/{REPO_NAME}/{ref}/templates/'
  cmd = ['aws', 's3', 'sync', templates_local_path, templates_s3_path]
  execute_command(cmd)

def delete(ref, cfn_bucket):
  '''Removes all files recursively for ref'''
  s3_path = f's3://{cfn_bucket}/{REPO_NAME}/{ref}/'
  cmd = ['aws', 's3', 'rm', '--recursive', s3_path]
  execute_command(cmd)


def list_refs(cfn_bucket):
  '''List all refs'''
  s3_path = f's3://{cfn_bucket}/{REPO_NAME}/'
  cmd = ['aws','s3','ls', s3_path]
  execute_command(cmd)


def main():
  args = read_args()
  if args.environment == "prod":
    cfn_bucket = "sceptre-cloudformation-bucket-bucket-10mwvvuhlvtk9"
  else:
    cfn_bucket = "sceptre-cloudformation-bucket-bucket-65ci2qog5w6l"
  if args.upload:
    upload(ref=args.ref, cfn_bucket=cfn_bucket)
  elif args.remove:
    delete(ref=args.ref, cfn_bucket=cfn_bucket)
  else:
    list_refs(cfn_bucket=cfn_bucket)

if __name__ == "__main__":
  main()

import argparse
import subprocess

cfn_bucket = 'sceptre-cloudformation-bucket-bucket-65ci2qog5w6l'
repo_name = 'BridgeDownstream'


def read_args():
  descriptions = '''
  Uploading to S3 and deletion from S3 of scripts and templates.
  '''
  parser = argparse.ArgumentParser(
    description='')
  parser.add_argument(
    '--ref')
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument('--upload', action='store_true')
  group.add_argument('--remove', action='store_true')
  group.add_argument('--list', action='store_true')
  args = parser.parse_args()
  return args


def execute_command(cmd):
  print(f'Invoking command: {" ".join(cmd)}')
  subprocess.run(cmd)


def upload(ref):
  '''Copy Glue scripts to the artifacts bucket'''
  scripts_local_path = 'src/glue/'
  scripts_s3_path = f's3://{cfn_bucket}/{repo_name}/{ref}/glue/'
  cmd = ['aws', 's3', 'sync', scripts_local_path, scripts_s3_path]
  execute_command(cmd)

  '''Copies Lambda code and template to the artifacts bucket'''
  lambda_local_path = 'src/lambda/'
  lambda_s3_path = f's3://{cfn_bucket}/{repo_name}/{ref}/lambda/'
  cmd = ['aws', 's3', 'sync', lambda_local_path, lambda_s3_path]
  execute_command(cmd)

  '''Copy EC2 resources (e.g., crontab) to the artifacts bucket'''
  resources_local_path = 'src/ec2/resources/'
  resources_s3_path = f's3://{cfn_bucket}/{repo_name}/{ref}/ec2/resources/'
  cmd = ['aws', 's3', 'sync', resources_local_path, resources_s3_path]
  execute_command(cmd)

  '''Copy CFN templates to the artifacts bucket'''
  templates_local_path = 'templates/'
  templates_s3_path = f's3://{cfn_bucket}/{repo_name}/{ref}/templates/'
  cmd = ['aws', 's3', 'sync', templates_local_path, templates_s3_path]
  execute_command(cmd)

def delete(ref):
  '''Removes all files recursively for ref'''
  s3_path = f's3://{cfn_bucket}/{repo_name}/{ref}/'
  cmd = ['aws', 's3', 'rm', '--recursive', s3_path]
  execute_command(cmd)


def list_refs():
  '''List all refs'''
  s3_path = f's3://{cfn_bucket}/{repo_name}/'
  cmd = ['aws','s3','ls', s3_path]
  execute_command(cmd)


def main(args):
  if args.upload:
    upload(args.ref)
  elif args.remove:
    delete(args.ref)
  else:
    list_refs()

if __name__ == "__main__":
  args = read_args()
  main(args)

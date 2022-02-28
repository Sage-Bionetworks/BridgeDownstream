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
    '--namespace',
    default='bridge-downstream-dev')
  group = parser.add_mutually_exclusive_group(required=True)
  group.add_argument('--upload', action='store_true')
  group.add_argument('--remove', action='store_true')
  group.add_argument('--list', action='store_true')
  args = parser.parse_args()
  return args


def execute_command(cmd):
  print(f'Invoking command: {" ".join(cmd)}')
  subprocess.run(cmd)


def upload(namespace):
  '''Copies Glue scripts and CFN templates to the artifacts bucket'''
  scripts_local_path = 'src/glue/'
  scripts_s3_path = f's3://{cfn_bucket}/{repo_name}/{namespace}/glue/'
  cmd = ['aws', 's3', 'sync', scripts_local_path, scripts_s3_path]
  execute_command(cmd)


  templates_local_path = 'templates/'
  templates_s3_path = f's3://{cfn_bucket}/{repo_name}/{namespace}/templates/'
  cmd = ['aws', 's3', 'sync', templates_local_path, templates_s3_path]
  execute_command(cmd)


def delete(namespace):
  '''Removes all files recursively for namespace'''
  s3_path = f's3://{cfn_bucket}/{repo_name}/{namespace}/'
  cmd = ['aws', 's3', 'rm', '--recursive', s3_path]
  execute_command(cmd)


def list_namespaces():
  '''List all namespaces'''
  s3_path = f's3://{cfn_bucket}/{repo_name}/'
  cmd = ['aws','s3','ls', s3_path]
  execute_command(cmd)


def main(namespace):
  namespace = args.namespace
  if args.upload:
    upload(namespace)
  elif args.remove:
    delete(namespace)
  else:
    list_namespaces()

if __name__ == "__main__":
  args = read_args()
  main(args)

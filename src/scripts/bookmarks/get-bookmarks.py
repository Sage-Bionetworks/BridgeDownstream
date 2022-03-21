#!/usr/bin/env python3

import argparse
import boto3


def read_args():
  descriptions = '''
  Get job bookmarks.
  '''
  parser = argparse.ArgumentParser(
    description='')
  parser.add_argument(
    '--namespace',
    default='bridge-downstream')


def main(namespace):
  client = boto3.client('glue')
  response = client.get_jobs()
  all_jobs = response['Jobs']
  job_names = [job['Name'] for job in all_jobs if job['Name'].startswith(namespace)]

  for job_name in job_names:
    print('')
    response = client.get_job_bookmark(JobName=job_name)
    print(response)


if __name__ == "__main__":
  args = read_args()
  main(args.namespace)

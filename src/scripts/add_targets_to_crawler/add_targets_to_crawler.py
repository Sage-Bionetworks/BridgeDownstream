"""
This module provides functions to update a Glue crawler by adding
additional S3 targets.

If a Glue crawler is configured to crawl new folders only, it cannot be updated
via a regular stack update. Instead, we need to perform the following steps
to completed the update:

    1. Perform the desired crawler updates, but ensure that the update has a
       recrawl policy of CRAWL_EVERYTHING so that the update completes successfully.
    2. If the crawler originally had a different recrawl policy, we can
       "undo" the change to the recrawl policy we made in the previous
       step by doing an additional update -- but only to set the recrawl policy
       back to its original value.

This script supports a use case which occurs when we add support for
new datasets. Additional S3 targets must be added to the crawlers, but our crawlers
recrawl policy is to only crawl new folders. So the default behavior of the function
`add_targets_to_crawler` is to add the S3 targets while overwriting the recrawl policy.
We then update the crawler once more to change the recrawl policy back to crawl new
folders only.
"""
import argparse
import json
import boto3

CRAWL_EVERYTHING_POLICY = "CRAWL_EVERYTHING"
CRAWLER_FORMAT = "{namespace}-{app}-{study}-{crawler_type}"
TARGET_FORMAT = "s3://{bucket}/{namespace}/{app}/{study}/raw_json/dataset={dataset}/"

def read_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--environment",
        required=True,
        choices=["develop", "prod"],
        help="The AWS environment (used to determine the S3 bucket name of each target)"
    )
    parser.add_argument(
        "--crawler-type",
        required=True,
        choices=["standard", "array-of-records"],
        help="The type of crawler"
    )
    parser.add_argument(
        "--studies",
        nargs="+",
        help="List of study identifiers (e.g., cxhnxd)"
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        help="List of dataset identifiers (e.g., ArchiveMetadata_v1)"
    )
    parser.add_argument(
        "--namespace",
        default="bridge-downstream",
        help="The namespace."
    )
    parser.add_argument(
        "--app",
        default="mobile-toolbox",
        help="The app name."
    )
    parser.add_argument(
        "--record-state",
        action="store_true",
        help=("Whether to record the state of the crawler before and "
             "after targets have been added.")
    )
    parser.add_argument(
        "--aws-profile",
        default="default",
        help="Which AWS profile to use."
    )
    args = parser.parse_args()
    return args

def prepare_crawler_update(
        aws_session: "boto3.Session",
        crawler_name: str,
        record_state=False
    ) -> dict:
    """
    Get a Glue crawler and remove properties which are not allowed when
    updating the crawler.

    Args:
        aws_session (boto3.Session): A boto3 session object.
        crawler_name (str): The name of the crawler.
        record_state (bool, optional): Whether to write the crawler and all its properties
            (including disallowed fields) to a JSON file. This can help
            save the state of the crawler before any updates are made.
            The file will be named `{crawler_name}_original.json`. Defaults to False.

    Returns:
        (dict): Crawler with all disallowed properties removed
    """
    glue_client = aws_session.client("glue")
    crawler_update = glue_client.get_crawler(Name=crawler_name)["Crawler"]
    if record_state:
        with open(f"{crawler_name}_original.json", "w") as f:
            json.dump(crawler_update, f, default=str)
    disallowed_fields = [
            "State", "CrawlElapsedTime", "CreationTime",
            "LastUpdated", "LastCrawl", "Version"
    ]
    for key in disallowed_fields:
        if key in crawler_update:
            crawler_update.pop(key)
    return crawler_update

def add_targets_to_crawler(
        aws_session: "boto3.Session",
        crawler_name: str,
        s3_paths: list,
        record_state=False
    ) -> dict:
    """
    Adds a target to a Glue crawler.

    Args:
        aws_session (boto3.Session): A boto3 session object.
        crawler_name (str): The name of the Glue crawler to update.
        s3_paths (list): A list of S3 paths to add as targets for the crawler.
        record_state (bool, optional): Whether to write the crawler and all its properties
            to a JSON file. This can help save the state of the crawler after the targets
            have been added. The file will be named `{crawler_name}_updated.json`.
            Defaults to False.

    Returns:
        dict: The response from the Glue service after updating the crawler.

    Raises:
        Boto3Error: If there is an error calling the Glue service.
    """
    glue_client = aws_session.client("glue")
    # Prepare initial crawler update with recrawl behavior set to `CRAWL_EVERYTHING_POLICY`
    # so that Glue permits us to update the targets.
    crawler_update = prepare_crawler_update(
            aws_session=aws_session,
            crawler_name=crawler_name,
            record_state=record_state
    )
    original_recrawl_behavior = crawler_update["RecrawlPolicy"]["RecrawlBehavior"]
    crawler_update["RecrawlPolicy"] = {"RecrawlBehavior": CRAWL_EVERYTHING_POLICY}
    # Add S3 targets to the crawler update
    for path in s3_paths:
        crawler_update["Targets"]["S3Targets"].append({
            "Path": path,
            "Exclusions": []
        })
    # Update the crawler with new targets
    response = glue_client.update_crawler(**crawler_update)
    # If we changed the recrawl policy, change it back
    if original_recrawl_behavior != CRAWL_EVERYTHING_POLICY:
        crawler_update = prepare_crawler_update(
                aws_session=aws_session,
                crawler_name=crawler_name
        )
        crawler_update["RecrawlPolicy"] = {"RecrawlBehavior": original_recrawl_behavior}
        response = glue_client.update_crawler(**crawler_update)
    # Save the updated crawler details to a JSON file
    with open(f"{crawler_name}_updated.json", "w") as f:
        crawler_update = glue_client.get_crawler(Name=crawler_name)["Crawler"]
        json.dump(crawler_update, f, default=str)
    return response

def main():
    args = read_args()
    aws_session = boto3.Session(profile_name=args.aws_profile)
    if args.environment == "develop":
        bucket_name = "bridge-downstream-dev-intermediate-data"
    elif args.environment == "prod":
        bucket_name = "bridge-downstream-intermediate-data"
    for study in args.studies:
        crawler_name = CRAWLER_FORMAT.format(
                namespace=args.namespace,
                app=args.app,
                study=study,
                crawler_type=args.crawler_type
        )
        s3_paths = [
                TARGET_FORMAT.format(
                    bucket=bucket_name,
                    namespace=args.namespace,
                    app=args.app,
                    study=study,
                    dataset=dataset
                ) for dataset in args.datasets
        ]
        add_targets_to_crawler(
                aws_session=aws_session,
                crawler_name=crawler_name,
                s3_paths=s3_paths,
                record_state=args.record_state
        )

if __name__ == "__main__":
    main()

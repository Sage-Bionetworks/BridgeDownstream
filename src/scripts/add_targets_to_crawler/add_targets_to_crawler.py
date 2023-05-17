"""
This module provides functions to update a Glue crawler by adding
additional S3 targets and (potentially) modifying the recrawl policy.

If a Glue crawler is configured to crawl new folders only, it cannot be updated
via a regular stack update. Instead, we need to perform the following steps
to completed the update:

    1. Perform the desired crawler updates, but ensure that the update has a
       recrawl policy of CRAWL_EVERYTHING so that the update completes successfully.
    2. If the crawler originally had a different recrawl policy, we can
       "undo" the change to the recrawl policy we made in the previous
       step by doing an additional update -- but only to set the recrawl policy
       back to its original value

This script supports a specific use case which occurs when we add support for
new datasets. Additional S3 targets must be added to the crawlers, but our crawlers
recrawl policy is to only crawl new folders. So the default behavior of the function
`add_target_to_crawler` is to add the S3 targets while overwriting the recrawl policy.
We then update the crawler once more to change the recrawl policy back to crawl new
folders only.
"""
import json
import boto3

def prepare_crawler_update(crawler_name: str, debug=False) -> dict:
    """
    Get a Glue crawler and remove properties which are not allowed when
    updating the crawler.

    Args:
        crawler_name (str): the name of the crawler.
        debug (bool, optional): Whether to write the crawler and all its properties
            (including disallowed fields) to a JSON file. This can help
            save the state of the crawler before any updates are made.
            The file will be named `{crawler_name}_original.json`. Defaults to False.

    Returns:
        (dict): Crawler with all disallowed properties removed
    """
    glue_client = boto3.client('glue')
    crawler_update = glue_client.get_crawler(Name=crawler_name)["Crawler"]
    if debug:
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

def add_target_to_crawler(crawler_name: str, s3_paths: list, recrawl_new_only=True) -> dict:
    """
    Adds a target to a Glue crawler and updates its recrawl policy
    to recrawl new folders only.

    Args:
        crawler_name (str): The name of the Glue crawler to update.
        s3_paths (list): A list of S3 paths to add as targets for the crawler.
        recrawl_new_only (bool, optional): Whether to set the recrawl policy
            to 'CRAWL_NEW_FOLDERS_ONLY'. Defaults to True.

    Returns:
        dict: The response from the Glue service after updating the crawler.

    Raises:
        Boto3Error: If there is an error calling the Glue service.
    """
    glue_client = boto3.client('glue')
    # Prepare initial crawler update with recrawl behavior set to 'CRAWL_EVERYTHING'
    # so that Glue permits us to update the crawler.
    crawler_update = prepare_crawler_update(crawler_name=crawler_name, debug=True)
    crawler_update["RecrawlPolicy"] = {'RecrawlBehavior': 'CRAWL_EVERYTHING'}
    # Add S3 targets to the crawler update
    for path in s3_paths:
        crawler_update["Targets"]["S3Targets"].append({
            "Path": path,
            "Exclusions": []
        })
    # Update the crawler with new targets
    response = glue_client.update_crawler(**crawler_update)
    # If recrawl_new_only is True, update the recrawl policy to 'CRAWL_NEW_FOLDERS_ONLY'
    if recrawl_new_only:
        crawler_update = prepare_crawler_update(crawler_name=crawler_name)
        crawler_update["RecrawlPolicy"] = {'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'}
        response = glue_client.update_crawler(**crawler_update)
    # Save the updated crawler details to a JSON file
    with open(f"{crawler_name}_updated.json", "w") as f:
        crawler_update = glue_client.get_crawler(Name=crawler_name)["Crawler"]
        json.dump(crawler_update, f, default=str)
    return response

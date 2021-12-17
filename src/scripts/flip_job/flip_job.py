"""
This script is intended to be used as part of the schema change protocol.
Run this script to disable a job in preperation of archiving that job's
outputted parquet dataset. Likewise, re-enable that job after archiving
by omitting --disable and specifying --triggers. This script uses
a `contains`-like function to select jobs, but its intended
use is to only affect one job at a time. The `contains` functionality is
intended as a generic way of selecting a specific job, a job whose exact
name may change with the specific stack it is deployed in. For example,
passing --name-contains MetadataParquetJobStack_v2 is intended to only
select one job: that which produces the metadata_v2 parquet dataset.

Jobs are disabled by removing them from the `Actions` list of every trigger
which references that job. Any running job runs are then stopped. Jobs are
re-enabled by adding them back to the `Actions` list of each trigger,
but stopped jobs are not resumed. The records which were being processed by
the stopped job run will be processed next job run, assuming job bookmarks
are being used.
"""
import argparse
import boto3


def read_args():
    parser = argparse.ArgumentParser(
            description=("Disable or enable all jobs which contain --name-contains."
                         "Jobs are disabled by removing them from the actions of "
                         "any triggers which reference them and stopping any currently "
                         "running job runs of that job. Jobs are enabled by re-adding "
                         "those jobs to the provided --triggers actions."))
    parser.add_argument("--disable",
                        action="store_true",
                        help=("Disable the trigger(s) of the matching jobs. "
                              "The defualt action (by omitting this flag) is to "
                              "enable the trigger(s)."))
    parser.add_argument("--name-contains",
                        help=("The string to search for in the name of each "
                              "job. Matching jobs will be affected "
                              "by the specified action."))
    parser.add_argument("--triggers",
                        nargs="+",
                        help=("A list of triggers to add the matching jobs "
                              "to each triggers actions. Use this argument "
                              "only if you are enabling, rather than disabling, "
                              "the matching jobs"))
    parser.add_argument("--profile",
                        help="The AWS profile to use.")
    args = parser.parse_args()
    return args


def remove_job_from_trigger_action(glue_client, jobs):
    all_affected_triggers = list()
    for j in jobs:
        triggers = glue_client.get_triggers(DependentJobName=j)
        for t in triggers["Triggers"]: # Remove job from trigger
            unnessecary_parameters = ["WorkflowName", "Type", "State"]
            for p in unnessecary_parameters:
                t.pop(p)
            new_actions = [action for action in t["Actions"] if action["JobName"] != j]
            t["Actions"] = new_actions
            glue_client.update_trigger(
                    Name=t["Name"],
                    TriggerUpdate=t)
            all_affected_triggers.append(t["Name"])
        job_runs = glue_client.get_job_runs(JobName=j)
        running_jobs = [jr for jr in job_runs["JobRuns"] if jr["JobRunState"] == "RUNNING"]
        if len(running_jobs) > 0: # Stop currently running jobs
            glue_client.batch_stop_job_run(
                    JobName=j,
                    JobRunIds=[job_run["Id"] for job_run in running_jobs])
    return all_affected_triggers


def add_job_to_trigger_action(glue_client, jobs, trigger_names):
    triggers = glue_client.batch_get_triggers(TriggerNames=trigger_names)
    for t in triggers["Triggers"]:
        unnessecary_parameters = ["WorkflowName", "Type", "State"]
        for p in unnessecary_parameters:
            t.pop(p)
        for j in jobs:
            t["Actions"].append({"JobName": j})
        glue_client.update_trigger(
                Name=t["Name"],
                TriggerUpdate=t)
    return trigger_names


def main():
    args = read_args()
    aws_session = boto3.session.Session(profile_name=args.profile)
    glue_client = aws_session.client("glue")
    all_jobs = glue_client.list_jobs()
    matching_jobs = [j for j in all_jobs["JobNames"] if args.name_contains in j]
    if args.disable:
        affected_triggers = remove_job_from_trigger_action(
                glue_client=glue_client,
                jobs=matching_jobs)
    else:
        if args.triggers is not None and len(args.triggers) > 0:
            affected_triggers = add_job_to_trigger_action(
                    glue_client=glue_client,
                    jobs=matching_jobs,
                    trigger_names=args.triggers)
        else:
            raise ValueError("If --disable is not passed, a list of --triggers "
                             "must be included.")
    print(" ".join(affected_triggers))
    return affected_triggers


if __name__ == "__main__":
    main()

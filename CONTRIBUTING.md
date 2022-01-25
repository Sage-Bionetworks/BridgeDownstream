This document is a work in progress.

*Things to cover: pipenv, Cloudformation, Sceptre, SAM, Github workflows, git hooks*

### Monitoring

Use a combination of Workflows and Glue Studio, both within the [Glue console](https://console.aws.amazon.com/glue/), for graphical monitoring. Workflows gives a good view of what's happening within a workflow, but for a better overview of all the job runs, Glue Studio is recommended. Either option will give you a link to the Cloudwatch logs for each job.

#### Debugging Spark Issues
If your job is failing due to Spark issues, the Spark UI may give additional useful information. All jobs are configured to output spark logs. The current solution for viewing the Spark History Server is to run it locally through Docker. In order to do this, you need to be able to authenticate with AWS and get temporary credentials, and to run a docker image. Follow these [directions](https://docs.aws.amazon.com/glue/latest/dg/monitor-spark-ui-history.html#monitor-spark-ui-history-local) provided by AWS to start the server.

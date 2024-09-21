# PySpark Tutorial

Relevant files and examples related to PySpark tutorials.

## AWS Related Assets

Contains AWS related content that can be used with PySpark.

### `aws/automation`

A serverless application model (SAM) project that is based on lambda functions to capture events in AWS cloud for ETL related functionality.

#### Run Lambda Functions

Take note that these are ran within the `aws/automation` directory since SAM is a "subproject" of this main project.

##### Trigger Glue Job

```bash
sam local invoke TriggerGlueJobFromS3Function --events events/event_s3_put.json --debug
```

# PySpark Tutorial

Relevant files and examples related to PySpark tutorials.

## AWS Related Assets

Contains AWS related content that can be used with PySpark.

### `aws/aggregator`

A serverless application model (SAM) project that is based on lambda functions to capture events in AWS cloud for ETL related functionality.

#### Run Lambda Functions

Take note that these are ran within the `aws/aggregator` directory since SAM is a "subproject" of this main project.

##### Departments Sum Salaries

```bash
sam local invoke DepartmentSumSalariesFunction --events events/s3_aggregate_example.json --debug
```

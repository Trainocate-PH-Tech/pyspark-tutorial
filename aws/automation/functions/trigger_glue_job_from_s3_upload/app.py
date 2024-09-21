import json
import boto3

# Reference: https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-python-calling.html

def lambda_handler(event, context):
    # Define the job name
    glue_job_name = "Print Parameters"

    # Define job parameters
    job_parameters = {
        '--bucket_name': "my-bucket",
        '--key': "my-file"
    }

    # Create a Glue client
    glue_client = boto3.client('glue')

    try:
        # Start the Glue job
        response = glue_client.start_job_run(
            JobName=glue_job_name,
            Arguments=job_parameters
        )

        print(f"Started Glue job: {glue_job_name}, Run ID: {response['JobRunId']}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Done.",
                # "location": ip.text.replace("\n", "")
            }),
        }
    except Exception as e:
        error_message = f"Error starting Glue job: {e}"

        print(error_message)

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": error_message
            }),
        }

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv
import os

load_dotenv()

# Initialize boto3 Glue client
glue_client = boto3.client('glue')

# Define parameters for the Glue job
job_name = os.getenv('GLUE_JOB_NAME')
role_arn = os.getenv('GLUE_JOB_ROLE_ARN')
script_location = os.getenv('GLUE_JOB_SCRIPT_LOCATION')
source_bucket = os.getenv('GLUE_JOB_SOURCE_BUCKET')
target_bucket = os.getenv('GLUE_JOB_TARGET_BUCKET')
temp_dir = os.getenv('GLUE_JOB_TEMP_DIR')

# Glue job parameters
glue_job_params = {
    'Name': job_name,
    'Role': role_arn,
    'Command': {
        'Name': 'glueetl',
        'ScriptLocation': script_location,
        'PythonVersion': '3',
    },
    'DefaultArguments': {
        '--TempDir': temp_dir,  # Temporary directory for intermediate files
        '--job-bookmark-option': 'job-bookmark-disable',  # Disable job bookmarking
        '--additional-python-modules': 'pyarrow==3, pandas==1.1.5'  # Required dependencies for PySpark
    },
    'MaxCapacity': 10,  # Defines the maximum capacity for the job (e.g., 10 DPUs)
    'Timeout': 30,  # Timeout for job in minutes
    'MaxRetries': 3  # Maximum number of retries
}

# Create the Glue job
try:
    response = glue_client.create_job(**glue_job_params)
    print(f"Glue Job '{job_name}' created successfully!")
except ClientError as e:
    print(f"Error creating Glue Job: {e}")

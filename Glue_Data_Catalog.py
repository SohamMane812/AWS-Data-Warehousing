import boto3
from dotenv import load_dotenv
import os
import time

# Load environment variables from .env file
load_dotenv()

# Get environment variables
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')
database_name = os.getenv('GLUE_DATABASE_NAME')
crawler_name = os.getenv('GLUE_CRAWLER_NAME')
bucket_name = os.getenv('S3_BUCKET_NAME')
role_arn = os.getenv('GLUE_ROLE_ARN')
s3_file_path=os.getenv('S3_FILE_PATH')

# Create Glue client
glue_client = boto3.client(
    'glue',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

try:
    # Create database if it doesn't exist
    try:
        glue_client.get_database(Name=database_name)
        print(f"Database {database_name} already exists")
    except:
        glue_client.create_database(
            DatabaseInput={
                'Name': database_name,
                'Description': 'Database for raw data catalog'
            }
        )
        print(f"Created database: {database_name}")

    # Create crawler if it doesn't exist
    try:
        glue_client.get_crawler(Name=crawler_name)
        print(f"Crawler {crawler_name} already exists")
    except:
        glue_client.create_crawler(
            Name=crawler_name,
            Role=role_arn,  # Make sure this role exists with appropriate permissions
            DatabaseName=database_name,
            Targets={
                'S3Targets': [
                    {
                        'Path': s3_file_path
                    }
                ]
            },
            Description='Crawler for raw data',
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'LOG'
            }
        )
        print(f"Created crawler: {crawler_name}")

    # Start crawler
    glue_client.start_crawler(Name=crawler_name)
    print(f"Started crawler: {crawler_name}")

    # Wait for crawler to finish
    while True:
        crawler_status = glue_client.get_crawler(Name=crawler_name)['Crawler']['State']
        if crawler_status == 'READY':
            break
        print(f"Crawler status: {crawler_status}")
        time.sleep(30)

    print("Crawler finished successfully")

except Exception as e:
    print(f"An error occurred: {str(e)}")

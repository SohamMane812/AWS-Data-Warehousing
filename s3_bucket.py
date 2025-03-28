import boto3
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Get environment variables
bucket_name = os.getenv('S3_BUCKET_NAME')
file_name = os.getenv('S3_FILE_NAME')
object_name = os.getenv('S3_OBJECT_NAME')

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
aws_region = os.getenv('AWS_REGION')

# Create S3 client
s3_client = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=aws_region
)

try:
    # Create bucket if it doesn't exist
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} already exists")
    except:
        s3_client.create_bucket(Bucket=bucket_name)
        print(f"Created bucket: {bucket_name}")

    # Upload file to S3
    s3_client.upload_file(
        Filename=file_name,
        Bucket=bucket_name,
        Key=object_name
    )
    print(f"Successfully uploaded {file_name} to {bucket_name}/{object_name}")

except Exception as e:
    print(f"An error occurred: {str(e)}")

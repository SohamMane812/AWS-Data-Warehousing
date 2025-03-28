import boto3
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Get environment variables
target_bucket_name = os.getenv('S3_TARGET_BUCKET_NAME') 
target_folder_name = os.getenv('S3_TARGET_FOLDER_NAME')

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
        s3_client.head_bucket(Bucket=target_bucket_name)
        print(f"Bucket {target_bucket_name} already exists")

        s3_client.put_object(Bucket=target_bucket_name, Key=target_folder_name, Body='')
        print(f"Folder '{target_folder_name}' created successfully in bucket '{target_bucket_name}'")

    except:
        s3_client.create_bucket(Bucket=target_bucket_name)
        print(f"Created bucket: {target_bucket_name}")

        s3_client.put_object(Bucket=target_bucket_name, Key=target_folder_name, Body='')
        print(f"Folder '{target_folder_name}' created successfully in bucket '{target_bucket_name}'")

except Exception as e:
    print(f"An error occurred: {str(e)}")
